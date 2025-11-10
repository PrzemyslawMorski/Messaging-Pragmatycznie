using Microsoft.Extensions.Logging;
using TicketFlow.Services.Inquiries.Core.Data.Repositories;
using TicketFlow.Services.Inquiries.Core.Messaging.Consuming;
using TicketFlow.Shared.Messaging;
using TicketFlow.Shared.Serialization;

namespace TicketFlow.Services.Inquiries.Core.Messaging.Consuming.Demultiplexing;

public class TicketChangesHandler
{
    private readonly ISerializer _serializer;
    private readonly IInquiriesRepository _inquiriesRepository;
    private readonly ILogger<TicketChangesHandler> _logger;

    public TicketChangesHandler(
        ISerializer serializer,
        IInquiriesRepository inquiriesRepository,
        ILogger<TicketChangesHandler> logger)
    {
        _serializer = serializer;
        _inquiriesRepository = inquiriesRepository;
        _logger = logger;
    }

    public async Task HandleAsync(MessageData messageData, CancellationToken cancellationToken = default)
    {
        _logger.LogWarning("[{Timestamp:O}] {HandlerName} is processing:{NewLine} {MessageData}", 
            DateTime.UtcNow, nameof(TicketChangesHandler), Environment.NewLine, messageData);
        
        ITicketChange message = Demultiplex(messageData);
        
        _logger.LogWarning("[{Timestamp:O}] {HandlerName} demultiplexed message data to:{NewLine} {Message}", 
            DateTime.UtcNow, nameof(TicketChangesHandler), Environment.NewLine, message);
        
        var inquiry = await _inquiriesRepository.GetByTicketIdAsync(message.TicketId, cancellationToken);

        if (inquiry == null)
        {
            // Inquiry might not exist or might not be linked to a ticket yet
            _logger.LogInformation("No inquiry found for ticket {TicketId}, skipping status update", message.TicketId);
            return;
        }

        switch (message)
        {
            case AgentAssignedToTicket:
                inquiry.SetInProgress();
                await _inquiriesRepository.UpdateAsync(inquiry, cancellationToken);
                _logger.LogInformation("Updated inquiry {InquiryId} to InProgress for ticket {TicketId}", inquiry.Id, message.TicketId);
                break;
            case TicketResolved:
                inquiry.Close();
                await _inquiriesRepository.UpdateAsync(inquiry, cancellationToken);
                _logger.LogInformation("Closed inquiry {InquiryId} for ticket {TicketId}", inquiry.Id, message.TicketId);
                break;
            case TicketQualified:
                // TicketQualified doesn't change inquiry status - inquiry starts as New
                // and only changes when agent is assigned or ticket is resolved
                break;
            default:
                _logger.LogWarning("Unhandled ticket change event type: {EventType}", message.GetType().Name);
                break;
        }
        
        _logger.LogWarning("[{Timestamp:O}] {HandlerName} processed message:{NewLine} {Message}", 
            DateTime.UtcNow, nameof(TicketChangesHandler), Environment.NewLine, message);
    }
    
    private ITicketChange Demultiplex(MessageData messageData)
    {
        return messageData.Type switch
        {
            "TicketQualified" => _serializer.DeserializeBinary<TicketQualified>(messageData.Payload),
            "AgentAssignedToTicket" => _serializer.DeserializeBinary<AgentAssignedToTicket>(messageData.Payload),
            "TicketResolved" => _serializer.DeserializeBinary<TicketResolved>(messageData.Payload),
            _ => _serializer.DeserializeBinary<FallbackTicketChangeEvent>(messageData.Payload)
        };
    }
}

