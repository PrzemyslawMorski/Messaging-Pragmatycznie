using Microsoft.Extensions.Hosting;
using TicketFlow.Services.Inquiries.Core.Messaging.Consuming.AgentAssignedToTicket;
using TicketFlow.Services.Inquiries.Core.Messaging.Consuming.TicketCreated;
using TicketFlow.Services.Inquiries.Core.Messaging.Consuming.TicketResolved;
using TicketFlow.Shared.AnomalyGeneration.MessagingApi;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Inquiries.Core.Messaging;

public class InquiriesConsumerService(IMessageConsumer messageConsumer, AnomalySynchronizationConfigurator anomalyConfigurator) : BackgroundService
{
    public const string TicketCreatedQueue = "inquiries-ticket-created";
    public const string TicketResolvedQueue = "inquiries-ticket-resolved";
    public const string AgentAssignedQueue = "inquiries-agent-assigned";
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await messageConsumer
            .ConsumeMessage<TicketCreated>(
                queue: TicketCreatedQueue,
                acceptedMessageTypes: null, // Handled by binding on RMQ instead (routing-key: ticket-created)
                cancellationToken: stoppingToken);
        
        await messageConsumer
            .ConsumeMessage<AgentAssignedToTicket>(
                queue: AgentAssignedQueue,
                acceptedMessageTypes: null, // Handled by binding on RMQ instead (routing-key: agent-assigned)
                cancellationToken: stoppingToken);
        
        await messageConsumer
            .ConsumeMessage<TicketResolved>(
                queue: TicketResolvedQueue,
                acceptedMessageTypes: null, // Handled by binding on RMQ instead (routing-key: ticket-resolved)
                cancellationToken: stoppingToken);
        
        await anomalyConfigurator.ConsumeAnomalyChanges();
    }
}