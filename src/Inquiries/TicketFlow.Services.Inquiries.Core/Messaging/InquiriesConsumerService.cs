using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TicketFlow.Services.Inquiries.Core.Messaging.Consuming.Demultiplexing;
using TicketFlow.Services.Inquiries.Core.Messaging.Consuming.TicketCreated;
using TicketFlow.Shared.AnomalyGeneration.MessagingApi;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Inquiries.Core.Messaging;

public class InquiriesConsumerService(
    IMessageConsumer messageConsumer,
    AnomalySynchronizationConfigurator anomalyConfigurator,
    IServiceProvider serviceProvider) : BackgroundService
{
    public const string TicketCreatedQueue = "inquiries-ticket-created";
    public const string TicketChangesQueue = "inquiries-ticket-changes";
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // TicketCreated is handled separately as it links inquiry to ticket
        await messageConsumer
            .ConsumeMessage<TicketCreated>(
                queue: TicketCreatedQueue,
                acceptedMessageTypes: null, // Handled by binding on RMQ instead (routing-key: ticket-created)
                cancellationToken: stoppingToken);
        
        // All other ticket changes are handled via demultiplexing
        await messageConsumer
            .ConsumeNonGeneric(
                handleRawPayload: async (messageData) =>
                {
                    var demultiplexingHandler = CreateDemultiplexingHandler();
                    await demultiplexingHandler.HandleAsync(messageData, stoppingToken);
                },
                queue: TicketChangesQueue,
                acceptedMessageTypes: ["TicketQualified", "AgentAssignedToTicket", "TicketBlocked", "TicketResolved"],
                cancellationToken: stoppingToken);
        
        await anomalyConfigurator.ConsumeAnomalyChanges();
    }
    
    private TicketChangesHandler CreateDemultiplexingHandler()
    {
        var iocScope = serviceProvider.CreateScope();
        return iocScope.ServiceProvider.GetRequiredService<TicketChangesHandler>();
    }
}