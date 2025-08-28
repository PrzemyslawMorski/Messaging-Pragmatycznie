using Microsoft.Extensions.Hosting;
using TicketFlow.Services.Inquiries.Core.Messaging.Consuming.TicketCreated;
using TicketFlow.Shared.AnomalyGeneration.MessagingApi;
using TicketFlow.Shared.AsyncAPI;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Inquiries.Core.Messaging;

public class InquiriesConsumerService(
    IMessageConsumer messageConsumer,
    AnomalySynchronizationConfigurator anomalyConfigurator,
    TopologyDescription topologyDescription) : BackgroundService
{
    public const string TicketCreatedQueue = "inquiries-ticket-created";
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await messageConsumer
            .ConsumeMessage<TicketCreated>(
                queue: TicketCreatedQueue,
                acceptedMessageTypes: null, // Handled by binding on RMQ instead (routing-key: ticket-created)
                cancellationToken: stoppingToken);
        
        await anomalyConfigurator.ConsumeAnomalyChanges();
        topologyDescription.MarkConsumersRegistered();
    }
}