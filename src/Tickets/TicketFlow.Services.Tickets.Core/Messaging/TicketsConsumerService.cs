using Microsoft.Extensions.Hosting;
using TicketFlow.CourseUtils;
using TicketFlow.Services.Tickets.Core.Messaging.Consuming.DeadlinesCalculated;
using TicketFlow.Services.Tickets.Core.Messaging.Consuming.InquirySubmitted;
using TicketFlow.Services.Tickets.Core.Messaging.Consuming.TranslationCompleted;
using TicketFlow.Services.Tickets.Core.Messaging.Publishing;
using TicketFlow.Shared.AnomalyGeneration.MessagingApi;
using TicketFlow.Shared.AsyncAPI;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Tickets.Core.Messaging;

internal sealed class TicketsConsumerService(IMessageConsumer messageConsumer, 
    AnomalySynchronizationConfigurator anomalyConfigurator,
    TopologyDescription topologyDescription) : BackgroundService
{
    public const string SLAChangesQueue = "tickets-sla-changes";
    public const string TicketCreatedQueue = "tickets-ticket-created";
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await messageConsumer.ConsumeMessage<InquirySubmitted>();
        await messageConsumer.ConsumeMessage<TranslationCompleted>();
        await messageConsumer.ConsumeMessage<DeadlinesCalculated>(queue: SLAChangesQueue, acceptedMessageTypes: ["DeadlinesCalculated"]);
        if (FeatureFlags.UseListenToYourselfExample)
        {
            await messageConsumer.ConsumeMessage<TicketCreated>(queue: TicketCreatedQueue, acceptedMessageTypes: ["TicketCreated"]);
        }
        await anomalyConfigurator.ConsumeAnomalyChanges();
        topologyDescription.MarkConsumersRegistered();
    }
}