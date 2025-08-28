using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Neuroglia.AsyncApi.v3;
using TicketFlow.Shared.AnomalyGeneration.MessagingApi;
using TicketFlow.Shared.App;
using TicketFlow.Shared.AsyncAPI;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Shared.AnomalyGeneration.HttpApi;

public static class AnomalyEndpoints
{
    public static void UseAnomalyEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("anomalies", ([FromServices] IAnomaliesStorage anomaliesStorage) =>
        {
            var anomalies = anomaliesStorage.GetAnomalies();
            return anomalies;
        });

        endpoints.MapPost("anomalies", async (
            [FromServices] IAnomaliesStorage anomaliesStorage,
            [FromServices] IEnumerable<IMessagePublisher> publishers,
            [FromServices] IOptions<AppOptions> appOptions,
            [FromBody] EnableAnomalyRequest request) =>
        {
            anomaliesStorage.EnableAnomaly(request);
            
            // Skip outbox to reduce the noise
            var publisher = publishers.FirstOrDefault(x => !x.GetType().Name.Contains("Outbox"));
            await PublishAnomalyEnabled(publisher, request, appOptions);
        });
        
        endpoints.MapDelete("anomalies/{anomalyType}/messages/{messageType}", async (
            [FromServices] IAnomaliesStorage anomaliesStorage, 
            [FromServices] IEnumerable<IMessagePublisher> publishers,
            [FromServices] IOptions<AppOptions> appOptions,
            [FromRoute] string anomalyType,
            [FromRoute] string messageType) =>
        {
            var anomalyParsed = Enum.Parse<AnomalyType>(anomalyType);
            anomaliesStorage.DisableAnomaly(anomalyParsed, messageType);
            
            // Skip outbox to reduce the noise
            var publisher = publishers.FirstOrDefault(x => !x.GetType().Name.Contains("Outbox"));
            await PublishAnomalyDisabled(publisher, anomalyParsed, messageType, appOptions);
        });
    }

    [Operation(Conventions.Operation.PublishPrefix + "AnomalyEventWrapper", V3OperationAction.Send, Conventions.Ref.ChannelPrefix + "AnomalyEventWrapper", Description = "Notify that anomaly was enabled")]
    private static async Task PublishAnomalyEnabled(IMessagePublisher? publisher, EnableAnomalyRequest request,
        IOptions<AppOptions> appOptions)
    {
        await publisher.PublishAsync(
            message: AnomalyEnabled.FromRequest(request).Wrapped(), 
            destination: AnomalyTopologyBuilder.AnomaliesExchange,
            routingKey: appOptions.Value.AppName);
    }
    
    [Operation(Conventions.Operation.PublishPrefix + "AnomalyEventWrapper", V3OperationAction.Send, Conventions.Ref.ChannelPrefix + "AnomalyEventWrapper", Description = "Notify that anomaly was disabled")]
    private static async Task PublishAnomalyDisabled(IMessagePublisher? publisher, AnomalyType anomalyParsed,
        string messageType, IOptions<AppOptions> appOptions)
    {
        await publisher.PublishAsync(
            message: new AnomalyDisabled(anomalyParsed, messageType).Wrapped(), 
            destination: AnomalyTopologyBuilder.AnomaliesExchange,
            routingKey: appOptions.Value.AppName);
    }
}