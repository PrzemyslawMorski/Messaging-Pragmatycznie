using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TicketFlow.Services.SystemMetrics.Generator.Data;
using TicketFlow.Shared.AsyncAPI;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.SystemMetrics.Core.LiveMetrics;

public class LiveMetricsPullService(
    IMessageConsumer messageConsumer,
    LiveMetricsHub hub,
    LiveMetricsOptions opts,
    TopologyDescription topologyDescription) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        topologyDescription.MarkConsumersRegistered(); //Only for AsyncAPI to finish describing; below get is not mapped (challenge yourself 😉)
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!hub.HasActiveClients)
                {
                    await Task.Delay(opts.PollingIntervalInMs, stoppingToken);
                    continue;
                }

                await messageConsumer.GetMessage<MetricTick>(
                    handle: async msg => await hub.PushOldestMetricTick(msg),
                    LiveMetricsHub.LiveMetricsQueue,
                    stoppingToken);

                await Task.Delay(opts.PollingIntervalInMs, stoppingToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error while pulling the message from broker: {0}", ex.Message);
            }
        }
    }
}