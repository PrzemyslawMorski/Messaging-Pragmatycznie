using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Neuroglia.AsyncApi.v3;
using TicketFlow.Services.SLA.Core.Data.Models;
using TicketFlow.Services.SLA.Core.Data.Repositories;
using TicketFlow.Services.SLA.Core.Http.Communication;
using TicketFlow.Services.SLA.Core.Messaging.Publishing;
using TicketFlow.Shared.AsyncAPI;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.SLA.Core.Schedulers;

public class DeadlineBreachWatcher(
    DeadlineBreachOptions options,
    IServiceProvider serviceProvider) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (stoppingToken.IsCancellationRequested is false)
        {
            await Task.Delay(options.WatcherIntervalInSeconds, stoppingToken);
            try
            {
                await MarkBreachedDeadlines(stoppingToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{nameof(DeadlineBreachWatcher)} failed with exception: {ex}");
            }
        }
    }

    private async Task MarkBreachedDeadlines(CancellationToken stoppingToken)
    {
        using var iocScope = serviceProvider.CreateScope();
        var slaRepository = iocScope.ServiceProvider.GetRequiredService<ISLARepository>();
        var messagePublisher = iocScope.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var communicationClient = iocScope.ServiceProvider.GetRequiredService<ICommunicationClient>();
        
        var overdueDeadlines = await slaRepository.GetWithDeadlineDateBreached(stoppingToken);
        foreach (var deadline in overdueDeadlines)
        {
            deadline.DetectDeadlineBreached();
            
            var lastBreachNotification = deadline.LastDeadlineBreachedAlertSentDateUtc ?? deadline.DeadlineDateUtc;
            var canAlertAgain = DateTimeOffset.UtcNow - lastBreachNotification > TimeSpan.FromSeconds(options.SecondsBetweenBreachAlerts);
            
            if (canAlertAgain)
            {
                deadline.MarkDeadlineBreachAlertSent();
                /* Explicit decision to use CancellationToken.None - email was already sent so it's better to "force save" */
                await PublishSLABreached(messagePublisher, deadline);

                if (deadline.UserIdToRemind.HasValue)
                {
                    await communicationClient.SendReminderMessage(
                        deadline.UserIdToRemind!.Value,
                        deadline.ServiceType,
                        deadline.ServiceSourceId,
                        ICommunicationClient.ReminderMessageType.SLABreachedRecurring,
                        stoppingToken);
                }
            }
            
            await slaRepository.SaveReminders(deadline, CancellationToken.None);
        }

        await Task.CompletedTask;
    }

    [Operation(Conventions.Operation.PublishPrefix + "SLABreached", V3OperationAction.Send, Conventions.Ref.ChannelPrefix + "SLABreached", Description = "Notify that SLA was breached")]
    private static async Task PublishSLABreached(IMessagePublisher messagePublisher, DeadlineReminders deadline)
    {
        await messagePublisher.PublishAsync(new SLABreached(deadline.ServiceType, deadline.ServiceSourceId), cancellationToken: CancellationToken.None);
    }
}