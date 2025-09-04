using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using TicketFlow.Shared.Messaging.Partitioning;
using TicketFlow.Shared.Messaging.Resiliency;
using TicketFlow.Shared.Messaging.Topology;
using TicketFlow.Shared.Observability;
using TicketFlow.Shared.Serialization;

namespace TicketFlow.Shared.Messaging.RabbitMQ;

internal sealed class RabbitMqMessageConsumer(
    ChannelFactory channelFactory,
    IMessageConsumerConventionProvider conventionProvider,
    IServiceProvider serviceProvider,
    MessagePropertiesAccessor messagePropertiesAccessor,
    ISerializer serializer,
    ILogger<RabbitMqMessageConsumer> logger,
    TopologyReadinessAccessor topologyReadinessAccessor,
    ReliableConsuming reliableConsuming,
    ResiliencyOptions resiliencyOptions) : IMessageConsumer
{
    public async Task<IMessageConsumer> ConsumeMessage<TMessage>(
        Func<TMessage, Task>? handle = default, 
        string? queue = default,
        string[]? acceptedMessageTypes = default,
        CancellationToken cancellationToken = default) where TMessage : class, IMessage
    {
        var channel = await channelFactory.CreateForConsumerAsync();
        await ConfigureConsumerQosAsync(channel);
        var consumer = new AsyncEventingBasicConsumer(channel);
        var (destination, _) = conventionProvider.Get<TMessage>();
        var destinationResolved = queue ?? destination;
        
        consumer.ReceivedAsync += async (model, ea) =>
        {
            SetMessageProperties(ea.BasicProperties, ea.Redelivered);
            using var activity = CreateMessagingConsumeActivity(ea.BasicProperties);

            if (IsNotAcceptedMessageType(acceptedMessageTypes, ea))
            {
                await channel.BasicAckAsync(ea.DeliveryTag, false, cancellationToken); // ACK instead of NACK or REJECT to not trigger DLQ routing
                return;
            }

            try
            {
                await HandleMessageAsync(ea, handle, cancellationToken);
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "An error occured while handling a message");
                Activity.Current?.SetStatus(ActivityStatusCode.Error, exception.Message);
                await OnHandleFailure<TMessage>(ea, channel, exception, destinationResolved, cancellationToken);
                return;
            }
            
            await channel.BasicAckAsync(ea.DeliveryTag, false, cancellationToken);
        };

        await EnsureTopologyReady(cancellationToken);
        
        await channel.BasicConsumeAsync(queue: destinationResolved, autoAck: false, consumer: consumer, cancellationToken);
        return this;
    }

    public async Task<IMessageConsumer> ConsumeNonGeneric(Func<MessageData, Task> handleRawPayload, string queue, string[]? acceptedMessageTypes = default,
        CancellationToken cancellationToken = default)
    {
        var channel = await channelFactory.CreateForConsumerAsync();
        await ConfigureConsumerQosAsync(channel);
        var consumer = new AsyncEventingBasicConsumer(channel);
        var destinationResolved = queue;
        
        consumer.ReceivedAsync += async (model, ea) =>
        {
            SetMessageProperties(ea.BasicProperties, ea.Redelivered);
            using var activity = CreateMessagingConsumeActivity(ea.BasicProperties);

            if (IsNotAcceptedMessageType(acceptedMessageTypes, ea))
            {
                await channel.BasicAckAsync(ea.DeliveryTag, false, cancellationToken); // ACK instead of NACK or REJECT to not trigger DLQ routing
                return;
            }

            try
            {
                var messageData = CreateMessageData(ea);
                await handleRawPayload(messageData);
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "An error occured while handling a message");
                Activity.Current?.SetStatus(ActivityStatusCode.Error, exception.Message);
                await OnHandleFailureNonGeneric(ea, channel, exception, destinationResolved, cancellationToken);
                return;
            }
          
            await channel.BasicAckAsync(ea.DeliveryTag, false, cancellationToken);
        };

        await EnsureTopologyReady(cancellationToken);
        
        await channel.BasicConsumeAsync(queue: queue, autoAck: false, consumer: consumer, cancellationToken);
        return this;
    }

    public async Task GetMessage<TMessage>(
        Func<TMessage, Task> handle,
        string? queue = default,
        CancellationToken cancellationToken = default) where TMessage : class, IMessage
    {
        var channel = await channelFactory.CreateForConsumerAsync();
        var (destination, _) = conventionProvider.Get<TMessage>();

        await EnsureTopologyReady(cancellationToken);
        
        var result = await channel.BasicGetAsync(queue: queue ?? destination, autoAck: false, cancellationToken);
        if (result is null)
        {
            return;
        }
        var message = serializer.DeserializeBinary<TMessage>(result.Body.ToArray());
        SetMessageProperties(result.BasicProperties, result.Redelivered);

        await handle(message);
        
        await channel.BasicAckAsync(result.DeliveryTag, false, cancellationToken);
    }

    public async Task<IMessageConsumer> ConsumeMessageFromPartitions<TMessage>(
        IConsumerSpecificPartitioningSetup consumerPartitioningSetup,
        Func<TMessage, Task>? handle = default,
        string? queue = default,
        string[]? acceptedMessageTypes = default,
        CancellationToken cancellationToken = default) where TMessage : class, IMessage
    {
        ArgumentNullException.ThrowIfNull(consumerPartitioningSetup);

        if (consumerPartitioningSetup.PartitionNumbersToConsume.Count == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(consumerPartitioningSetup.PartitionNumbersToConsume), 
                "At least one partition must be provided.");
        }

        if (consumerPartitioningSetup.PartitionNumbersToConsume.Any(x => x < 1 || x > consumerPartitioningSetup.PartitioningOptions.NumberOfPartitions))
        {
            throw new ArgumentOutOfRangeException(
                nameof(consumerPartitioningSetup.PartitionNumbersToConsume), 
                $"Consumed partitions must be in range 1-{consumerPartitioningSetup.PartitioningOptions.NumberOfPartitions}!");
        }

        foreach (var partition in consumerPartitioningSetup.PartitionNumbersToConsume.Distinct())
        {
            var partitionQueue = PartitionName.ForQueue(queue, partition);
            await ConsumeMessage(handle, partitionQueue, acceptedMessageTypes, cancellationToken);
        }

        return this;
    }

    public async Task<IMessageConsumer> ConsumeNonGenericFromPartitions(
        IConsumerSpecificPartitioningSetup consumerPartitioningSetup,
        Func<MessageData, Task> handleRawPayload, 
        string? queue, 
        string[]? acceptedMessageTypes = default,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(consumerPartitioningSetup);

        if (consumerPartitioningSetup.PartitionNumbersToConsume.Count == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(consumerPartitioningSetup.PartitionNumbersToConsume), 
                "At least one partition must be provided.");
        }

        if (consumerPartitioningSetup.PartitionNumbersToConsume.Any(x => x < 1 || x > consumerPartitioningSetup.PartitioningOptions.NumberOfPartitions))
        {
            throw new ArgumentOutOfRangeException(
                nameof(consumerPartitioningSetup.PartitionNumbersToConsume), 
                $"Consumed partitions must be in range 1-{consumerPartitioningSetup.PartitioningOptions.NumberOfPartitions}!");
        }

        foreach (var partition in consumerPartitioningSetup.PartitionNumbersToConsume.Distinct())
        {
            var partitionQueue = PartitionName.ForQueue(queue, partition);
            await ConsumeNonGeneric(handleRawPayload, partitionQueue, acceptedMessageTypes, cancellationToken);
        }

        return this;
    }

    private async Task HandleMessageAsync<TMessage>(BasicDeliverEventArgs ea, Func<TMessage, Task>? handle = default, 
        CancellationToken cancellationToken = default) where TMessage : class, IMessage
    {
        var message = serializer.DeserializeBinary<TMessage>(ea.Body.ToArray());

        logger.LogWarning($"[{DateTime.UtcNow:O}] Received message:{Environment.NewLine} {message}");
        
        if (handle is null)
        {
            var scope = serviceProvider.CreateScope();
            var messageHandler = scope.ServiceProvider.GetRequiredService<IMessageHandler<TMessage>>();

            await messageHandler.HandleAsync(message, cancellationToken);
        }
        else
        {
            await handle(message);
        }
        
        logger.LogWarning($"[{DateTime.UtcNow:O}] Processed message:{Environment.NewLine} {message}");
    }
    
    private async Task OnHandleFailure<TMessage>(BasicDeliverEventArgs ea, IChannel channel, Exception exception,
            string destinationResolved, CancellationToken cancellationToken = default) where TMessage : class, IMessage
    {
        var messageId = GetMessageId(ea.BasicProperties);
        reliableConsuming.OnConsumeFailed(messageId);
                
        if (reliableConsuming.CanBrokerRetry(messageId))
        {
            // Let broker handle the retry
            logger.LogWarning("Consume failed for messageId: {messageId}; will retry via broker", messageId);
            await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: true, cancellationToken);
        }
        else
        {
            // Retries exhausted - say goodbye to the message
            logger.LogError("Broker retries limit exhausted for messageId: {messageId}", messageId);
            await channel.BasicRejectAsync(ea.DeliveryTag, false, cancellationToken);
                    
            var message = serializer.DeserializeBinary<TMessage>(ea.Body.ToArray());
            SetMessageProperties(ea.BasicProperties, ea.Redelivered);
            await reliableConsuming.OnBrokerRetriesExhausted(message, exception, destinationResolved);
        }
    }
    
    private async Task OnHandleFailureNonGeneric(BasicDeliverEventArgs ea, IChannel channel, Exception exception,
        string destinationResolved, CancellationToken cancellationToken = default)
    {
        var messageId = GetMessageId(ea.BasicProperties);
        reliableConsuming.OnConsumeFailed(messageId);
                
        if (reliableConsuming.CanBrokerRetry(messageId))
        {
            // Let broker handle the retry
            logger.LogWarning("Consume failed for messageId: {messageId}; will retry via broker", messageId);
            await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: true, cancellationToken);
        }
        else
        {
            // Retries exhausted - say goodbye to the message
            logger.LogError("Broker retries limit exhausted for messageId: {messageId}", messageId);
            await channel.BasicRejectAsync(ea.DeliveryTag, false, cancellationToken);
                    
            var message = serializer.DeserializeBinary<string>(ea.Body.ToArray());
            SetMessageProperties(ea.BasicProperties);
            await reliableConsuming.OnBrokerRetriesExhaustedNonGeneric(message, exception, destinationResolved);
        }
    }
    
    private static bool IsNotAcceptedMessageType(string[]? acceptedMessageTypes, BasicDeliverEventArgs ea)
    {
            return acceptedMessageTypes is not null && !acceptedMessageTypes.Contains(ea.BasicProperties.Type);
        }

    private void SetMessageProperties(IReadOnlyBasicProperties props, bool redelivered = false)
    {
        var messageId = props.MessageId;
        var headers = props.Headers?
            .Select(x => (x.Key, x.Value is byte[] bytes ? (object)Encoding.UTF8.GetString(bytes): x.Value.ToString())).ToDictionary();
            
        var messageType = props.Type;
        var messageProperties = new MessageProperties(messageId, headers, messageType, redelivered);
        messagePropertiesAccessor.Set(messageProperties);
    }

    private MessageData CreateMessageData(BasicDeliverEventArgs ea)
    {
        var messageId = GetMessageId(ea.BasicProperties);
        var messageType = ea.BasicProperties.Type;
        var payload = ea.Body.ToArray();
        
        return new MessageData(messageId, payload, messageType);
    }

    /// <summary>
    /// Build children span due to the -> https://github.com/jaegertracing/jaeger/issues/4516
    /// </summary>
    private Activity? CreateMessagingConsumeActivity(IReadOnlyBasicProperties props)
    {
        var isHeaderPresent = props.Headers?.ContainsKey(MessagingObservabilityHeaders.TraceParent) ?? false;

        if (isHeaderPresent is false)
        {
            return Activity.Current;
        }
        
        var traceIdBytes = props.Headers[MessagingObservabilityHeaders.TraceParent];
        var traceId = Encoding.UTF8.GetString((byte[]) traceIdBytes);
        var activitySource = new ActivitySource(MessagingActivitySources.MessagingConsumeSourceName);
        var parentContext = ActivityContext.Parse(traceId, default);
        return activitySource.StartActivity(
            $"Messaging Consume: {messagePropertiesAccessor.Get()?.MessageType}", 
            kind: ActivityKind.Consumer, 
            parentContext: parentContext,
            links: [new ActivityLink(parentContext)]);
    }

    private static Guid GetMessageId(IReadOnlyBasicProperties props)
    {
        var messageId = props.MessageId;
        return Guid.Parse(messageId);
    }

    private async Task ConfigureConsumerQosAsync(IChannel channel)
    {
        if (resiliencyOptions.Consumer.MaxMessagesFetchedPerConsumer > 0)
        {
            await channel.BasicQosAsync(
                prefetchSize: 0, 
                prefetchCount: (ushort)resiliencyOptions.Consumer.MaxMessagesFetchedPerConsumer, 
                global: false);
        }
        
    }
    
    private async Task EnsureTopologyReady(CancellationToken cancellationToken)
    {
        while (topologyReadinessAccessor.TopologyProvisioned is false)
        {
            logger.LogInformation("Waiting for topology to be provisioned...");
            await Task.Delay(1000, cancellationToken);
        }
    }
}