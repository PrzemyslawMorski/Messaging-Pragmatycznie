using RabbitMQ.Client;
using TicketFlow.CourseUtils;
using TicketFlow.Shared.Messaging.Partitioning;
using TicketFlow.Shared.Messaging.Resiliency;
using TicketFlow.Shared.Serialization;

namespace TicketFlow.Shared.Messaging.RabbitMQ;

internal sealed class RabbitMqMessagePublisher(ChannelFactory channelFactory, IMessagePublisherConventionProvider conventionProvider, 
    ISerializer serializer, MessagePropertiesAccessor propertiesAccessor, ReliablePublishing reliablePublishing) : IMessagePublisher
{
    public async Task PublishAsync<TMessage>(TMessage message, string? destination = default, string? routingKey = default, string? messageId = default,
        IDictionary<string, object>? headers = default, CancellationToken cancellationToken = default) where TMessage : class, IMessage
    {
        var channelOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: reliablePublishing.UsePublisherConfirms, 
            publisherConfirmationTrackingEnabled: reliablePublishing.UsePublisherConfirms);
        var channel = await channelFactory.CreateForProducerAsync(channelOptions);
        var payload = serializer.SerializeBinary(message);
        var properties = CreateMessageProperties<TMessage>(messageId, headers);
        SetPartitionKey(properties, message);
        
        var (conventionDestination, conventionRoutingKey) = conventionProvider.Get<TMessage>();
        
        ConfigureReliablePublishing<TMessage>(channel, messageId);
        
        await channel.BasicPublishAsync(
            exchange: destination ?? conventionDestination,
            routingKey: routingKey ?? conventionRoutingKey,
            basicProperties: properties,
            body: payload,
            mandatory: reliablePublishing.ShouldPublishAsMandatory<TMessage>(),
            cancellationToken: cancellationToken);
        
        
    }

    private BasicProperties CreateMessageProperties<TMessage>(string?  messageId = default, IDictionary<string, object>? headers = default)
        where TMessage : class,IMessage
    {
        var messageProperties = propertiesAccessor.Get();
        var basicProperties = new BasicProperties
        {
            MessageId = messageId ?? Guid.NewGuid().ToString(),
            Type = MessageTypeName.CreateFor<TMessage>(),
            DeliveryMode = DeliveryModes.Persistent,
            Headers = new Dictionary<string, object>()
        };

        var headersToAdd = headers 
                           ?? messageProperties?.Headers 
                           ?? Enumerable.Empty<KeyValuePair<string, object>>();

        foreach (var header in headersToAdd)
        {
            basicProperties.Headers.Add(header.Key, header.Value.ToString());
        }
        
        return basicProperties;
    }

    private void ConfigureReliablePublishing<TMessage>(IChannel channel, string? messageId)
    {
        if (reliablePublishing.UsePublisherConfirms)
        {
            channel.BasicNacksAsync += async (s, args) =>
            {
                Console.WriteLine(
                    $"Message {typeof(TMessage).Name}, id: {messageId} was not accepted by a broker!)");
            };
        }
        
        if (reliablePublishing.ShouldPublishAsMandatory<TMessage>())
        {
            channel.BasicReturnAsync += async (s, args) =>
            {
                Console.WriteLine($"Message {typeof(TMessage).Name}, id: {messageId} was not routed properly to any consumer!)");
            };
        }
    }

    private void SetPartitionKey<TMessage>(IBasicProperties basicProperties, TMessage? message)
    {
        if (FeatureFlags.UsePartitioningExample is false)
        {
            return;
        }

        if (typeof(IMessageWithPartitionKey).IsAssignableFrom(typeof(TMessage)))
        {
            var partitionKey = (message as IMessageWithPartitionKey)?.PartitionKey;
            basicProperties.Headers.Add(RabbitMqTopologyBuilder.PartitionKeyHeaderName, partitionKey);
        }
    }
}