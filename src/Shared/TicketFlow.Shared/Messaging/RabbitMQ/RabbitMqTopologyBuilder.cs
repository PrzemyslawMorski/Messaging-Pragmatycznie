using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using TicketFlow.Shared.Messaging.Partitioning;
using TicketFlow.Shared.Messaging.Resiliency;
using TicketFlow.Shared.Messaging.Topology;

namespace TicketFlow.Shared.Messaging.RabbitMQ;

internal class RabbitMqTopologyBuilder(ChannelFactory channelFactory, ResiliencyOptions resiliencyOptions, ILogger<RabbitMqTopologyBuilder> logger) : ITopologyBuilder
{
    public const string PartitionKeyHeaderName = "hash-on";

    public async Task CreateTopologyAsync(
        string publisherSource,
        string consumerDestination,
        TopologyType topologyType,
        string filter = "",
        Dictionary<string, object>? consumerCustomArgs = default,
        PartitioningOptions? partitioningOptions = default,
        CancellationToken cancellationToken = default)
    {
        var channel = await channelFactory.CreateForConsumerAsync();

        consumerCustomArgs ??= new Dictionary<string, object>();
        
        await ConfigureDeadletterAsync(consumerDestination, consumerCustomArgs, channel);
        
        switch (topologyType)
        {
            case TopologyType.Direct:
                await CreateDirectAsync(publisherSource, consumerDestination, filter, consumerCustomArgs, channel,
                    cancellationToken: cancellationToken);
                break;
            case TopologyType.PublishSubscribe:
                if (partitioningOptions is null)
                {
                    await CreatePubSubAsync(publisherSource, consumerDestination, filter, consumerCustomArgs, channel,
                        cancellationToken: cancellationToken);
                }
                else
                {
                    await CreatePubSubWithPartitioningAsync(publisherSource, consumerDestination, filter, consumerCustomArgs, 
                        channel, partitioningOptions, cancellationToken: cancellationToken);
                }
                break;
            case TopologyType.PublisherToPublisher:
                await CreatePubToPubAsync(publisherSource, consumerDestination, filter, channel, cancellationToken: cancellationToken);
                break;
            default:
                throw new NotImplementedException($"{nameof(topologyType)} is not supported!");
        }
    }

    private async Task ConfigureDeadletterAsync(string consumerDestination, Dictionary<string, object> consumerCustomArgs, IChannel channel)
    {
        var dlqDeclaredByCaller = consumerCustomArgs.TryGetValue("x-dead-letter-exchange", out var dlqDeclared);
        var dlqExchange = dlqDeclared?.ToString();
        
        if (dlqDeclaredByCaller is false && resiliencyOptions.Consumer.UseDeadletter)
        {
            dlqExchange = consumerDestination + "-dlq-exchange";
            consumerCustomArgs.Add("x-dead-letter-exchange", dlqExchange);
        }

        // (Override) Caller explicitly set DLQ to be empty -> no DLQ
        if (dlqDeclaredByCaller && dlqExchange!.Equals(string.Empty))
        {
            consumerCustomArgs.Remove("x-dead-letter-exchange");
            return;
        }

        if (resiliencyOptions.Consumer.UseDeadletter && !string.IsNullOrEmpty(consumerDestination))
        {
            var dlqQueue = consumerDestination + "-dlq";
            await CreatePubSubAsync(dlqExchange, dlqQueue, "", null!, channel);
        }
    }

    private async Task CreateDirectAsync(
        string publisherSource,
        string consumerDestination,
        string filter,
        Dictionary<string, object> consumerCustomArgs,
        IChannel channel,
        CancellationToken cancellationToken = default)
    {
        if (!string.IsNullOrEmpty(publisherSource))
        {
            logger.LogInformation($"Declaring exchange of name {publisherSource}");
            await channel.ExchangeDeclareAsync(publisherSource, ExchangeType.Direct, durable: true, cancellationToken: cancellationToken);
        }
        else
        {
            logger.LogInformation("Skipping publisher and binding due to direct publish to queue");
        }

        logger.LogInformation($"Declaring queue of name {consumerDestination}");
        await channel.QueueDeclareAsync(consumerDestination, durable: true, exclusive: false, autoDelete: false, consumerCustomArgs, cancellationToken: cancellationToken);

        if (!string.IsNullOrEmpty(publisherSource))
        {
            await channel.QueueBindAsync(queue: consumerDestination, exchange: publisherSource, routingKey: filter, cancellationToken: cancellationToken);
        }
    }

    private async Task CreatePubSubAsync(
        string publisherSource,
        string consumerDestination,
        string filter,
        Dictionary<string, object> consumerCustomArgs,
        IChannel channel,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation($"Declaring exchange of name {publisherSource}");
        await channel.ExchangeDeclareAsync(publisherSource, ExchangeType.Topic, durable: true, cancellationToken: cancellationToken);

        if (string.IsNullOrEmpty(consumerDestination))
        {
            logger.LogInformation($"In {nameof(TopologyType.PublishSubscribe)} publisher is consumer-ignorant; skipping consumer creation and binding...");
            return;
        }
        
        logger.LogInformation($"Declaring queue of name {consumerDestination}");
        await channel.QueueDeclareAsync(consumerDestination, durable: true, exclusive: false, autoDelete: false, 
            consumerCustomArgs, cancellationToken: cancellationToken);
        
        await channel.QueueBindAsync(queue: consumerDestination, exchange: publisherSource, 
            routingKey: string.IsNullOrEmpty(filter) 
                ? "#"       // Broadcast like fanout
                : filter    // custom filter pattern with substitute chars ('#' or '*')
        , cancellationToken: cancellationToken);
    }

    private async Task CreatePubSubWithPartitioningAsync(
        string publisherSource,
        string consumerDestination,
        string filter,
        Dictionary<string, object> consumerCustomArgs,
        IChannel channel,
        PartitioningOptions? partitioningOptions = default,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation($"Declaring exchange of name {publisherSource}");
        await channel.ExchangeDeclareAsync(publisherSource, ExchangeType.Topic, durable: true, cancellationToken: cancellationToken);

        if (string.IsNullOrEmpty(consumerDestination))
        {
            logger.LogInformation($"In {nameof(TopologyType.PublishSubscribe)} publisher is consumer-ignorant; skipping consumer creation and binding...");
            return;
        }
        
        logger.LogInformation($"Requested partitioned topology for {consumerDestination}");
        var partitionedExchange = PartitionName.ForConsumerDedicatedExchange(consumerDestination);
        logger.LogInformation($"Declaring dedicated exchange for consumer with name {consumerDestination}");
        CreatePubToPubAsync(publisherSource, partitionedExchange, filter, channel, forPartitioning: true);

        var consumerCustomArgsForPartitioning = new Dictionary<string, object>(consumerCustomArgs);
        consumerCustomArgsForPartitioning.Add("x-single-active-consumer", partitioningOptions.OnlyOneActiveConsumerPerPartition);
            
        logger.LogInformation($"Requested {partitioningOptions.NumberOfPartitions} partitions for {consumerDestination}");
            
        foreach (var partitionNum in Enumerable.Range(1, partitioningOptions.NumberOfPartitions))
        {
            var partitionedQueueName = PartitionName.ForQueue(consumerDestination, partitionNum);
            logger.LogInformation($"Declaring queue of name {partitionedQueueName}");
                
            await channel.QueueDeclareAsync(partitionedQueueName, durable: true, exclusive: false, autoDelete: false, 
                consumerCustomArgsForPartitioning, cancellationToken: cancellationToken);
            
            await channel.QueueBindAsync(queue: partitionedQueueName, exchange: partitionedExchange, 
                routingKey: "1" /* Let's assume that each consumer has same weight, so they get partitions split evenly */
            , cancellationToken: cancellationToken);
        }
    }

    private async Task CreatePubToPubAsync(string publisherSource, string consumerDestination, string filter, IChannel channel,
        bool forPartitioning = false, CancellationToken cancellationToken = default)
    {
        logger.LogInformation($"Declaring exchange of name {publisherSource}");
        await channel.ExchangeDeclareAsync(publisherSource, ExchangeType.Topic, durable: true, cancellationToken: cancellationToken);
            
        logger.LogInformation($"Declaring exchange of name {consumerDestination}");
        if (!forPartitioning)
        {
            await channel.ExchangeDeclareAsync(consumerDestination, ExchangeType.Topic, durable: true, cancellationToken: cancellationToken);
        }
        else
        {
            await channel.ExchangeDeclareAsync(
                consumerDestination,
                "x-consistent-hash",
                durable: true,
                arguments: new Dictionary<string, object>
                {
                    {
                        "hash-header", PartitionKeyHeaderName
                    }
                },
                cancellationToken: cancellationToken);
        }

        await channel.ExchangeBindAsync(source: publisherSource, destination: consumerDestination, routingKey: string.IsNullOrEmpty(filter) 
                ? "#"       // Broadcast like fanout
                : filter    // custom filter pattern with substitute chars ('#' or '*')
        , cancellationToken: cancellationToken);
    }
}