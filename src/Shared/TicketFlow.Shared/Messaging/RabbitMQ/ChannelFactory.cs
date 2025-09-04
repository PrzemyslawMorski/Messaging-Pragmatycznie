using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using TicketFlow.Shared.App;

namespace TicketFlow.Shared.Messaging.RabbitMQ;

internal sealed class ChannelFactory(Func<Task<ConnectionProvider>> createConnectionProvider) : IDisposable
{
    private readonly ThreadLocal<IChannel> _consumerCache = new(true);
    private readonly ThreadLocal<IChannel> _producerCache = new(true);
    private ConnectionProvider? _connectionProvider;

    public Task<IChannel> CreateForProducerAsync(CreateChannelOptions? options = null) => Create(x => x.ProducerConnection, _producerCache, options);
    
    public Task<IChannel> CreateForConsumerAsync(CreateChannelOptions? options = null) => Create(x => x.ConsumerConnection, _consumerCache, options);
    
    private async Task<IChannel> Create(Func<ConnectionProvider, IConnection> selectConnection, ThreadLocal<IChannel> cache, CreateChannelOptions? options = null)
    {
        _connectionProvider ??= await createConnectionProvider();
        
        if (cache.Value is not null)
        {
            return cache.Value;
        }
        
        var channel = await selectConnection(_connectionProvider).CreateChannelAsync(options);
        cache.Value = channel;
        return channel;
    }
    
    public void Dispose()
    {
        foreach (var channel in _consumerCache.Values)
        {
            channel.Dispose();
        }
        foreach (var channel in _producerCache.Values)
        {
            channel.Dispose();
        }
        
        _consumerCache.Dispose();
        _producerCache.Dispose();
    }
}
