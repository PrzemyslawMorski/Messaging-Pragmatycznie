using Neuroglia.AsyncApi.v3;

namespace TicketFlow.Shared.AsyncAPI;

public class TopologyDescription
{
    private readonly Dictionary<string, V3ChannelDefinition> _channels = new();
    private readonly Dictionary<string, V3MessageDefinition> _messages = new();
    private readonly Dictionary<string, V3OperationDefinition> _operations = new();
    private bool _consumersRegistered;

    public void AddOperation(string name, V3OperationDefinition operation)
    {
        _operations.TryAdd(name, operation);
    }

    public void AddMessage(string name, V3MessageDefinition message)
    {
        _messages.TryAdd(name, message);
    }

    public void AddChannel(string name, V3ChannelDefinition channel)
    {
        _channels.TryAdd(name, channel);
    }

    public void MarkConsumersRegistered()
    {
        _consumersRegistered = true;
    }
    
    public bool TopologyDescribed => _consumersRegistered;
    public IReadOnlyDictionary<string, V3ChannelDefinition> Channels
    {
        get
        {
            if (TopologyDescribed is false)
            {
                throw new InvalidOperationException("Topology is not described yet");   
            }
            return _channels;
        }
    }

    public IReadOnlyDictionary<string, V3MessageDefinition> Messages
    {
        get
        {
            if (TopologyDescribed is false)
            {
                throw new InvalidOperationException("Topology is not described yet");   
            }
            return _messages;
        }
    }

    public IReadOnlyDictionary<string, V3OperationDefinition> Operations
    {
        get
        {
            if (TopologyDescribed is false)
            {
                throw new InvalidOperationException("Topology is not described yet");   
            }
            return _operations;
        }
    }
}