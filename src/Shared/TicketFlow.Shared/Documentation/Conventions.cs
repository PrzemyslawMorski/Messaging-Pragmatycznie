using TicketFlow.Shared.Messaging;

namespace TicketFlow.Shared.AsyncAPI;

public static class Conventions
{
    public static class Ref
    {
        public const string ChannelPrefix = "#/channels/";
        public static string Channel(string name) => $"{ChannelPrefix}{name}";
        public const string ComponentMessagePrefix = "#/components/messages/";
        public static string ComponentMessage(string name) => $"{ComponentMessagePrefix}{name}";
        public static string ChannelMessage(string channelName, string messageName) => $"#/channels/{channelName}/messages/{messageName}";   
    }
    
    public static class Operation
    {
        public const string PublishPrefix = "Publish";
        public const string ConsumePrefix = "Consume";
    
        public static string Publish(string messageType) => $"{PublishPrefix}{messageType}";
        public static string Consume(string messageType) => $"{ConsumePrefix}{messageType}";   
        public static string Publish<TMessage>() where TMessage : IMessage => Publish(typeof(TMessage).Name);
        public static string Consume<TMessage>() where TMessage : IMessage => Consume(typeof(TMessage).Name);   
    }
    
    public static class Channel
    {
        public static string Name(string messageType) => messageType;
        public static string Name<TMessage>() where TMessage : IMessage => Name(typeof(TMessage).Name);
        public static string DereferenceNameFromChannelRef(string reference) => reference.Replace(Ref.ChannelPrefix, "");
    }

    public static class Message
    {
        public static string FromChannelName(string channelName) => channelName;
    }
}