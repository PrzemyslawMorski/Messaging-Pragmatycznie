namespace TicketFlow.Services.Inquiries.Core.Messaging.Consuming.Demultiplexing;

public record FallbackTicketChangeEvent(Guid TicketId, int Version) : ITicketChange;


