using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Inquiries.Core.Messaging.Consuming;

public record TicketResolved(Guid TicketId, int Version) : IMessage, ITicketChange;


