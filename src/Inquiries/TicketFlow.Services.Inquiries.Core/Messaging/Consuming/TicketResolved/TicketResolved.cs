using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Inquiries.Core.Messaging.Consuming.TicketResolved;

public record TicketResolved(Guid TicketId, int Version) : IMessage;

