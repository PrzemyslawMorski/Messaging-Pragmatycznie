using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Inquiries.Core.Messaging.Consuming;

public record TicketQualified(Guid TicketId, int Version) : IMessage, ITicketChange;


