using TicketFlow.Services.Inquiries.Core.Data.Repositories;
using TicketFlow.Shared.Exceptions;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Inquiries.Core.Messaging.Consuming.TicketResolved;

public class TicketResolvedHandler(IInquiriesRepository inquiriesRepository) : IMessageHandler<TicketResolved>
{
    public async Task HandleAsync(TicketResolved message, CancellationToken cancellationToken = default)
    {
        var inquiry = await inquiriesRepository.GetByTicketIdAsync(message.TicketId, cancellationToken);

        if (inquiry == null)
        {
            // Inquiry might not exist or might not be linked to a ticket yet
            // This is not necessarily an error - just log and return
            return;
        }
        
        inquiry.Close();
        await inquiriesRepository.UpdateAsync(inquiry, cancellationToken);
    }
}

