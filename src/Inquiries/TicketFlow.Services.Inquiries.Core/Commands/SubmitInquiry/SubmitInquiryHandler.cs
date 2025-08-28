using Microsoft.Extensions.Logging;
using Neuroglia.AsyncApi.v3;
using TicketFlow.Services.Inquiries.Core.Data.Models;
using TicketFlow.Services.Inquiries.Core.Data.Repositories;
using TicketFlow.Services.Inquiries.Core.LanguageDetection;
using TicketFlow.Services.Inquiries.Core.Messaging.Publishing;
using TicketFlow.Shared.AsyncAPI;
using TicketFlow.Shared.Commands;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Inquiries.Core.Commands.SubmitInquiry;

internal sealed class SubmitInquiryHandler(IInquiriesRepository repository, ILanguageDetector languageDetector, 
    IMessagePublisher messagePublisher, ILogger<SubmitInquiryHandler> logger) : ICommandHandler<SubmitInquiry>
{
    private const string EnglishLanguageCode = "en";
    public async Task HandleAsync(SubmitInquiry command, CancellationToken cancellationToken = default)
    {
        var (name, email, title, description, category) = command;
        var categoryParsed = ParseCategory(category);
        var inquiry = new Inquiry(name, email, title, description, categoryParsed);
        
        await repository.AddAsync(inquiry, cancellationToken);
        var languageCode = await languageDetector.GetTextLanguageCode(inquiry.Description, cancellationToken);

        var inquiryReportedMessage = new InquirySubmitted(
            inquiry.Id,
            inquiry.Name,
            inquiry.Email,
            inquiry.Title,
            inquiry.Description,
            inquiry.Category.ToString(),
            languageCode,
            inquiry.CreatedAt);
        await PublishInquirySubmitted(cancellationToken, inquiryReportedMessage);
        
        logger.LogInformation($"Inquiry with id: {inquiry.Id} submitted successfully.");
        
        if (languageCode is not EnglishLanguageCode)
        {
            var requestTranslationV1 = new RequestTranslationV1(inquiry.Description, inquiry.Id);
            var requestTranslationV2 = new RequestTranslationV2(inquiry.Description, languageCode, inquiry.Id);
            
            await PublishRequestTranslationV1(cancellationToken, requestTranslationV1);
            await PublishRequestTranslationV2(cancellationToken, requestTranslationV2);
            
            logger.LogInformation($"Translation for inquiry with id: {inquiry.Id} has been requested.");
        }
    }

    [Operation(Conventions.Operation.PublishPrefix + "InquirySubmitted", V3OperationAction.Send, Conventions.Ref.ChannelPrefix + "InquirySubmitted", Description = "Notify that inquiry was submitted")]
    private async Task PublishInquirySubmitted(CancellationToken cancellationToken, InquirySubmitted inquiryReportedMessage)
    {
        await messagePublisher.PublishAsync(inquiryReportedMessage, cancellationToken: cancellationToken);
    }
    
    [Operation(Conventions.Operation.PublishPrefix + "RequestTranslationV1", V3OperationAction.Send, Conventions.Ref.ChannelPrefix + "RequestTranslationV1", Description = "Notify that translation was requested (V1)")]
    private async Task PublishRequestTranslationV1(CancellationToken cancellationToken,
        RequestTranslationV1 requestTranslationV1)
    {
        await messagePublisher.PublishAsync(requestTranslationV1, destination: "", routingKey: "request-translation-v1-queue", cancellationToken: cancellationToken);
    }
    
    [Operation(Conventions.Operation.PublishPrefix + "RequestTranslationV2", V3OperationAction.Send, Conventions.Ref.ChannelPrefix + "RequestTranslationV2", Description = "Notify that translation was requested (V2)")]
    private async Task PublishRequestTranslationV2(CancellationToken cancellationToken,
        RequestTranslationV2 requestTranslationV2)
    {
        await messagePublisher.PublishAsync(requestTranslationV2, destination: "", routingKey: "request-translation-v2-queue", cancellationToken: cancellationToken);
    }

    private static InquiryCategory ParseCategory(string category)
    {
        CapitalizeInput();
        var parseSucceeded = Enum.TryParse<InquiryCategory>(category, out var categoryParsed);
        
        if (!parseSucceeded)
        {
            categoryParsed = InquiryCategory.Other;
        }

        return categoryParsed;

        void CapitalizeInput()
        {
            category = category[0].ToString().ToUpper() + category.Substring(1, category.Length - 1);
        }
    }
}