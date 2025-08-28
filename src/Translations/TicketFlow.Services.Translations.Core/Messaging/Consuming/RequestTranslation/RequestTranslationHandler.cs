using Neuroglia.AsyncApi.v3;
using TicketFlow.Services.Translations.Core.Messaging.Publishing;
using TicketFlow.Services.Translations.Core.Translations;
using TicketFlow.Shared.AsyncAPI;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Translations.Core.Messaging.Consuming.RequestTranslation;

internal sealed class RequestTranslationHandler(ITranslationsService translationsService, IMessagePublisher messagePublisher) 
    : IMessageHandler<RequestTranslationV1>, IMessageHandler<RequestTranslationV2>
{
    public Task HandleAsync(RequestTranslationV1 message, CancellationToken cancellationToken = default)
        => HandleAsync(message.Text, default, TranslationLanguage.English, message.TicketId, cancellationToken);

    public Task HandleAsync(RequestTranslationV2 message, CancellationToken cancellationToken = default)
        => HandleAsync(message.Text, message.LanguageCode, TranslationLanguage.English, message.ReferenceId, cancellationToken);
    
    private async Task HandleAsync(string text, string translateFrom, string languageCode, Guid referenceId, CancellationToken cancellationToken = default)
    {
        var translatedText = await translationsService.TranslateAsync(text, translateFrom, languageCode, cancellationToken);

        if (string.IsNullOrWhiteSpace(translatedText))
        {
            var translationSkippedMessage = new TranslationSkipped(text, referenceId);
            await PublishTranslationSkipped(cancellationToken, translationSkippedMessage);
            return;
        }
        
        var translationCompletedMessage = new TranslationCompleted(text, translatedText, referenceId);
        await PublishTranslationCompleted(cancellationToken, translationCompletedMessage);
    }

    [Operation(Conventions.Operation.PublishPrefix + "TranslationCompleted", V3OperationAction.Send, Conventions.Ref.ChannelPrefix + "TranslationCompleted", Description = "Notify that translation was completed")]
    private async Task PublishTranslationCompleted(CancellationToken cancellationToken,
        TranslationCompleted translationCompletedMessage)
    {
        await messagePublisher.PublishAsync(translationCompletedMessage, destination: "translation-completed-exchange", cancellationToken: cancellationToken);
    }

    [Operation(Conventions.Operation.PublishPrefix + "TranslationSkipped", V3OperationAction.Send, Conventions.Ref.ChannelPrefix + "TranslationSkipped", Description = "Notify that translation was skipped")]
    private async Task PublishTranslationSkipped(CancellationToken cancellationToken,
        TranslationSkipped translationSkippedMessage)
    {
        await messagePublisher.PublishAsync(translationSkippedMessage, destination: "translation-completed-exchange", cancellationToken: cancellationToken);
    }
}