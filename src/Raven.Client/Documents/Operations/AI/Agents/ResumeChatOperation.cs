using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents;
public class ResumeChatOperation<TSchema> : IMaintenanceOperation<ChatResult<TSchema>> where TSchema : new()
{
    private readonly string _chat;
    private readonly string _prompt;
    private readonly Dictionary<string, object> _parameters;

    public ResumeChatOperation(string chatId, string prompt, Dictionary<string, object> parameters = null)
    {
        ValidationMethods.AssertNotNullOrEmpty(chatId, nameof(chatId));
        ValidationMethods.AssertNotNullOrEmpty(prompt, nameof(prompt));

        _chat = chatId;
        _prompt = prompt;
        _parameters = parameters;
    }

    public RavenCommand<ChatResult<TSchema>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new ResumeChatOperationCommand(_chat, _prompt, _parameters, conventions);
    }

    internal sealed class ResumeChatOperationCommand : RavenCommand<ChatResult<TSchema>>
    {
        private readonly string _chat;
        private readonly string _prompt;
        private readonly Dictionary<string, object> _parameters;
        private readonly DocumentConventions _conventions;

        public ResumeChatOperationCommand(string chatId, string prompt, Dictionary<string, object> parameters, DocumentConventions conventions)
        {
            _chat = chatId;
            _prompt = prompt;
            _parameters = parameters;
            _conventions = conventions;
        }
        public override bool IsReadRequest => false;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent/resume?chat={_chat}";
            var body = new StartChatBody { Prompt = _prompt, Parameters = _parameters };

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await ctx.WriteAsync(stream, ctx.ReadObject(body.ToJson(),"chat-params")).ConfigureAwait(false);
                }, _conventions)
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            response.TryGet(nameof(ChatResult<TSchema>.Usage), out BlittableJsonReaderObject usage);
            response.TryGet(nameof(ChatResult<TSchema>.Response), out BlittableJsonReaderObject result);
            response.TryGet(nameof(ChatResult<TSchema>.ChatId), out string chatId);

            Result = new ChatResult<TSchema>
            {
                ChatId = chatId,
                Usage = JsonDeserializationClient.AiUsage(usage),
                Response = _conventions.Serialization.DefaultConverter.FromBlittable<TSchema>(result, chatId)
            };
        }
    }
}
