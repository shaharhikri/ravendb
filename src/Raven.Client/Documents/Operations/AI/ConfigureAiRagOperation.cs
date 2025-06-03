using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI
{
    public class ConfigureAiRagOperation : IMaintenanceOperation<ConfigureAiRagOperationResult>
    {
        private readonly string _agentId;
        private readonly AiRagConfiguration _configuration;

        public ConfigureAiRagOperation(AiRagConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public ConfigureAiRagOperation(string agentId, AiRagConfiguration configuration)
        {
            _agentId = agentId ?? throw new ArgumentNullException(nameof(agentId));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand<ConfigureAiRagOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ConfigureAiRagCommand(conventions, _agentId, _configuration);
        }

        private sealed class ConfigureAiRagCommand : RavenCommand<ConfigureAiRagOperationResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly string _agentId;
            private readonly AiRagConfiguration _configuration;

            public ConfigureAiRagCommand(DocumentConventions conventions, string agentId, AiRagConfiguration configuration)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _agentId = agentId;
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/ai/rag/config";
                if(_agentId!=null)
                    url += $"?agentId={_agentId}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content =
                        new BlittableJsonContent(
                            async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx))
                                .ConfigureAwait(false), _conventions)
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigureAiRagOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

    }
}
