using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

public class GetAiRagConfigurationOperation : IMaintenanceOperation<AiRagConfiguration>
{
    private readonly string _agentId;

    public GetAiRagConfigurationOperation(string agentId)
    {
        _agentId = agentId ?? throw new ArgumentNullException(nameof(agentId));
    }

    public RavenCommand<AiRagConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetAiRagConfigurationCommand(_agentId);
    }

    private sealed class GetAiRagConfigurationCommand(string agentId) : RavenCommand<AiRagConfiguration>
    {
        private readonly string _agentId = agentId;

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/rag/config?agentId={_agentId}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.AiRagConfiguration(response);
        }
    }
}
