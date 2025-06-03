using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.AI
{
    internal abstract class AbstractAiRagAdminHandlerProcessorForPostConfiguration<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        private string _agentId;

        protected AbstractAiRagAdminHandlerProcessorForPostConfiguration([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configurationJson,
            string raftRequestId)
        {
            _agentId = RequestHandler.GetStringQueryString("agentId", required: false);
            var database = RequestHandler.DatabaseName;

            var config = JsonDeserializationCluster.AiRagConfiguration(configurationJson);

            if (string.IsNullOrWhiteSpace(_agentId))
                _agentId = raftRequestId ?? Guid.NewGuid().ToString();

            var cmd = new AiRagConfigurationCommand(_agentId, config, database, raftRequestId);
            return ServerStore.SendToLeaderAsync(cmd);
        }

        protected override ValueTask OnAfterUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            RequestHandler.LogTaskToAudit(Web.RequestHandler.AiRagConfigTag, Index, configuration, additionalInfo: $"agent id '{_agentId}'");
            return ValueTask.CompletedTask;
        }

        protected override void OnBeforeResponseWrite(TransactionOperationContext _, DynamicJsonValue responseJson, BlittableJsonReaderObject configuration, long index)
        {
            responseJson["AgentId"] = _agentId;
        }
    }
}
