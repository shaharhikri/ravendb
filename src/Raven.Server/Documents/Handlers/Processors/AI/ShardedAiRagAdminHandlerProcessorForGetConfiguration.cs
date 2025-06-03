using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;


namespace Raven.Server.Documents.Handlers.Processors.AI;

internal sealed class ShardedAiRagAdminHandlerProcessorForGetConfiguration : AbstractDatabaseHandlerProcessorForGetConfiguration<ShardedDatabaseRequestHandler,
    TransactionOperationContext, AiRagConfiguration>
{
    public ShardedAiRagAdminHandlerProcessorForGetConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AiRagConfiguration GetConfiguration()
    {
        var agentid = RequestHandler.GetStringQueryString("agentId", required: true);
        var record = RequestHandler.DatabaseContext.DatabaseRecord;

        if (record == null ||
            record.AiRagConfigurations.TryGetValue(agentid, out var config) == false)
            return null;

        return config;
    }
}
