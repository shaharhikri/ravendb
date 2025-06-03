using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.AI;
internal class AiRagAdminHandlerProcessorForGetConfiguration : AbstractDatabaseHandlerProcessorForGetConfiguration<DatabaseRequestHandler, DocumentsOperationContext, AiRagConfiguration>
{
    public AiRagAdminHandlerProcessorForGetConfiguration([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AiRagConfiguration GetConfiguration()
    {
        var agentid = RequestHandler.GetStringQueryString("agentId", required: true);

        using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            using (var rawRecord = RequestHandler.Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
            {
                if (rawRecord == null || 
                    rawRecord.AiRagConfigurations.TryGetValue(agentid, out var config) == false)
                    return null;

                return config;
            }
        }
    }
}

