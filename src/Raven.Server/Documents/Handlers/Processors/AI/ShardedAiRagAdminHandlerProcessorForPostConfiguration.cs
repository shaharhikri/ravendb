using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.AI;

internal sealed class ShardedAiRagAdminHandlerProcessorForPostConfiguration : AbstractAiRagAdminHandlerProcessorForPostConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAiRagAdminHandlerProcessorForPostConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}

