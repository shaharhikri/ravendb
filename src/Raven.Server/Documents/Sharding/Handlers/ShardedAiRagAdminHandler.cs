using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.AI;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedAiRagAdminHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/ai/rag/config", "GET")]
    public async Task GetRevisionsBinConfig()
    {
        using (var processor = new ShardedAiRagAdminHandlerProcessorForGetConfiguration(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/ai/rag/config", "POST")]
    public async Task ConfigRevisionsBinCleaner()
    {
        using (var processor = new ShardedAiRagAdminHandlerProcessorForPostConfiguration(this))
            await processor.ExecuteAsync();
    }
}
