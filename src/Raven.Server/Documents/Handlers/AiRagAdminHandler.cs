using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.AI;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers;

public class AiRagAdminHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/ai/rag/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetRevisionsBinConfig()
    {
        using (var processor = new AiRagAdminHandlerProcessorForGetConfiguration(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/admin/ai/rag/config", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task ConfigRevisionsBinCleaner()
    {
        using (var processor = new AiRagAdminHandlerProcessorForPostConfiguration(this))
            await processor.ExecuteAsync();
    }
}

