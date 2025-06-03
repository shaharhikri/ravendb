using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.AI;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.AI;

internal sealed class AiRagAdminHandlerProcessorForPostConfiguration : AbstractAiRagAdminHandlerProcessorForPostConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AiRagAdminHandlerProcessorForPostConfiguration([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}

