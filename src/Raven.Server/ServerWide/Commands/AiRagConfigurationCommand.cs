using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class AiRagConfigurationCommand : UpdateDatabaseCommand
    {
        public AiRagConfiguration Configuration;

        public string AgentId;

        public AiRagConfigurationCommand()
        {
            // for deserialization
        }

        public AiRagConfigurationCommand(string agentId, AiRagConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
            AgentId = agentId;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (AgentId == null)
            {
                Debug.Assert(false); // Should never happen
                return;
            }

            record.AiRagConfigurations ??= new Dictionary<string, AiRagConfiguration>();
            record.AiRagConfigurations[AgentId] = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(AgentId)] = AgentId;
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
