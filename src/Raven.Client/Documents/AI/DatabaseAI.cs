using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;

namespace Raven.Client.Documents.AI;

public class DatabaseAI
{
    private string _databaseName;
    private IDocumentStore _store;

    public DatabaseAI(IDocumentStore store, string databaseName = null)
    {
        _databaseName = databaseName ?? store.Database;
        _store = store;
    }

    public async Task<long> CreateAgentAsync(AiRagConfiguration aiRagConfiguration)
    {
        // Send aiRagConfiguration to server
        // the server will create 'agentId1234'
        // it will save it in db record under AiAgents, like that:
        // record:
        // {
        //     ...,
        //     "AiAgents":
        //     {
        //         "agentId1234" : aiRagConfiguration value
        //     }
        // }
        throw new NotImplementedException();
    }

    public async Task<RagAiResponse> StartChatAsync(long agentId, string prompt, Func<RagAiBuilder, RagAiBuilder> func)
    {
        var builder = func.Invoke(new RagAiBuilder());
        var parameters = builder.GetParameters();
        // parameters will be sent as part of the url params like 'prompt' and 'id'
        // send to "/databases/*/ai/rag/test" EP
        // the EP should get the configuration from the record by agentId, and perform the request

        throw new NotImplementedException();
    }

    public async Task<RagAiResponse> ContinueChatAsync(long responseChatId, IEnumerable<string> answers)
    {
        throw new NotImplementedException();
    }

    public long CreateAgent(AiRagConfiguration aiRagConfiguration)
    {
        throw new NotImplementedException();
    }

    public RagAiResponse StartChat(long agentId, string prompt, Func<RagAiBuilder, RagAiBuilder> func)
    {
        throw new NotImplementedException();
    }

    public RagAiResponse ContinueChat(long responseChatId, IEnumerable<string> answers)
    {
        throw new NotImplementedException();
    }

    public class RagAiBuilder
    {
        private readonly Dictionary<string, string> _parameters = new();

        public RagAiBuilder AddParameter(string key, string value)
        {
            _parameters[key] = value;
            return this;
        }

        public Dictionary<string, string> GetParameters() => _parameters;
    }

    public class RagAiResponse
    { 
        public long ChatId { get; set; } // ConversationId
        public string ResponseType { get; set; }
        public AiRagConfiguration.ToolAction[] Actions { get; set; }
        public string Answer { get; set; }
    }
}

