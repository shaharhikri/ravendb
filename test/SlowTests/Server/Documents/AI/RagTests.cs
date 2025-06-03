using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Amazon.Runtime.Internal.UserAgent;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.AI.AiGen;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI;

public class RagTests : RavenTestBase
{

    private static string s_openAiApiKey = Environment.GetEnvironmentVariable("RAVEN_AI_INTEGRATION_OPENAI_API_KEY");

    private static string name = AbstractChatCompletionClient.GetAllowedUniqueName(DateTime.UtcNow.ToString());

    private static string defaultJsonSchema = @"{
  ""name"": """ + name + @""",
  ""strict"": true,
  ""schema"": {
    ""type"": ""object"",
    ""properties"": {
      ""Blocked"": {
        ""type"": ""boolean""
      },
      ""Reason"": {
        ""type"": ""string"",
        ""description"": ""Concise reason for why this comment was marked as spam or ham""
      }
    },
    ""required"": [
      ""Blocked"",
      ""Reason""
    ],
    ""additionalProperties"": false
  }
}";

    public RagTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task RagClient()
    {
        using DocumentStore store = GetDocumentStore();

        var connectionString = new AiConnectionString()
        {
            Name = "openai_connection",
            OpenAiSettings = new OpenAiSettings()
            {
                ApiKey = s_openAiApiKey, 
                Model = "gpt-4o", 
                Endpoint = "https://api.openai.com/v1"
            }
        };

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));

        var user = new User { Id = "Users/1", Name = "Foo", Height = 180, Weight = 80 };

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(user);

            await session.StoreAsync(new User
            {
                Id = "Users/2",
                Name = "Bar",
                Height = 170,
                Weight = 70
            });

            await session.StoreAsync(new User
            {
                Id = "Users/3",
                Name = "Bob",
                Height = 160,
                Weight = 60
            });

            await session.StoreAsync(new User
            {
                Id = "Users/4",
                Name = "Alice",
                Height = 160,
                Weight = 50
            });

            await session.SaveChangesAsync();
        }

        var answers = new List<string>();

        var config = new AiRagConfiguration
        {
            ConnectionStringName = "openai_connection",
            SystemPrompt = "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.",
            OutputSchema = "{\"Answer\": \"Answer to the user question\", \"Relevant\": true, \"RelevantOrdersId\": [\"The order ids relevant to the query or response\"], \"MatchinProductsId\": [\"All the product ids referenced either by the user or the system\"] }",
            // Parameters = parameters,
            Persistence = new AiRagConfiguration.PersistenceConfiguration
            {
                Collection = "Chats",
                Expires = TimeSpan.FromHours(3)
            },
            Queries = new List<AiRagConfiguration.ToolQuery>
            {
                new AiRagConfiguration.ToolQuery
                {
                    Name = "ProductSearch",
                    Description = "semantic search the store product catalog",
                    Query = "from Products where vector.search(embedding.text(Name), $query)",
                    ParametersSchema = "{ \"query\": [\"term or phrase to search in the catalog\"] }"
                },
                new AiRagConfiguration.ToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    ParametersSchema = "{}"
                }
            }
        };
        var agentId = await store.AI.CreateAgentAsync(config);

        var response = await store.AI.StartChatAsync(agentId, "How much cheese do I eat?",
            p => p
                .AddParameter("customer", "customers/1-A")
                .AddParameter("now", DateTime.UtcNow.ToString())
                .AddParameter("location", user.Location)
        );

        response = await store.AI.ContinueChatAsync(response.ChatId, answers);

        await store.Maintenance.ForDatabase(store.Database).SendAsync(new RagOperation(config));

        // WaitForUserToContinueTheTest(store, false);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All,SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task RagConfig(Options options)
    {
        using DocumentStore store = GetDocumentStore(options);

        var connectionString1 = new AiConnectionString()
        {
            Name = "openai_connection_1",
            OpenAiSettings = new OpenAiSettings()
            {
                ApiKey = s_openAiApiKey,
                Model = "gpt-4o",
                Endpoint = "https://api.openai.com/v1"
            }
        };

        var connectionString2 = new AiConnectionString()
        {
            Name = "openai_connection_2",
            OpenAiSettings = new OpenAiSettings()
            {
                ApiKey = s_openAiApiKey,
                Model = "gpt-4o-mini",
                Endpoint = "https://api.openai.com/v1"
            }
        };

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString1));
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString2));

        var config1 = new AiRagConfiguration
        {
            ConnectionStringName = "openai_connection_1",
            SystemPrompt = "You are an AI agent of an online shop",
            OutputSchema = "{\"Answer\": \"Answer to the user question\", \"Relevant\": true, \"RelevantOrdersId\": [\"The order ids relevant to the query or response\"], \"MatchinProductsId\": [\"All the product ids referenced either by the user or the system\"] }",
            // Parameters = parameters,
            Persistence = new AiRagConfiguration.PersistenceConfiguration
            {
                Collection = "Chats",
                Expires = TimeSpan.FromHours(3)
            },
            Queries = new List<AiRagConfiguration.ToolQuery>
            {
                new AiRagConfiguration.ToolQuery
                {
                    Name = "ProductSearch",
                    Description = "semantic search the store product catalog",
                    Query = "from Products where vector.search(embedding.text(Name), $query)",
                    ParametersSchema = "{ \"query\": [\"term or phrase to search in the catalog\"] }"
                },
                new AiRagConfiguration.ToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    ParametersSchema = "{}"
                }
            },
            Parameters = new Dictionary<string, string>()
            {
                {"User", "Users/1A"},
                {"Company", "RavenDB"},
                {"Country", null},
            }
        };

        var config2 = new AiRagConfiguration
        {
            ConnectionStringName = "openai_connection_2",
            SystemPrompt = "You are a UI/UX designer",
            OutputSchema = "{\"Answer\": \"Answer to the user question\"}",
        };

        var config3 = new AiRagConfiguration
        {
            ConnectionStringName = "openai_connection_1",
            SystemPrompt = "You are a nurse.",
            OutputSchema = "{\"Answer\": \"Answer to the user question\"}",
        };

        var agent1 = await store.Maintenance.SendAsync(new ConfigureAiRagOperation(config1));
        var agent2 = await store.Maintenance.SendAsync(new ConfigureAiRagOperation("agent2", config2));
        var agent3 = await store.Maintenance.SendAsync(new ConfigureAiRagOperation(config3));

        Assert.Equal("agent2", agent2.AgentId);

        var res1 = await store.Maintenance.SendAsync(new GetAiRagConfigurationOperation(agent1.AgentId));
        var res2 = await store.Maintenance.SendAsync(new GetAiRagConfigurationOperation(agent2.AgentId));
        var res3 = await store.Maintenance.SendAsync(new GetAiRagConfigurationOperation(agent3.AgentId));

        Assert.True(res1.Equals(config1));
        Assert.True(res2.Equals(config2));
        Assert.True(res3.Equals(config3));

        WaitForUserToContinueTheTest(store, false);
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Height { get; set; }
        public int Weight { get; set; }
        public string Location { get; set; }
    }

    public sealed class RagOperation : IMaintenanceOperation
    {
        private AiRagConfiguration _config;

        public RagOperation(AiRagConfiguration config)
        {
            _config = config;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RagCommand(_config);
        }

        internal sealed class RagCommand : RavenCommand
        {
            private AiRagConfiguration _config;

            public RagCommand(AiRagConfiguration config)
            {
                _config = config;
            }
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/ai/rag/test";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Reset
                };
            }
        }
    }

}
