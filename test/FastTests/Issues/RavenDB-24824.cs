using System;
using Raven.Client.Documents.AI;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues;

public class RavenDB_24824 : RavenTestBase
{
    public RavenDB_24824(ITestOutputHelper output) : base(output)
    {
    }

    private class AlternativeSchema
    {
        public string Summary { get; set; }
        public int Score { get; set; }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void NoSchemaWithStructuredTypeShouldThrow()
    {
        using var store = GetDocumentStore();

        var chat = store.AI.Conversation("agents/test", "chats/123", new AiConversationCreationOptions());
        chat.SetUserPrompt("hello");

        var ex = Assert.Throws<InvalidOperationException>(() =>
            chat.Run<AlternativeSchema>(new AiOutputOptions { NoSchema = true }));

        Assert.Contains(nameof(AiOutputOptions.NoSchema), ex.Message);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void StringWithExplicitSchemaShouldThrow()
    {
        using var store = GetDocumentStore();

        var chat = store.AI.Conversation("agents/test", "chats/123", new AiConversationCreationOptions());
        chat.SetUserPrompt("hello");

        var ex = Assert.Throws<InvalidOperationException>(() =>
            chat.Run<string>(new AiOutputOptions { OutputSchema = "{}" }));

        Assert.Contains("raw string", ex.Message);
    }
}
