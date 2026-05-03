using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.AI;

namespace Raven.Server.Documents.Handlers.AI.Agents;

/// <summary>
/// Resolved output-schema for a single conversation turn.
/// Created once via <see cref="Create"/> — that is the single place where
/// the agent configuration and the per-call <see cref="AiOutputOptions"/> are combined.
/// </summary>
public sealed class AiSchema
{
    /// <summary>
    /// The schema string to send to the LLM.
    /// <c>null</c> when <see cref="IsStructuredOutput"/> is <c>false</c> (NoSchema mode).
    /// </summary>
    public string Schema { get; }

    /// <summary>
    /// <c>true</c> when the LLM response should be parsed as structured JSON;
    /// <c>false</c> when NoSchema was requested (free-form text).
    /// </summary>
    public bool IsStructuredOutput { get; }

    private AiSchema(string schema, bool isStructuredOutput)
    {
        Schema = schema;
        IsStructuredOutput = isStructuredOutput;
    }

    /// <summary>
    /// Creates the <see cref="AiSchema"/> for a turn by merging the agent's own
    /// schema/sample-object with the per-call <paramref name="userOptions"/>.
    /// Priority (highest first): NoSchema → OutputSchema → SampleObject → agent default.
    /// </summary>
    public static AiSchema Create(AiAgentConfiguration agentConfig, AiOutputOptions userOptions)
    {
        if (userOptions?.NoSchema == true)
            return new AiSchema(schema: null, isStructuredOutput: false);

        if (string.IsNullOrWhiteSpace(userOptions?.OutputSchema) == false)
            return new AiSchema(userOptions.OutputSchema, isStructuredOutput: true);

        if (userOptions?.SampleObject != null)
            return new AiSchema(ChatCompletionClient.GetSchemaFromSampleObject(userOptions.SampleObject.ToString()), isStructuredOutput: true);

        // Fall back to the agent-level schema / sample-object.
        string schema = ChatCompletionClient.GetSchemaForRequest(agentConfig?.OutputSchema, agentConfig?.SampleObject);
        return new AiSchema(schema, isStructuredOutput: true);
    }
}
