namespace Raven.Client.Documents.AI;

/// <summary>
/// Controls the output format for a single AI conversation turn,
/// allowing callers to override the agent's default output schema.
/// </summary>
public class AiOutputOptions
{
    /// <summary>
    /// A sample object used to generate a JSON schema for structured output.
    /// You can provide either a .NET object instance (which will be serialized to JSON and sent to the server)
    /// or a JSON string representing the sample object directly.
    /// In both cases, the server converts the sample object to a JSON schema at request time.
    /// </summary>
    public object SampleObject { get; set; }

    /// <summary>
    /// An explicit JSON schema string for structured output.
    /// Takes precedence over <see cref="SampleObject"/> if both are set.
    /// </summary>
    public string OutputSchema { get; set; }

    /// <summary>
    /// When true, disables structured output entirely.
    /// The LLM returns free-form text instead of JSON conforming to a schema.
    /// </summary>
    public bool NoSchema { get; set; }
}
