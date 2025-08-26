using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsGoogleConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsGoogleConnectorForTesting>
{
    private const string Model = "text-embedding-004";

    public EmbeddingsGoogleConnectorForTesting()
    {
        RequiredEnvironmentVariables = [GoogleConnectorHelper.EnvironmentVariable];
    }
    
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.Google);

    protected override AiConnectionString CreateAiConnectionStringImpl() => GoogleConnectorHelper.CreateAiConnectionString(Model, AiModelType.Chat);

}

public class GenAiGoogleConnectorForTesting : AbstractGenAiConnectorForTesting<GenAiGoogleConnectorForTesting>
{
    private const string Model = "gemini-2.5-flash";

    public GenAiGoogleConnectorForTesting()
    {
        RequiredEnvironmentVariables = [GoogleConnectorHelper.EnvironmentVariable];
    }
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.Google);

    protected override AiConnectionString CreateAiConnectionStringImpl() => GoogleConnectorHelper.CreateAiConnectionString(Model, AiModelType.Chat);
}

internal static class GoogleConnectorHelper
{
    public const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_GOOGLE_API_KEY";
    private const string Endpoint = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"; 
    //"https://generativelanguage.googleapis.com/v1beta/openai/chat/completions";

    public static AiConnectionString CreateAiConnectionString(string model, AiModelType modelType)
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariable);
        var endPoint = Endpoint + $"?key={apiKey}";
        return new AiConnectionString
        {
            ModelType = modelType,
            GoogleSettings = new GoogleSettings(model, apiKey, endPoint: endPoint),
        };
    }
}
