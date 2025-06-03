namespace Raven.Client.Documents.Operations.AI
{
    public sealed class ConfigureAiRagOperationResult
    {
        public long? RaftCommandIndex { get; set; }
        public string AgentId { get; set; }
    }
}
