using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public class AiRagConfiguration : IDynamicJson
{
    public string ConnectionStringName { get; set; }
    public string SystemPrompt { get; set; }
    public string OutputSchema { get; set; }
    public List<ToolQuery> Queries { get; set; }= [];
    public List<ToolAction> Actions { get; set; } = [];
    public PersistenceConfiguration Persistence { get; set; }
    public Dictionary<string, string> Parameters { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ConnectionStringName)] = ConnectionStringName,
            [nameof(SystemPrompt)] = SystemPrompt,
            [nameof(OutputSchema)] = OutputSchema,
            [nameof(Queries)] = new DynamicJsonArray(Queries.Select(x => x.ToJson())),
            [nameof(Actions)] = new DynamicJsonArray(Actions.Select(x => x.ToJson())),
            [nameof(Persistence)] = Persistence,
            [nameof(Parameters)] = InnerToJson(Parameters)
        };
    }

    DynamicJsonValue InnerToJson(IDictionary<string, string> dict)
    {
        if (dict == null)
            return null;

        var json = new DynamicJsonValue();

        foreach (var kvp in dict)
        {
            json[kvp.Key] = kvp.Value;
        }

        return json;
    }

    public DynamicJsonValue ToAuditJson()
    {
        return ToJson();
    }

    public ToolQuery FindQuery(string name)
    {
        foreach (ToolQuery query in Queries ?? [])
        {
            if (query.Name == name)
                return query;
        }

        return null;
    }

    public ToolAction FindAction(string name)
    {
        foreach (ToolAction action in Actions ?? [])
        {
            if (action.Name == name)
                return action;
        }

        return null;
    }

    public bool Equals(AiRagConfiguration other)
    {
        if (ReferenceEquals(this, other))
            return true;

        return string.Equals(ConnectionStringName, other.ConnectionStringName)
               && string.Equals(SystemPrompt, other.SystemPrompt)
               && string.Equals(OutputSchema, other.OutputSchema)
               && Equals(Persistence, other.Persistence)
               && DictionaryEquals(Parameters, other.Parameters)
               && ListEquals(Queries, other.Queries)
               && ListEquals(Actions, other.Actions);
    }

    private static bool ListEquals<T>(IList<T> a, IList<T> b)
    {
        if (ReferenceEquals(a, b))
            return true;
                var aNullOrEmpty = a == null || a.Count == 0;
        var bNullOrEmpty = b == null || b.Count == 0;

        if (aNullOrEmpty && bNullOrEmpty)
            return true;
        if (aNullOrEmpty || bNullOrEmpty)
            return false;
        if (a.Count != b.Count)
            return false;
        for (int i = 0; i < a.Count; i++)
        {
            if (Equals(a[i], b[i]) == false)
                return false;
        }
        return true;
    }

    private static bool DictionaryEquals(Dictionary<string, string> a, Dictionary<string, string> b)
    {
        if (ReferenceEquals(a, b))
            return true;

        var aNullOrEmpty = a == null || a.Count == 0;
        var bNullOrEmpty = b == null || b.Count == 0;

        if (aNullOrEmpty && bNullOrEmpty)
            return true;
        if (aNullOrEmpty || bNullOrEmpty)
            return false;
        if (a.Count != b.Count)
            return false;
        foreach (var kvp in a)
        {
            if (b.TryGetValue(kvp.Key, out var value) == false || kvp.Value != value)
                return false;
        }
        return true;
    }

    public class PersistenceConfiguration : IDynamicJson
    {
        public string Collection { get; set; }
        public TimeSpan? Expires { get; set; }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj))
                return true;

            if (obj is not PersistenceConfiguration other)
                return false;
            return Equals(other);
        }

        public bool Equals(PersistenceConfiguration other)
        {
            return Collection == other.Collection && Nullable.Equals(Expires, other.Expires);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Collection != null ? Collection.GetHashCode() : 0;
                hashCode = (hashCode * 397) ^ (Expires != null ? Expires.GetHashCode() : 0);
                return hashCode;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Collection)] = Collection,
                [nameof(Expires)] = Expires
            };
        }
    }
    
    public class ToolAction : IDynamicJson
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string ParametersSchema { get; set; }

        public override bool Equals(object o)
        {
            if (ReferenceEquals(this, o))
                return true;

            if (o is not ToolAction other)
                return false;

            return Equals(other);
        }

        public bool Equals(ToolAction other)
        {
            return Name == other.Name && Description == other.Description && ParametersSchema == other.ParametersSchema;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Name != null ? Name.GetHashCode() : 0;
                hashCode = (hashCode * 397) ^ (Description != null ? Description.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (ParametersSchema != null ? ParametersSchema.GetHashCode() : 0);
                return hashCode;
            }
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Description)] = Description,
                [nameof(ParametersSchema)] = ParametersSchema
            };
        }
    }
    
    public class ToolQuery : ToolAction
    {
        public string Query { get; set; }

        public override bool Equals(object o)
        {
            if (ReferenceEquals(this, o))
                return true;

            if (o is not ToolQuery other)
                return false;

            return Equals(other);
        }

        public bool Equals(ToolQuery other)
        {
            return base.Equals((ToolAction)other) && Query == other.Query;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (Query != null ? Query.GetHashCode() : 0);
                return hashCode;
            }
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Query)] = Query;
            return json;
        }
    }

}
