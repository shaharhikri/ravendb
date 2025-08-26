using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Lextm.SharpSnmpLib.Pipeline;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;

namespace Raven.Server.Documents.AI;

internal class ChatCompletionVertexClient : ChatCompletionClient
{
    internal ChatCompletionVertexClient(IMemoryContextPool contextPool, string baseUri, string apiKey, string model,
        string organizationId, string projectId, bool? think = null, DocumentConventions conventions = null) : base(contextPool, baseUri, apiKey, model, organizationId, projectId, think, conventions)
    {
    }

    protected override void AddDefaultHeaders(HttpRequestMessage request)
    {
    }

    // todo: remove
    public override HttpRequestMessage CreateCompletionRequest(JsonOperationContext ctx,
        List<BlittableJsonReaderObject> messages,
        List<BlittableJsonReaderObject> tools,
        bool useTools,
        bool streaming,
        string schema)
    {
        var content = new BlittableJsonContent(async stream =>
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
            {
                var blittable = CreateVertexRequest(ctx, messages, tools, useTools, schema);
                writer.WriteObject(blittable);
            }
        }, ConventionsToUse);

        content.Headers.Add(Constants.RequestFields.HeaderContentType, Constants.RequestFields.MediaTypeApplicationJson);

        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = content
        };

        return request;
    }


    [DoesNotReturn]
    protected override void HandleUnsuccessfulResponse(HttpResponseMessage response, BlittableJsonReaderObject responseContent)
    {
        var headers = response.Headers;
        var reqId = GetRequestId(headers);

        // todo

        throw new UnsuccessfulRequestException(responseContent.ToString(), response.StatusCode)
        {
            RequestId = reqId
        };
    }


    private BlittableJsonReaderObject CreateVertexRequest(JsonOperationContext context, List<BlittableJsonReaderObject> messages, List<BlittableJsonReaderObject> tools, bool useTools, string schema)
    {
        var request = new DynamicJsonValue();

        if (messages.Count > 0)
        {
            // system prompt
            request["system_instruction"] = ConvertMessage(messages.First());

            var vertexMessages = new List<DynamicJsonValue>();

            bool first = true;
            foreach (var message in messages)
            {
                if (first)
                {
                    first = false;
                    continue;
                }

                vertexMessages.Add(ConvertMessage(message));
            }

            request["contents"] = vertexMessages;
        }

        if (tools?.Count > 0)
        {
            request["tools"] = new DynamicJsonValue[]
            {
                new DynamicJsonValue
                {
                    ["functionDeclarations"] = tools.Select(t =>
                    {
                        if (t.TryGet("function", out BlittableJsonReaderObject function) == false)
                            throw new FormatException($"Cannot convert tool '{t}' to a vertex tool");

                        if (function.TryGet("parameters", out BlittableJsonReaderObject parameters))
                            RemoveAdditionalPropertiesFromParametersObject(parameters);

                        return function;
                    }).ToArray()
                }
            };

            if (useTools == false)
            {
                request["toolConfig"] = new DynamicJsonValue
                {
                    ["functionCallingConfig"] = new DynamicJsonValue
                    {
                        ["mode"] = "NONE"
                    }
                };
            }
        }

        var openAiSchema = GetStructuredOutputSchemaAsBlittable(context, schema);
        if (openAiSchema.TryGet("schema", out BlittableJsonReaderObject innerSchema) == false)
            throw new FormatException($"Cannot convert open-ai schema '{openAiSchema}' to a vertex schema");

        RemoveAdditionalPropertiesFromParametersObject(innerSchema);

        request["generationConfig"] = new DynamicJsonValue
        {
            ["responseMimeType"] = Constants.RequestFields.MediaTypeApplicationJson,
            ["responseSchema"] = innerSchema
        };

        return context.ReadObject(request, "vertexRequest");

        DynamicJsonValue ConvertMessage(BlittableJsonReaderObject msg)
        {
            if (msg.TryGet(Constants.RequestFields.Content, out string msgContent) == false)
                throw new FormatException($"Cannot convert message '{msg}' to a vertex message - there is no '{Constants.RequestFields.Content}' field in it");

            if (msg.TryGet(Constants.RequestFields.Role, out string msgRole) == false)
                throw new FormatException($"Cannot convert message '{msg}' to a vertex message - there is no '{Constants.RequestFields.Role}' field in it");

            msgRole = msgRole == Constants.RequestFields.RoleAssistantValue ? "model" : msgRole;

            return new DynamicJsonValue
            {
                ["role"] = msgRole,
                ["parts"] = new DynamicJsonValue[]
                {
                    new DynamicJsonValue { ["text"] = msgContent }
                }
            };
        }

        void RemoveAdditionalPropertiesFromParametersObject(BlittableJsonReaderObject parametersObj)
        {
            if (parametersObj.TryGet("additionalProperties", out bool _))
            {
                parametersObj.Modifications = new DynamicJsonValue(parametersObj);
                parametersObj.Modifications.Remove("additionalProperties");
            }
        }
    }

    // todo: remove

    public override async Task<AiResponse> CompleteAsync(JsonOperationContext context, HttpRequestMessage request, AiUsage usage, CancellationToken token)
    {
        AddDefaultHeaders(request);
        using var response = await SendRequestAsync(request, token);
        var responseContent = await GetResponseContentAsync(context, response, token);
    
        var responseParser = new AiResponseVertexParser(this, response, responseContent);
        responseParser.EnsureSuccessfulResponse();
        responseParser.ParseMessage(usage);
        if (responseParser.TryParseToolCalls(out var tools))
        {
            return new AiResponse(AiResponseType.Tool) { ToolCalls = tools, Message = responseParser.GetMessage(context) };
        }
    
        var result = responseParser.GetContent(context);
        return new AiResponse(AiResponseType.Result) { Result = result, Message = responseParser.GetMessage(context) };
    }

    private struct AiResponseVertexParser
    {
        public readonly ChatCompletionVertexClient Client { get; }
        public readonly HttpResponseMessage Response { get; }
        public readonly BlittableJsonReaderObject ResponseContent { get; }

        private BlittableJsonReaderObject _vertexMessage; // Message
        /*
         {
               "parts": [{
                       "text": "{\n  \"Answer\": \"To recommend pairings for your cheese from recent orders...\",\n  \"MatchingProductsId\": [],\n  \"Relevant\": false,\n  \"RelevantOrdersId\": []\n}"
                   }
               ],
               "role": "model"
           }
         */

        private string _text; //_content;

        private BlittableJsonReaderObject _choice0;

        private BlittableJsonReaderArray _parts;

        public AiResponseVertexParser(ChatCompletionVertexClient client, HttpResponseMessage response, BlittableJsonReaderObject responseContent)
        {
            Client = client;
            Response = response;
            ResponseContent = responseContent;
        }

        public void EnsureSuccessfulResponse()
        {
            if (Response.IsSuccessStatusCode)
                return;

            Client.HandleUnsuccessfulResponse(Response, ResponseContent);
            Debug.Assert(false, "we should never get here");
        }

        public void ParseMessage(AiUsage usage)
        {
            if (ResponseContent.TryGet("candidates", out BlittableJsonReaderArray candidates) == false)
                throw new UnexpectedResponseException("Missing 'candidates' array in Vertex response: " + ResponseContent) { RequestId = GetRequestId(Response.Headers) };


            if (candidates.Length == 0)
                throw new UnexpectedResponseException("Empty 'candidates' array in Vertex response: " + ResponseContent) { RequestId = GetRequestId(Response.Headers) };

            var candidate0 = candidates.First() as BlittableJsonReaderObject;

            if (candidate0.TryGet("content", out _vertexMessage) == false)
                throw new UnexpectedResponseException("Missing 'content' object in first candidate: " + ResponseContent) { RequestId = GetRequestId(Response.Headers) };

            if (_vertexMessage.TryGet("parts", out _parts) == false)
                throw new UnexpectedResponseException("Missing 'parts' array inside candidate 'content': " + ResponseContent) { RequestId = GetRequestId(Response.Headers) };

            var part0 = _parts.First() as BlittableJsonReaderObject;

            part0.TryGet("text", out _text);

            if (ResponseContent.TryGet("usageMetadata", out BlittableJsonReaderObject usageVertexJson) == false)
                throw new UnexpectedResponseException("Missing 'usageMetadata' object in Vertex response: " + ResponseContent) { RequestId = GetRequestId(Response.Headers) };

            if (usageVertexJson.TryGet("promptTokenCount", out int promptTokenCount) == false ||
                usageVertexJson.TryGet("candidatesTokenCount", out int candidatesTokenCount) == false ||
                usageVertexJson.TryGet("totalTokenCount", out int totalTokenCount) == false)
            {
                throw new UnexpectedResponseException("Missing required token count fields ('promptTokenCount', 'candidatesTokenCount', 'totalTokenCount') in 'usageMetadata': " + ResponseContent) { RequestId = GetRequestId(Response.Headers) };
            }

            var currentUsage = new AiUsage()
            {
                TotalTokens = totalTokenCount,
                PromptTokens = promptTokenCount,
                CompletionTokens = candidatesTokenCount
            };

            usage.UpdateFrom(currentUsage);
        }

        public bool TryParseToolCalls(out List<AiToolCall> toolCalls)
        {
            toolCalls = [];
            foreach (BlittableJsonReaderObject part in _parts)
            {
                if (part.TryGet("functionCall", out BlittableJsonReaderObject functionCall) == false)
                    continue;

                if (part.TryGet("name", out string name) == false ||
                    part.TryGet("name", out string args) == false)
                    throw new UnexpectedResponseException("Invalid vertex function call: " + part)
                    {
                        RequestId = GetRequestId(Response.Headers)
                    };

                toolCalls.Add(new AiToolCall(Guid.NewGuid().ToString(), name, args));
            }

            if (toolCalls.Count == 0)
            {
                toolCalls = null;
                return false;
            }

            return true;
        }

        public BlittableJsonReaderObject GetMessage(JsonOperationContext context)
        {
            if (_vertexMessage == null)
                throw new NullReferenceException(nameof(_vertexMessage) + " is null. Vertex message probably wasn't parsed yet");

            // convert Vertex message to open-ai compatible message

            return context.ReadObject(new DynamicJsonValue
            {
                ["role"] = Constants.RequestFields.RoleAssistantValue,
                ["content"] = _text
            }, "vertexRequest");
        }

        public BlittableJsonReaderObject GetContent(JsonOperationContext context)
        {
            if (string.IsNullOrEmpty(_text))
            {
                _choice0.TryGet(Constants.ResponseFields.FinishReason, out string finishReason);
                _choice0.TryGet(Constants.ResponseFields.Refusal, out string refusal);
                throw new RefusedToAnswerException("The request was refused by the model")
                {
                    Refusal = refusal,
                    FinishReason = finishReason,
                    RequestId = GetRequestId(Response.Headers)
                };
            }

            return context.Sync.ReadForMemory(_text, "ai/output");
        }
    }
}
