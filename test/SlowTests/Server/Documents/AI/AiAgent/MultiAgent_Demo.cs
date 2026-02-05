using System;
using System.Collections.Generic;
using System.Diagnostics.PerformanceData;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Hl7.Fhir.Utility;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class MultiAgent_Demo(ITestOutputHelper output) : RavenTestBase(output)
    {

        static bool WaitForUser = false;

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ComputerStore_Simplified_SingleOrder_CustomerIdValidation(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            await SeedProducts(store);

            // products-search-agent
            var productsSearchAgent = new AiAgentConfiguration(
                "products-search-agent",
                config.ConnectionStringName,
                "Find top 3 laptops within a budget and minimum RAM. and return all matching results. if you have 3 return 3, not less!"
            )
            {
                Queries = new List<AiAgentToolQuery>
                {
                    new AiAgentToolQuery
                    {
                        Name = "FindTopLaptops",
                        Description = "Return top 3 laptops that match budget and min RAM. Return all of the laptops you found that will fit to the request.",
                        Query =
                            "from Products as p " +
                            "where p.Category = 'Laptop' " +
                            "and p.PriceNis <= $maxBudgetNis " +
                            "and p.RamGb >= $minRamGb " +
                            "order by p.PriceNis asc " +
                            "limit 0, 3",
                        ParametersSampleObject = "{}"
                    }
                }
            };
            productsSearchAgent.Parameters.Add(new AiAgentParameter("maxBudgetNis", "Max budget (NIS)", AiAgentParameter.AiAgentParameterPolicy.AllowedModelGeneration));
            productsSearchAgent.Parameters.Add(new AiAgentParameter("minRamGb", "Min RAM (GB)", AiAgentParameter.AiAgentParameterPolicy.AllowedModelGeneration));
            var productsSearchId = (await store.AI.CreateAgentAsync(productsSearchAgent, AgentAnswer.Instance)).Identifier;

            // orders-agent
            var ordersAgent = new AiAgentConfiguration(
                "orders-agent",
                config.ConnectionStringName,
                "Create ONE order that contains ALL returned product IDs. " +
                "You MUST always include customerId in tool calls. "
            )
            {
                Actions = new List<AiAgentToolAction>
                {
                    new AiAgentToolAction("CreateOrder", "Create a single order with many product ids.")
                    {
                        ParametersSampleObject = JsonConvert.SerializeObject(CreateOrderRequest.Instance)
                    }
                }
            };
            ordersAgent.Parameters.Add(new AiAgentParameter("customerId", "REQUIRED. The id of the customer making the order.", AiAgentParameter.AiAgentParameterPolicy.Default));
            var ordersAgentId = (await store.AI.CreateAgentAsync(ordersAgent, AgentAnswer.Instance)).Identifier;

            // payment-agent
            var paymentAgent = new AiAgentConfiguration(
                "payment-agent",
                config.ConnectionStringName,
                "Charge payment for an order. You MUST always include customerId in tool calls."
            )
            {
                Actions = new List<AiAgentToolAction>
                {
                    new AiAgentToolAction("ChargeOrder", "Mark order as paid (no-op charge).")
                    {
                        ParametersSampleObject = JsonConvert.SerializeObject(ChargeOrderRequest.Instance)
                    }
                }
            };
            paymentAgent.Parameters.Add(new AiAgentParameter("orderId", "REQUIRED. The id of the order.", AiAgentParameter.AiAgentParameterPolicy.AllowedModelGeneration));
            paymentAgent.Parameters.Add(new AiAgentParameter("customerId", "REQUIRED. The id of the customer making the order.", AiAgentParameter.AiAgentParameterPolicy.Default));
            paymentAgent.Parameters.Add(new AiAgentParameter("creditCardNumber", "REQUIRED. the credit card number for paying.", AiAgentParameter.AiAgentParameterPolicy.Default));
            var paymentAgentId = (await store.AI.CreateAgentAsync(paymentAgent, AgentAnswer.Instance)).Identifier;

            // root agent
            var root = new AiAgentConfiguration(
                "store-clerk-agent",
                config.ConnectionStringName,
                "You are the store clerk (root) for a computer store.\n\n" +
                "Rules:\n" +
                "1) Always act for the current customer identified by the REQUIRED parameter 'customerId'.\n" +
                "2) Never invent product ids or order ids. Always obtain ids from your sub-agents.\n" +
                "3) The store uses ONE order that contains multiple product ids (NOT one order per product).\n" +
                "Always ensure the sub-agent receives 'customerId' so it can validate ownership.\n" +
                "4) For payments, delegate to the payment sub-agent.\n" +
                "5) PAYMENT GATING (STRICT):\n" +
                "   - Do NOT ask, suggest, or initiate payment.\n" +
                "   - Only delegate to the payment sub-agent if the user explicitly requests payment (e.g. 'pay', 'charge', 'checkout', 'place the order').\n" +
                "   - If the user did NOT explicitly request payment, you MUST NOT mention payment at all.\n" +
                "6) The parameter 'creditCardNumber' is hidden from you. Do not ask for it and do not mention it to the user.\n\n" +
                "7) ORDER GATING (STRICT): Do NOT create an order until the user explicitly requests creating an order.\n\n" +
                "Flow:\n" +
                "A) Ask the products sub-agent to find matching products.\n" +
                "B) If the user explicitly requests an order: ask the orders sub-agent to create ONE order containing the desired product ids.\n" +
                "C) If (and only if) the user explicitly requests payment: ask the payment sub-agent to charge the order.\n\n" +
                "Output:\n" +
                "Return your answer to the user and a short confirmation of what happened."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent { Identifier = productsSearchId, Description = "Find products - Use this tool to get products and their ids." },
                    new AiAgentToolSubAgent { Identifier = ordersAgentId, Description = "Create a single order - Use this tool to create ONE order containing ALL ids. " },
                    new AiAgentToolSubAgent { Identifier = paymentAgentId, Description = "Charge payment - use this tool to make an order payment (must include customerId AND orderId)" }
                ]
            };
            root.Parameters.Add(new AiAgentParameter("customerId", "REQUIRED. The id of the customer."));
            root.Parameters.Add(new AiAgentParameter("creditCardNumber", "REQUIRED. the credit card number for paying.", sendToModel: false)); // we wont expose this to the roo-agent model, we'll only pass it to the payment-agent
            var rootId = (await store.AI.CreateAgentAsync(root, AgentAnswer.Instance)).Identifier;

            var chat = store.AI.Conversation(
                rootId,
                "Chats/1",
                new AiConversationCreationOptions().AddParameter("customerId", "Customers/1").AddParameter("creditCardNumber", "1234-1234-1234-1234")
            );

            // ------------------- Handles -------------------
            int orderNumber = 0;
            int messageNumber = 0;

            chat.Handle<CreateOrderRequest>("orders-agent/CreateOrder", async req =>
            {
                if (string.IsNullOrWhiteSpace(req.CustomerId))
                    return new ActionToolResult { IsSuccessful = false, Answer = "customerId is required" };

                if (req.ProductIds == null || req.ProductIds.Count == 0)
                    return new ActionToolResult { IsSuccessful = false, Answer = "ProductIds is required" };

                string orederId = null;

                using (var session = store.OpenAsyncSession())
                {
                    // Validate products exist (minimal)
                    foreach (var pid in req.ProductIds.Distinct(StringComparer.OrdinalIgnoreCase))
                    {
                        var p = await session.LoadAsync<Product>(pid);
                        if (p == null)
                            return new ActionToolResult { IsSuccessful = false, Answer = $"Product '{pid}' not found" };
                    }

                    var order = new Order
                    {
                        Id = orederId = $"Orders/{Interlocked.Increment(ref orderNumber)}",
                        CustomerId = req.CustomerId,
                        ProductIds = req.ProductIds.Distinct(StringComparer.OrdinalIgnoreCase).ToList(),
                        Status = "Created"
                    };

                    await session.StoreAsync(order);
                    await session.SaveChangesAsync();
                }

                return new ActionToolResult { IsSuccessful = true, Answer = $"Order has been created, with id '{orederId}'" };
            });
            
            chat.Handle<ChargeOrderRequest>("payment-agent/ChargeOrder", async req =>
            {
                if (string.IsNullOrWhiteSpace(req.CustomerId))
                    return new ActionToolResult { IsSuccessful = false, Answer = "customerId is required" };

                if (string.IsNullOrWhiteSpace(req.CreditCardNumber))
                    return new ActionToolResult { IsSuccessful = false, Answer = "CreditCardNumber is required" };

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(req.OrderId);
                    if (order == null)
                        return new ActionToolResult { IsSuccessful = false, Answer = $"Order '{req.OrderId}' not found" };

                    if (order.CustomerId != req.CustomerId)
                        return new ActionToolResult { IsSuccessful = false, Answer = $"Order '{req.OrderId}' not found" };

                    order.Status = "Paid";
                    await session.SaveChangesAsync();
                }

                return new ActionToolResult { IsSuccessful = true, Answer = "PAID" };
            });

            // // ------------------- Run: search, create, pay -------------------
            // await Talk("Find laptops within a budget of 3,500 NIS that have at least 16 GB of RAM.");
            //
            // await Talk("Create ONE order that contains ALL top matching laptops for my customer.");
            //
            // await Talk("Now I want to pay, charge the order please.");

            // ------------------- Run: search, create, create, pay -------------------
            await Talk("Find laptops within a budget of 3,500 NIS that have at least 16 GB of RAM.");

            await Talk("Create ONE order that contains ALL top matching laptops for my customer.");

            await Talk("Create Another order that contains only one of the matching laptops for my customer.");

            await Talk("Now I want to pay for the second order, charge the order please.");


            WaitForUserToContinueTheTest(store, false);
           
            async Task Talk(string prompt)
            {
                chat.SetUserPrompt(prompt);
                var r1 = await chat.RunAsync<AgentAnswer>();
                Assert.Equal(AiConversationResult.Done, r1.Status);

                // for demo
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"User[{++messageNumber}]: " + prompt);
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine($"Agent[{messageNumber}]: " + r1.Answer.Answer);
                Console.ForegroundColor = ConsoleColor.DarkCyan;

                Console.Write("Orders: ");
                using (var session = store.OpenAsyncSession())
                {
                    var orders = await session.Advanced.AsyncRawQuery<Order>("from Orders").ToListAsync();
                    Console.WriteLine(orders.IsNullOrEmpty() ? "None" : Environment.NewLine + System.Text.Json.JsonSerializer.Serialize(orders, new JsonSerializerOptions
                    {
                        WriteIndented = true
                    }));
                }
                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine();

                if (WaitForUser)
                {
                    WaitForUserToContinueTheTest(store, false);
                    Console.WriteLine();
                }
            }
        }

        private static async Task SeedProducts(IDocumentStore store)
        {
            using var session = store.OpenAsyncSession();

            // Matches (<=3500 & >=16GB): Products/5 (2790), Products/4 (2990), Products/1 (3290)
            await session.StoreAsync(new Product { Id = "Products/1", Category = "Laptop", RamGb = 16, PriceNis = 3290 });
            await session.StoreAsync(new Product { Id = "Products/4", Category = "Laptop", RamGb = 16, PriceNis = 2990 });
            await session.StoreAsync(new Product { Id = "Products/5", Category = "Laptop", RamGb = 16, PriceNis = 2790 });

            // Additional valid (<=3500 & >=16GB) for add flow
            await session.StoreAsync(new Product { Id = "Products/6", Category = "Laptop", RamGb = 24, PriceNis = 3490 });

            // Non-matching examples
            await session.StoreAsync(new Product { Id = "Products/2", Category = "Laptop", RamGb = 32, PriceNis = 4890 }); // above budget
            await session.StoreAsync(new Product { Id = "Products/3", Category = "Laptop", RamGb = 8, PriceNis = 1990 }); // below RAM

            await session.SaveChangesAsync();
        }

        // Entities
        private class Product
        {
            public string Id { get; set; }
            public string Category { get; set; } // "Laptop"
            public int RamGb { get; set; }
            public decimal PriceNis { get; set; }
        }

        private class Order
        {
            public string Id { get; set; } // "Orders/"
            public string CustomerId { get; set; }
            public List<string> ProductIds { get; set; } = new();
            public string Status { get; set; } // Created / Paid
        }


        // Minimal tool payloads
        private class CreateOrderRequest
        {
            public static CreateOrderRequest Instance = new()
            {
                CustomerId = "Customers/1",
                ProductIds = new List<string> { "Products/5", "Products/4", "Products/1" }
            };

            public string CustomerId { get; set; }
            public List<string> ProductIds { get; set; }
        }

        private class ChargeOrderRequest
        {
            public static ChargeOrderRequest Instance = new()
            {
                CustomerId = "Customers/1",
                OrderId = "Orders/1",
                CreditCardNumber = "1111-2222-3333-4444"
            };

            public string CustomerId { get; set; }
            public string OrderId { get; set; }
            public string CreditCardNumber { get; set; }
        }


        // Tool Result
        private class ActionToolResult
        {
            public bool IsSuccessful { get; set; }
            public string Answer { get; set; }
        }

        // root result
        private class AgentAnswer
        {
            public static AgentAnswer Instance = new AgentAnswer() { Answer = "The model answer" };

            public string Answer { get; set; }
        }
    }

}
