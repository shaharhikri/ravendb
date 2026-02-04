using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
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

            // orders-agent (CreateOrder + ChangeOrder) with REQUIRED customerId parameter
            var ordersAgent = new AiAgentConfiguration(
                "orders-agent",
                config.ConnectionStringName,
                "Create ONE order that contains ALL returned product IDs. " +
                "You MUST always include customerId in tool calls. " +
                "When changing an order, you MUST provide the same customerId and orderId."
            )
            {
                Actions = new List<AiAgentToolAction>
                {
                    new AiAgentToolAction("CreateOrder", "Create a single order with many product ids.")
                    {
                        ParametersSampleObject = JsonConvert.SerializeObject(CreateOrderRequest.Instance)
                    },
                    new AiAgentToolAction("ChangeOrder", "Add or remove a single product id from an existing order.")
                    {
                        ParametersSampleObject = JsonConvert.SerializeObject(ChangeOrderRequest.Instance)
                    }
                }
            };
            ordersAgent.Parameters.Add(new AiAgentParameter("customerId", "REQUIRED. The id of the customer making the order.", AiAgentParameter.AiAgentParameterPolicy.Default));

            // payment-agent (no-op, mark Paid) also receives customerId for symmetry
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
            paymentAgent.Parameters.Add(new AiAgentParameter("customerId", "REQUIRED. The id of the customer making the order.", AiAgentParameter.AiAgentParameterPolicy.Default));
            paymentAgent.Parameters.Add(new AiAgentParameter("creditCardNumber", "REQUIRED. the credit card number for paying.", AiAgentParameter.AiAgentParameterPolicy.Default));

            var productsSearchId = (await store.AI.CreateAgentAsync(productsSearchAgent, new { Answer = "" })).Identifier;
            var ordersAgentId = (await store.AI.CreateAgentAsync(ordersAgent, new { Answer = "" })).Identifier;
            var paymentAgentId = (await store.AI.CreateAgentAsync(paymentAgent, new { Answer = "" })).Identifier;

            // root agent
            var root = new AiAgentConfiguration(
                "store-clerk-agent",
                config.ConnectionStringName,
                "You are the store clerk (root). " +
                "1) Use products-search-agent to get product ids. " +
                "2) Use orders-agent/CreateOrder to create ONE order containing ALL ids. " +
                "3) If user asks, use orders-agent/ChangeOrder (must include customerId and validate ownership). " +
                "4) Then use payment-agent/ChargeOrder (must include customerId)."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent { Identifier = productsSearchId, Description = "Find products" },
                    new AiAgentToolSubAgent { Identifier = ordersAgentId, Description = "Create/change a single order" },
                    new AiAgentToolSubAgent { Identifier = paymentAgentId, Description = "Charge payment" }
                ]
            };
            root.Parameters.Add(new AiAgentParameter("customerId", "REQUIRED. The id of the customer."));
            root.Parameters.Add(new AiAgentParameter("creditCardNumber", "REQUIRED. the credit card number for paying.", sendToModel: false)); // we wont expose this to the model, we'll only pass it to the payment-agent


            var rootId = (await store.AI.CreateAgentAsync(root, new { Answer = "" })).Identifier;

            var chat = store.AI.Conversation(
                rootId,
                "Chats/1",
                new AiConversationCreationOptions().AddParameter("customerId", "Customers/1").AddParameter("creditCardNumber", "1234-1234-1234-1234")
            );

            // ------------------- Handles -------------------
            int orderNumber = 0;

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

                return new ActionToolResult { IsSuccessful = true, Answer = orederId };
            });

            chat.Handle<ChangeOrderRequest>("orders-agent/ChangeOrder", async req =>
            {
                if (string.IsNullOrWhiteSpace(req.CustomerId))
                    return new ActionToolResult { IsSuccessful = false, Answer = "customerId is required" };

                if (string.IsNullOrWhiteSpace(req.OrderId))
                    return new ActionToolResult { IsSuccessful = false, Answer = "orderId is required" };

                using (var session = store.OpenAsyncSession())
                {

                    var order = await session.LoadAsync<Order>(req.OrderId);
                    if (order == null)
                        return new ActionToolResult { IsSuccessful = false, Answer = $"Order '{req.OrderId}' not found" };

                    // Ownership validation requested
                    if (string.Equals(order.CustomerId, req.CustomerId, StringComparison.OrdinalIgnoreCase) == false)
                        return new ActionToolResult { IsSuccessful = false, Answer = "Order does not belong to this customerId" };

                    if (string.Equals(order.Status, "Paid", StringComparison.OrdinalIgnoreCase))
                        return new ActionToolResult { IsSuccessful = false, Answer = "Cannot change a paid order" };

                    var add = req.AddProductId;
                    var remove = req.RemoveProductId;

                    // Exactly one of them
                    if (string.IsNullOrWhiteSpace(add) == string.IsNullOrWhiteSpace(remove))
                        return new ActionToolResult { IsSuccessful = false, Answer = "Provide exactly one of AddProductId or RemoveProductId" };

                    if (string.IsNullOrWhiteSpace(add) == false)
                    {
                        var p = await session.LoadAsync<Product>(add);
                        if (p == null)
                            return new ActionToolResult { IsSuccessful = false, Answer = $"Product '{add}' not found" };

                        if (order.ProductIds.Contains(add, StringComparer.OrdinalIgnoreCase) == false)
                            order.ProductIds.Add(add);
                    }

                    if (string.IsNullOrWhiteSpace(remove) == false)
                        order.ProductIds.RemoveAll(x => string.Equals(x, remove, StringComparison.OrdinalIgnoreCase));
                    
                    await session.SaveChangesAsync();
                }

                return new ActionToolResult { IsSuccessful = true, Answer = "OK" };
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

            // ------------------- Run 1: create + pay -------------------

            chat.SetUserPrompt(
                "Budget 3500 NIS, min 16GB RAM. " +
                "Create ONE order that contains ALL top matching laptops for my customer, then charge it."
            );

            var r1 = await chat.RunAsync<object>();
            Assert.Equal(AiConversationResult.Done, r1.Status);

            WaitForUserToContinueTheTest(store, false);


            string orderId;
            using (var session = store.OpenAsyncSession())
            {
                var orders = await session.Advanced.AsyncRawQuery<Order>("from Orders")
                    .ToListAsync();

                Assert.Equal(1, orders.Count);
                var o = orders.Single();
                orderId = o.Id;

                Assert.Equal("Customers/1", o.CustomerId);
                Assert.Equal("Paid", o.Status);
                Assert.Equal(3, o.ProductIds.Count); // top 3
            }

            // ------------------- Run 2: attempt change on paid (should not change) -------------------

            chat.SetUserPrompt($"Add product 'Products/6' to order '{orderId}'");
            var r2 = await chat.RunAsync<object>();
            Assert.Equal(AiConversationResult.Done, r2.Status);


            using (var session = store.OpenAsyncSession())
            {
                var o = await session.LoadAsync<Order>(orderId);
                Assert.Equal("Paid", o.Status);
                Assert.Equal(3, o.ProductIds.Count); // unchanged
            }

            // // ------------------- Run 3: new chat create, change BEFORE pay, then pay -------------------
            //
            // var chat2 = store.AI.Conversation(
            //     rootId,
            //     "store-chats/2",
            //     new AiConversationCreationOptions().AddParameter("customerId", "Customers/1")
            // );
            //
            // // re-register minimal handlers (copy)
            // chat2.Handle<CreateOrderRequest>("orders-agent/CreateOrder", chat.Handlers["orders-agent/CreateOrder"]);
            // chat2.Handle<ChangeOrderRequest>("orders-agent/ChangeOrder", chat.Handlers["orders-agent/ChangeOrder"]);
            // chat2.Handle<ChargeOrderRequest>("payment-agent/ChargeOrder", chat.Handlers["payment-agent/ChargeOrder"]);
            //
            // chat2.SetUserPrompt(
            //     "Budget 3500 NIS, min 16GB RAM. Create ONE order with the top 3 products for my customer. Do NOT charge yet."
            // );
            //
            // var r3 = await chat2.RunAsync<object>();
            // Assert.Equal(AiConversationResult.Done, r3.Status);
            //
            // string createdOrderId;
            // using (var session = store.OpenAsyncSession())
            // {
            //     var orders = await session.Advanced.AsyncRawQuery<Order>("from Orders")
            //         .ToListAsync();
            //
            //     // now 2 orders total
            //     Assert.Equal(2, orders.Count);
            //
            //     // pick the Created one
            //     var created = orders.Single(x => x.Status == "Created");
            //     createdOrderId = created.Id;
            //     Assert.Equal(3, created.ProductIds.Count);
            // }
            //
            // chat2.SetUserPrompt(
            //     $"Add product 'Products/6' to order '{createdOrderId}', then charge that order."
            // );
            //
            // var r4 = await chat2.RunAsync<object>();
            // Assert.Equal(AiConversationResult.Done, r4.Status);
            //
            // using (var session = store.OpenAsyncSession())
            // {
            //     var o = await session.LoadAsync<Order>(createdOrderId);
            //     Assert.Equal("Paid", o.Status);
            //     Assert.Equal(4, o.ProductIds.Count);
            //     Assert.Contains("Products/6", o.ProductIds);
            // }
            //
            // // ------------------- Run 4: ownership validation -------------------
            // // Attempt to change with WRONG customerId should fail (handler blocks)
            //
            // var chat3 = store.AI.Conversation(
            //     rootId,
            //     "store-chats/3",
            //     new AiConversationCreationOptions().AddParameter("customerId", "Customers/999")
            // );
            //
            // chat3.Handle<CreateOrderRequest>("orders-agent/CreateOrder", chat.Handlers["orders-agent/CreateOrder"]);
            // chat3.Handle<ChangeOrderRequest>("orders-agent/ChangeOrder", chat.Handlers["orders-agent/ChangeOrder"]);
            // chat3.Handle<ChargeOrderRequest>("payment-agent/ChargeOrder", chat.Handlers["payment-agent/ChargeOrder"]);
            //
            // chat3.SetUserPrompt($"Add product 'Products/6' to order '{createdOrderId}'");
            // var r5 = await chat3.RunAsync<object>();
            // Assert.Equal(AiConversationResult.Done, r5.Status);
            //
            // using (var session = store.OpenAsyncSession())
            // {
            //     var o = await session.LoadAsync<Order>(createdOrderId);
            //     Assert.Equal(4, o.ProductIds.Count); // unchanged by wrong customer
            // }
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

        private class ChangeOrderRequest
        {
            public static ChangeOrderRequest Instance = new()
            {
                CustomerId = "Customers/1",
                OrderId = "Orders/1",
                AddProductId = "Products/6"
            };

            public string CustomerId { get; set; }
            public string OrderId { get; set; }

            // ultra-simple: either add OR remove a single product
            public string AddProductId { get; set; }
            public string RemoveProductId { get; set; }
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
    }

}
