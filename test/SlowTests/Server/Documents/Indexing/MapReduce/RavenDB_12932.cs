﻿using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.OutputToCollection;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_12932 : RavenTestBase
    {
        public RavenDB_12932(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCreateIndexesWithPattern()
        {
            using (var store = GetDocumentStore())
            {
                AbstractIndexCreationTask<Order, DifferentApproachesToDefinePatternOfReduceOutputReferences.Result> index;

                index = new DifferentApproachesToDefinePatternOfReduceOutputReferences.One_A("One_A");
                Assert.Equal("reports/daily/{OrderedAt:yyyy-MM-dd}", index.CreateIndexDefinition().PatternOfReduceOutputReferences);
                index.Execute(store);

                index = new DifferentApproachesToDefinePatternOfReduceOutputReferences.One_B("One_B");
                Assert.Equal("{OrderedAt:yyyy-MM-dd}reports/daily/{Profit:C}", index.CreateIndexDefinition().PatternOfReduceOutputReferences);
                index.Execute(store);
                
                index = new DifferentApproachesToDefinePatternOfReduceOutputReferences.Two_A("Two_A");
                Assert.Equal("reports/daily/{OrderedAt}", index.CreateIndexDefinition().PatternOfReduceOutputReferences);
                index.Execute(store);

                index = new DifferentApproachesToDefinePatternOfReduceOutputReferences.Two_B("Two_B");
                Assert.Equal("reports/daily/{OrderedAt}/product/{Product}", index.CreateIndexDefinition().PatternOfReduceOutputReferences);
                index.Execute(store);

                index = new DifferentApproachesToDefinePatternOfReduceOutputReferences.Three_A("Three_A");
                Assert.Equal("reports/daily/{OrderedAt:MM/dd/yyyy}", index.CreateIndexDefinition().PatternOfReduceOutputReferences);
                index.Execute(store);

                index = new DifferentApproachesToDefinePatternOfReduceOutputReferences.Three_B("Three_B");
                Assert.Equal("reports/daily/{OrderedAt:MM/dd/yyyy}/{Product}", index.CreateIndexDefinition().PatternOfReduceOutputReferences);
                index.Execute(store);
            }
        }

        [Fact]
        public void CanDefinePatternForReferenceDocumentsOfReduceOutputs()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order()
                    {
                        OrderedAt = new DateTime(2019, 10, 26),
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine()
                            {
                                Product = "products/1",
                            },
                            new OrderLine()
                            {
                                Product = "products/2",
                            }
                        }
                    }, "orders/1");

                    session.Store(new Order()
                    {
                        OrderedAt = new DateTime(2019, 10, 25),
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine()
                            {
                                Product = "products/2",
                            }
                        }
                    }, "orders/2");

                    session.Store(new Order()
                    {
                        OrderedAt = new DateTime(2019, 10, 24),
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine()
                            {
                                Product = "products/1",
                            }
                        }
                    }, "orders/3");

                    session.SaveChanges();
                }

                new Orders_ProfitByProductAndOrderedAt().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // 2019-10-26
                    var doc = session.Load<ReduceOutputIdsReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(2, doc.ReduceOutputs.Count);

                    Orders_ProfitByProductAndOrderedAt.Result output;

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }

                    // 2019-10-24

                    doc = session.Load<ReduceOutputIdsReference>("reports/daily/2019-10-24", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(1, doc.ReduceOutputs.Count);

                    output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(doc.ReduceOutputs[0]);

                    Assert.NotNull(output);

                    // 2019-10-25

                    doc = session.Load<ReduceOutputIdsReference>("reports/daily/2019-10-25", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(1, doc.ReduceOutputs.Count);

                    output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(doc.ReduceOutputs[0]);

                    Assert.NotNull(output);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1");
                    session.Delete("orders/2");
                    session.Delete("orders/3");

                    session.SaveChanges();

                    WaitForIndexing(store);

                    Assert.Null(session.Load<ReduceOutputIdsReference>("reports/daily/2019-10-24"));
                    Assert.Null(session.Load<ReduceOutputIdsReference>("reports/daily/2019-10-25"));
                    Assert.Null(session.Load<ReduceOutputIdsReference>("reports/daily/2019-10-26"));
                }
            }
        }


        [Fact]
        public void MultipleReduceOutputsIntoSingleReferenceDocument()
        {
            using (var store = GetDocumentStore())
            {
                var numberOfOutputs = 100;

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < numberOfOutputs; i++)
                    {
                        session.Store(new Order
                        {
                            OrderedAt = new DateTime(2019, 10, 26),
                            Lines = new List<OrderLine>()
                            {
                                new OrderLine()
                                {
                                    Product = "products/" + i,
                                }
                            }
                        }, "orders/" + i);
                    }

                    session.SaveChanges();
                }

                new Orders_ProfitByProductAndOrderedAt().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<ReduceOutputIdsReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(numberOfOutputs, doc.ReduceOutputs.Count);

                    Orders_ProfitByProductAndOrderedAt.Result output;

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/15");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<ReduceOutputIdsReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(numberOfOutputs - 1, doc.ReduceOutputs.Count);

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        Orders_ProfitByProductAndOrderedAt.Result output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/37");
                    session.Delete("orders/83");
                    session.Delete("orders/12");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<ReduceOutputIdsReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(numberOfOutputs - 4, doc.ReduceOutputs.Count);

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        Orders_ProfitByProductAndOrderedAt.Result output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }
                }
            }
        }

        [Fact]
        public async void CanUpdateIndexWithPatternOfReduceOutputReferences()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order()
                    {
                        OrderedAt = new DateTime(2019, 10, 26),
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine()
                            {
                                Product = "products/1",
                            },
                            new OrderLine()
                            {
                                Product = "products/2",
                            }
                        }
                    }, "orders/1");

                    session.Store(new Order()
                    {
                        OrderedAt = new DateTime(2019, 10, 25),
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine()
                            {
                                Product = "products/2",
                            }
                        }
                    }, "orders/2");

                    session.Store(new Order()
                    {
                        OrderedAt = new DateTime(2019, 10, 24),
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine()
                            {
                                Product = "products/1",
                            }
                        }
                    }, "orders/3");

                    session.SaveChanges();
                }

                new Orders_ProfitByProductAndOrderedAt().Execute(store);

                WaitForIndexing(store);
                
                WaitForUserToContinueTheTest(store);

                await store.ExecuteIndexAsync(new Replacement.Orders_ProfitByProductAndOrderedAt());

                WaitForIndexing(store);

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    // 2019-10-26
                    var doc = session.Load<ReduceOutputIdsReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(2, doc.ReduceOutputs.Count);

                    Replacement.Orders_ProfitByProductAndOrderedAt.Result output;

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        output = session.Load<Replacement.Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }

                    // 2019-10-24

                    doc = session.Load<ReduceOutputIdsReference>("reports/daily/2019-10-24", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(1, doc.ReduceOutputs.Count);

                    output = session.Load<Replacement.Orders_ProfitByProductAndOrderedAt.Result>(doc.ReduceOutputs[0]);

                    Assert.NotNull(output);

                    // 2019-10-25

                    doc = session.Load<ReduceOutputIdsReference>("reports/daily/2019-10-25", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(1, doc.ReduceOutputs.Count);

                    output = session.Load<Replacement.Orders_ProfitByProductAndOrderedAt.Result>(doc.ReduceOutputs[0]);

                    Assert.NotNull(output);
                }
            }
        }

        [Fact]
        public void OutputReferencesPatternTests()
        {
            var sut = new OutputReferencesPattern("reports/daily/{OrderedAt:yyyy-MM-dd}");

            using (sut.BuildReferenceDocumentId(out var builder))
            {
                builder.Add("OrderedAt", new DateTime(2019, 10, 26));

                Assert.Equal("reports/daily/2019-10-26", builder.GetId());
            }

            sut = new OutputReferencesPattern("output/{UserId}/{Date:yyyy-MM-dd}");

            using (sut.BuildReferenceDocumentId(out var builder))
            {
                builder.Add("Date", new DateTime(2019, 10, 26));
                builder.Add("UserId", "arek");

                Assert.Equal("output/arek/2019-10-26", builder.GetId());
            }
        }

        private class Orders_ProfitByProductAndOrderedAt : AbstractIndexCreationTask<Order, Orders_ProfitByProductAndOrderedAt.Result>
        {
            public class Result
            {
                public DateTime OrderedAt { get; set; }
                public string Product { get; set; }
                public decimal Profit { get; set; }
            }

            public Orders_ProfitByProductAndOrderedAt()
            {
                Map = orders => from order in orders
                    from line in order.Lines
                    select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                Reduce = results => from r in results
                    group r by new { r.OrderedAt, r.Product }
                    into g
                    select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                OutputReduceToCollection = "Profits";

                PatternOfReduceOutputReferences = x => $"reports/daily/{x.OrderedAt:yyyy-MM-dd}";
            }
        }

        private static class Replacement
        {
            public class Orders_ProfitByProductAndOrderedAt : AbstractIndexCreationTask<Order, Orders_ProfitByProductAndOrderedAt.Result>
            {
                public class Result
                {
                    public DateTime OrderedAt { get; set; }
                    public string MyProduct { get; set; }
                    public decimal MyProfit { get; set; }
                }

                public Orders_ProfitByProductAndOrderedAt()
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new Result { MyProduct = line.Product, OrderedAt = order.OrderedAt, MyProfit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { OrderedAt = r.OrderedAt, Product = r.MyProduct }
                        into g
                        select new Result
                        {
                            MyProduct = g.Key.Product, OrderedAt = g.Key.OrderedAt, MyProfit = g.Sum(r => r.MyProfit)
                        };

                    OutputReduceToCollection = "Profits";

                    PatternOfReduceOutputReferences = x => string.Format("reports/daily/{0:yyyy-MM-dd}", x.OrderedAt);

                }
            }
        }

        private static class DifferentApproachesToDefinePatternOfReduceOutputReferences
        {
            public class Result
            {
                public DateTime OrderedAt { get; set; }
                public string Product { get; set; }
                public decimal Profit { get; set; }
            }

            public class One_A : AbstractIndexCreationTask<Order, Result>
            {
                public One_A(string outputReduceToCollection = null)
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { r.OrderedAt, r.Product }
                        into g
                        select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                    OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                    PatternOfReduceOutputReferences = x => $"reports/daily/{x.OrderedAt:yyyy-MM-dd}";
                }
            }

            public class One_B : AbstractIndexCreationTask<Order, Result>
            {
                public One_B(string outputReduceToCollection = null)
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { r.OrderedAt, r.Product }
                        into g
                        select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                    OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                    PatternOfReduceOutputReferences = x => $"{x.OrderedAt:yyyy-MM-dd}reports/daily/{x.Profit:C}";
                }
            }

            public class Two_A : AbstractIndexCreationTask<Order, Result>
            {
                public Two_A(string outputReduceToCollection = null)
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { r.OrderedAt, r.Product }
                        into g
                        select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                    OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                    PatternOfReduceOutputReferences = x => "reports/daily/" + x.OrderedAt;
                }
            }

            public class Two_B : AbstractIndexCreationTask<Order, Result>
            {
                public Two_B(string outputReduceToCollection = null)
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { r.OrderedAt, r.Product }
                        into g
                        select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                    OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                    PatternOfReduceOutputReferences = x => "reports/daily/" + x.OrderedAt + "/product/" + x.Product;
                }
            }

            public class Three_A : AbstractIndexCreationTask<Order, Result>
            {
                public Three_A(string outputReduceToCollection = null)
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { r.OrderedAt, r.Product }
                        into g
                        select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                    OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                    PatternOfReduceOutputReferences = x => string.Format("reports/daily/{0:MM/dd/yyyy}", x.OrderedAt);
                }
            }

            public class Three_B : AbstractIndexCreationTask<Order, Result>
            {
                public Three_B(string outputReduceToCollection = null)
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { r.OrderedAt, r.Product }
                        into g
                        select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                    OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                    PatternOfReduceOutputReferences = x => string.Format("reports/daily/{0:MM/dd/yyyy}/{1}", x.OrderedAt, x.Product);
                }
            }
        }
    }
}
