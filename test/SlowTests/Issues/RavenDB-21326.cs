using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21326 : RavenTestBase
    {
        public RavenDB_21326(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task TestReadAndWriteLastRevisionsBinCleanerState()
        {
            using (var store = GetDocumentStore())
            {
                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    db.DocumentsStorage.SetLastRevisionsBinCleanerState(context, new RevisionsBinCleaner.RevisionsBinCleanerState()
                    {
                        Etag = 1234567890123456789,
                        Skip = 8765432109876543211
                    });
                    tx.Commit();
                }

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var state = DocumentsStorage.ReadLastRevisionsBinCleanerState(context.Transaction.InnerTransaction);

                    Assert.Equal(1234567890123456789, state.Etag);
                    Assert.Equal(8765432109876543211, state.Skip);
                }
            }
        }


        [RavenTheory(RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RevisionsBinCleanerTest(Options options)
        {
            using var store = GetDocumentStore(options);

            var revisionsConfig = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, store.Database, revisionsConfig);

            var user1 = new User { Id = "Users/1-A", Name = "Shahar" };
            var user2 = new User { Id = "Users/2-B", Name = "Shahar" };
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user1);
                await session.StoreAsync(user2);
                await session.SaveChangesAsync();

                for (int i = 1; i <= 10; i++)
                {
                    (await session.LoadAsync<User>(user1.Id)).Name = $"Shahar{i}";
                    (await session.LoadAsync<User>(user2.Id)).Name = $"Shahar{i}";
                    await session.SaveChangesAsync();
                }

                session.Delete(user1.Id);
                session.Delete(user2.Id);
                await session.SaveChangesAsync();

                await session.StoreAsync(user2); // revive user2
                await session.SaveChangesAsync();

                Assert.Equal(12, await session.Advanced.Revisions.GetCountForAsync(user1.Id));
                Assert.Equal(13, await session.Advanced.Revisions.GetCountForAsync(user2.Id));
            }

            var config = new RevisionsBinConfiguration
            {
                MinimumEntriesAgeToKeep = TimeSpan.Zero,
                RefreshFrequency = TimeSpan.FromMilliseconds(200)
            };
            await ConfigRevisionsBinCleaner(store, config);

            await AssertWaitForValueAsync(async () =>
            {
                using (var session = store.OpenAsyncSession())
                {
                    return await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                }
            }, 0);

            // End Cleanup
            config.Disabled = true;
            await ConfigRevisionsBinCleaner(store, config);

            using (var session = store.OpenAsyncSession())
            {
                session.Delete(user2.Id);
                await session.SaveChangesAsync(); // delete user2
            }

            using (var session = store.OpenAsyncSession())
            {
               Assert.Equal(14, await session.Advanced.Revisions.GetCountForAsync(user2.Id));
            }

        }

        private async Task ConfigRevisionsBinCleaner(DocumentStore store, RevisionsBinConfiguration config)
        {
            var result = await store.Maintenance.SendAsync(new ConfigureRevisionsBinCleanerOperation(config));
            await store.Maintenance.SendAsync(new WaitForIndexNotificationOperation(result.RaftCommandIndex.Value));

            await AssertWaitForTrueAsync(async () =>
            {
                var record = await GetDatabaseRecordAsync(store);
                return config.Equals(record.RevisionsBin);
            });
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

    }
}
