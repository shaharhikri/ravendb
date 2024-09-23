﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Voron.Data.Tables;
using static Raven.Server.Documents.RevisionsBinCleaner;

namespace Raven.Server.Documents.Revisions
{
    public partial class RevisionsStorage
    {
        internal class TestingStuff
        {
            private RevisionsStorage _parent;

            public TestingStuff(RevisionsStorage revisionsStorage)
            {
                _parent = revisionsStorage;
            }

            internal void DeleteLastRevisionFor(DocumentsOperationContext context, string id, string collection)
            {
                var collectionName = new CollectionName(collection);
                using (DocumentIdWorker.GetSliceFromId(context, id, out var lowerId))
                using (_parent.GetKeyPrefix(context, lowerId, out var lowerIdPrefix))
                using (GetKeyWithEtag(context, lowerId, etag: long.MaxValue, out var compoundPrefix))
                {
                    var table = _parent.EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
                    var holder = table.SeekOneBackwardFrom(_parent.RevisionsSchema.Indexes[Schemas.Revisions.IdAndEtagSlice], lowerIdPrefix, compoundPrefix);
                    var lastRevision = TableValueToRevision(context, ref holder.Reader, DocumentFields.ChangeVector | DocumentFields.LowerId);
                    _parent.DeleteRevisionFromTable(context, table, new Dictionary<string, Table>(), lastRevision, collectionName, context.GetChangeVector(lastRevision.ChangeVector), _parent._database.Time.GetUtcNow().Ticks, lastRevision.Flags);
                    IncrementCountOfRevisions(context, lowerIdPrefix, -1);

                    var revisionsBinCleanerState = DocumentsStorage.ReadLastRevisionsBinCleanerState(context.Transaction.InnerTransaction);
                    if (_parent.UpdateRevisionsBinCleanerStateIfNeeded(context, revisionsBinCleanerState, lastRevision))
                        _parent._documentsStorage.SetLastRevisionsBinCleanerState(context, revisionsBinCleanerState);
                }
            }
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff(this);
        }
    }
}
