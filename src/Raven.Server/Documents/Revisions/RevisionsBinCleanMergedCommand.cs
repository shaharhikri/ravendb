using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Voron;
using static Raven.Server.Documents.RevisionsBinCleaner;
using static Raven.Server.Utils.MetricCacher.Keys;

namespace Raven.Server.Documents.Revisions;

public partial class RevisionsStorage
{
    internal sealed class RevisionsBinCleanMergedCommand : DocumentMergedTransactionCommand
    {
        public DateTime _before;

        public long _maxTotalDeletes;

        public long _maxReadsPerBatch;

        public (long DeletedEntries, bool HasMore)? Result { get; private set; }

        public RevisionsBinCleanMergedCommand(DateTime before, long maxTotalDeletes, long maxReadsPerBatch)
        {
            _before = before;
            _maxTotalDeletes = maxTotalDeletes;
            _maxReadsPerBatch = maxReadsPerBatch;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            var state = DocumentsStorage.ReadLastRevisionsBinCleanerState(context.Transaction.InnerTransaction) ?? new RevisionsBinCleanerState();
            var newState = state.Clone();
            Result = DeleteRevisions(context, newState);
            if(newState.Equals(state) == false)
                context.DocumentDatabase.DocumentsStorage.SetLastRevisionsBinCleanerState(context, newState);
            return 1;
        }

        private (long DeletedRevisions, bool HasMore)? DeleteRevisions(DocumentsOperationContext context, RevisionsBinCleanerState state)
        {
            if (_maxReadsPerBatch == 0)
                return (0, false);

            var revisionsStorage = context.DocumentDatabase.DocumentsStorage.RevisionsStorage;
            var revisions = revisionsStorage.GetRevisionsBinEntries(context, _before, DocumentFields.Id | DocumentFields.ChangeVector, state); // forget about enumerator

            var deletedRevisions = 0L;
            var deletedEntries = 0L;
            var numOfReads = 0L;

            foreach (var r in revisions)
            {
                numOfReads++;
                var id = r.Id;
                using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
                using (revisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefixSlice))
                {
                    var collectionName = revisionsStorage.GetCollectionFor(context, prefixSlice);
                    if (collectionName == null)
                    {
                        if (revisionsStorage._logger.IsInfoEnabled)
                            revisionsStorage._logger.Info($"Tried to delete revisions for '{id}' but no revisions found.");
                        continue;
                    }

                    var remainingDeletes = _maxTotalDeletes - deletedRevisions;
                    var result = revisionsStorage.ForceDeleteAllRevisionsFor(context, lowerId, prefixSlice, collectionName, remainingDeletes, shouldSkip: null);

                    if (result.MoreWork == false)
                    {
                        deletedEntries++;
                        // long lastEtag = ChangeVectorUtils.GetEtagById(r.ChangeVector, context.DocumentDatabase.DbBase64Id);
                        // state.Etag = lastEtag;
                    }

                    deletedRevisions += result.Deleted;
                    if (deletedRevisions >= _maxTotalDeletes)
                        break;
                }

                if (numOfReads == _maxReadsPerBatch)
                    break;
            }

            var hasMore = (deletedRevisions < _maxTotalDeletes && numOfReads < _maxReadsPerBatch) == false;
            return (deletedEntries, hasMore);
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new RevisionsBinCleanMergedCommandDto(_before, _maxTotalDeletes, _maxReadsPerBatch);
        }

        public sealed class RevisionsBinCleanMergedCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, RevisionsBinCleanMergedCommand>
        {
            public DateTime _before;

            public long _maxTotalDeletes;

            public long _maxReadsPerBatch;

            public RevisionsBinCleanMergedCommandDto(DateTime before, long maxTotalDeletes, long maxReadsPerBatch)
            {
                _before = before;
                _maxTotalDeletes = maxTotalDeletes;
                _maxReadsPerBatch = maxReadsPerBatch;
            }

            public RevisionsBinCleanMergedCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new RevisionsBinCleanMergedCommand(_before, _maxTotalDeletes, _maxReadsPerBatch);
            }
        }
    }
}
