﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Studio
{
    internal class StudioCollectionRunner : CollectionRunner
    {
        private readonly HashSet<string> _excludeIds;

        public StudioCollectionRunner(DocumentDatabase database, DocumentsOperationContext context, HashSet<string> excludeIds) : base(database, context, null)
        {
            _excludeIds = excludeIds;
        }

        public override Task<IOperationResult> ExecuteDelete(string collectionName, long start, long take, CollectionOperationOptions options, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            if (_excludeIds.Count == 0)
                return base.ExecuteDelete(collectionName, start, take, options, onProgress, token);

            // specific collection w/ exclusions
            return ExecuteOperation(collectionName, start, take, options, Context, onProgress, key =>
            {
                if (_excludeIds.Contains(key) == false)
                    return new DeleteDocumentCommand(key, null, Database);

                return null;
            }, token);
        }
    }
}
