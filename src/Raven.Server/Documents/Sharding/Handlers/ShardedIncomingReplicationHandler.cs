﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Extensions;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Server.Json.Sync;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedIncomingReplicationHandler : AbstractIncomingReplicationHandler
    {
        private readonly ShardedDatabaseContext.ShardedReplicationContext _parent;
        private readonly ReplicationQueue _replicationQueue;
        private long _lastDocumentEtag;

        public long LastDocumentEtag => _lastDocumentEtag;

        public ShardedIncomingReplicationHandler(TcpConnectionOptions tcpConnectionOptions, ShardedDatabaseContext.ShardedReplicationContext parent,
            JsonOperationContext.MemoryBuffer buffer, ReplicationLatestEtagRequest replicatedLastEtag, ReplicationQueue replicationQueue)
            : base(tcpConnectionOptions, buffer, parent.Server, parent.DatabaseName, replicatedLastEtag, parent.Context.DatabaseShutdown)
        {
            _parent = parent;
            _replicationQueue = replicationQueue;
            _attachmentStreamsTempFile = new StreamsTempFile("ShardedReplication" + Guid.NewGuid(), false);
        }

        protected override void ReceiveReplicationBatches()
        {
            NativeMemory.EnsureRegistered();
            try
            {

                using (_tcpConnectionOptions.ConnectionProcessingInProgress("Replication"))
                using (_parent.Server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (_stream)
                {
                    while (_cts.IsCancellationRequested == false)
                    {
                        try
                        {
                            using (var msg = context.Sync.ParseToMemory(
                                       _stream,
                                       "IncomingReplication/read-message",
                                       BlittableJsonDocumentBuilder.UsageMode.None,
                                       _copiedBuffer.Buffer))
                            {
                                if (msg != null)
                                {
                                    _parent.EnsureNotDeleted(_parent.Server.NodeTag);

                                    using (var writer = new BlittableJsonTextWriter(context, _stream))
                                    {
                                        HandleSingleReplicationBatch(context,
                                            msg,
                                            writer);
                                    }
                                }
                                else // notify peer about new change vector
                                {
                                    using (var writer = new BlittableJsonTextWriter(context, _stream))
                                    {
                                        SendHeartbeatStatusToSource(
                                            context,
                                            writer,
                                            _lastDocumentEtag,
                                            "Notify");
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                            {
                                if (e is AggregateException ae &&
                                    ae.InnerExceptions.Count == 1 &&
                                    ae.InnerException is SocketException ase)
                                {
                                    HandleSocketException(ase);
                                }
                                else if (e.InnerException is SocketException se)
                                {
                                    HandleSocketException(se);
                                }
                                else
                                {
                                    //if we are disposing, do not notify about failure (not relevant)
                                    if (_parent.Context.DatabaseShutdown.IsCancellationRequested == false)
                                        if (_log.IsInfoEnabled)
                                            _log.Info("Received unexpected exception while receiving replication batch.", e);
                                }
                            }

                            throw;
                        }

                        void HandleSocketException(SocketException e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Failed to read data from incoming connection. The incoming connection will be closed and re-created.", e);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                //if we are disposing, do not notify about failure (not relevant)
                if (_cts.IsCancellationRequested == false)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Connection error {FromToString}: an exception was thrown during receiving incoming document replication batch.", e);
                }
            }
        }

        private void HandleSingleReplicationBatch(
    TransactionOperationContext context,
    BlittableJsonReaderObject message,
    BlittableJsonTextWriter writer)
        {
            message.BlittableValidation();
            //note: at this point, the valid messages are heartbeat and replication batch.
            _cts.Token.ThrowIfCancellationRequested();
            string messageType = null;
            try
            {
                if (!message.TryGet(nameof(ReplicationMessageHeader.Type), out messageType))
                    throw new InvalidDataException("Expected the message to have a 'Type' field. The property was not found");

                if (!message.TryGet(nameof(ReplicationMessageHeader.LastDocumentEtag), out _lastDocumentEtag))
                    throw new InvalidOperationException("Expected LastDocumentEtag property in the replication message, " +
                                                        "but didn't find it..");

                switch (messageType)
                {
                    case ReplicationMessageType.Documents:

                        if (!message.TryGet(nameof(ReplicationMessageHeader.ItemsCount), out int itemsCount))
                            throw new InvalidDataException($"Expected the '{nameof(ReplicationMessageHeader.ItemsCount)}' field, " +
                                                           $"but had no numeric field of this value, this is likely a bug");

                        if (!message.TryGet(nameof(ReplicationMessageHeader.AttachmentStreamsCount), out int attachmentStreamCount))
                            throw new InvalidDataException($"Expected the '{nameof(ReplicationMessageHeader.AttachmentStreamsCount)}' field, " +
                                                           $"but had no numeric field of this value, this is likely a bug");

                        ReceiveSingleDocumentsBatch(context, itemsCount, attachmentStreamCount);

                        break;

                    case ReplicationMessageType.Heartbeat:
                        break;

                    default:
                        throw new ArgumentOutOfRangeException("Unknown message type: " + messageType);
                }

                SendHeartbeatStatusToSource(context, writer, _lastDocumentEtag, messageType);

            }
            catch (ObjectDisposedException)
            {
                //we are shutting down replication, this is ok
            }
            catch (EndOfStreamException e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Received unexpected end of stream while receiving replication batches. " +
                              "This might indicate an issue with network.", e);
                throw;
            }
            catch (Exception e)
            {
                //if we are disposing, ignore errors
                if (_cts.IsCancellationRequested)
                    return;

                DynamicJsonValue returnValue;

                if (e.ExtractSingleInnerException() is MissingAttachmentException mae)
                {
                    returnValue = new DynamicJsonValue
                    {
                        [nameof(ReplicationMessageReply.Type)] = ReplicationMessageReply.ReplyType.MissingAttachments.ToString(),
                        [nameof(ReplicationMessageReply.MessageType)] = messageType,
                        [nameof(ReplicationMessageReply.LastEtagAccepted)] = -1,
                        [nameof(ReplicationMessageReply.Exception)] = mae.ToString()
                    };

                    context.Write(writer, returnValue);
                    writer.Flush();

                    return;
                }

                if (_log.IsInfoEnabled)
                    _log.Info($"Failed replicating documents {FromToString}.", e);

                //return negative ack
                returnValue = new DynamicJsonValue
                {
                    [nameof(ReplicationMessageReply.Type)] = ReplicationMessageReply.ReplyType.Error.ToString(),
                    [nameof(ReplicationMessageReply.MessageType)] = messageType,
                    [nameof(ReplicationMessageReply.LastEtagAccepted)] = -1,
                    [nameof(ReplicationMessageReply.Exception)] = e.ToString()
                };

                context.Write(writer, returnValue);
                writer.Flush();

                throw;
            }
        }

        private void ReceiveSingleDocumentsBatch(TransactionOperationContext context, int replicatedItemsCount, int attachmentStreamCount)
        {
            var sw = Stopwatch.StartNew();
            var stats = new IncomingReplicationStatsScope(new IncomingReplicationRunStats());

            try
            {
                using (_attachmentStreamsTempFile.Scope())
                using (var shardAllocator = new IncomingReplicationAllocator(context.Allocator, maxSizeToSend: null))
                {
                    using (var networkStats = stats.For(ReplicationOperation.Incoming.Network))
                    {
                        var reader = new Reader(_stream, _copiedBuffer, shardAllocator);

                        var dictionary = ReadItemsFromSource(replicatedItemsCount, context, reader, stats);
                        ReadAttachmentStreamsFromSource(attachmentStreamCount, context, reader, networkStats);

                        foreach (var kvp in dictionary)
                        {
                            var shard = kvp.Key;
                            _replicationQueue.Items[shard].Add(kvp.Value);
                        }

                        _replicationQueue.SendToShardCompletion.Wait();
                        _replicationQueue.SendToShardCompletion = new CountdownEvent(_parent.Context.ShardCount);
                    }
                }

                sw.Stop();
            }
            catch (Exception)
            {
                // ignore this failure, if this failed, we are already
                // in a bad state and likely in the process of shutting
                // down
            }
        }

        private Dictionary<int, List<ReplicationBatchItem>> ReadItemsFromSource(int replicatedDocs, TransactionOperationContext context, Reader reader, IncomingReplicationStatsScope stats)
        {
            var dict = new Dictionary<int, List<ReplicationBatchItem>>();

            for (var shard = 0; shard < _parent.Context.ShardCount; shard++)
            {
                dict[shard] = new List<ReplicationBatchItem>();
            }

            for (var i = 0; i < replicatedDocs; i++)
            {
                var item = ReadItemFromSource(reader, context, context.Allocator, stats);

                int shard = GetShardNumberForReplicationItem(context, item);

                if (item is AttachmentReplicationItem attachment)
                {
                    var shardAttachments = _replicationQueue.AttachmentsPerShard[shard] ??= new Dictionary<Slice, AttachmentReplicationItem>(SliceComparer.Instance);

                    if (shardAttachments.ContainsKey(attachment.Base64Hash) == false)
                        shardAttachments[attachment.Base64Hash] = attachment;
                }

                var list = dict[shard];
                list.Add(item);
            }

            return dict;
        }

        private void ReadAttachmentStreamsFromSource(int attachmentStreamCount, TransactionOperationContext context, Reader reader, IncomingReplicationStatsScope stats)
        {
            if (attachmentStreamCount == 0)
                return;

            for (var i = 0; i < attachmentStreamCount; i++)
            {
                var attachment = (AttachmentReplicationItem)ReplicationBatchItem.ReadTypeAndInstantiate(reader);
                Debug.Assert(attachment.Type == ReplicationBatchItem.ReplicationItemType.AttachmentStream);

                using (stats.For(ReplicationOperation.Incoming.AttachmentRead))
                {
                    attachment.ReadStream(context.Allocator, _attachmentStreamsTempFile);

                    for (var shard = 0; shard < _parent.Context.ShardCount; shard++)
                    {
                        var shardAttachments = _replicationQueue.AttachmentsPerShard[shard];

                        if (shardAttachments.ContainsKey(attachment.Base64Hash))
                        {
                            var attachmentStream = new AttachmentReplicationItem
                            {
                                Type = ReplicationBatchItem.ReplicationItemType.AttachmentStream,
                                Base64Hash = attachment.Base64Hash,
                                Stream = new MemoryStream()
                            };

                            _attachmentStreamsTempFile._file.InnerStream.CopyTo(attachmentStream.Stream);
                            shardAttachments[attachment.Base64Hash] = attachmentStream;
                        }
                    }
                }
            }
        }

        private void SendHeartbeatStatusToSource(JsonOperationContext context, BlittableJsonTextWriter writer, long lastDocumentEtag, string handledMessageType)
        {
            var heartbeat = new DynamicJsonValue
            {
                [nameof(ReplicationMessageReply.Type)] = "Ok",
                [nameof(ReplicationMessageReply.MessageType)] = handledMessageType,
                [nameof(ReplicationMessageReply.LastEtagAccepted)] = lastDocumentEtag,
                [nameof(ReplicationMessageReply.Exception)] = null,
                [nameof(ReplicationMessageReply.NodeTag)] = _parent.Server.NodeTag
            };

            context.Write(writer, heartbeat);
            writer.Flush();
        }


        private readonly DocumentInfoHelper _documentInfoHelper = new DocumentInfoHelper();
        private LazyStringValue GetDocumentId(Slice key) => _documentInfoHelper.GetDocumentId(key);
        public string GetItemInformation(ReplicationBatchItem item) => _documentInfoHelper.GetItemInformation(item);

        public int GetShardNumberForReplicationItem(TransactionOperationContext context, ReplicationBatchItem item)
        {
            return item switch
            {
                AttachmentReplicationItem a => _parent.Context.GetShardNumber(context, (GetDocumentId(a.Key))),
                AttachmentTombstoneReplicationItem at => _parent.Context.GetShardNumber(context, (GetDocumentId(at.Key))),
                CounterReplicationItem c => _parent.Context.GetShardNumber(context, c.Id),
                DocumentReplicationItem d => _parent.Context.GetShardNumber(context, d.Id),
                RevisionTombstoneReplicationItem _ => throw new NotSupportedInShardingException("TODO: implement for sharding"), // revision tombstones doesn't contain any info about the doc. The id here is the change-vector of the deleted revision
                TimeSeriesDeletedRangeItem td => _parent.Context.GetShardNumber(context, (GetDocumentId(td.Key))),
                TimeSeriesReplicationItem t => _parent.Context.GetShardNumber(context, (GetDocumentId(t.Key))),
                _ => throw new ArgumentOutOfRangeException($"{nameof(item)} - {item}")
            };
        }
    }
}
