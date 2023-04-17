﻿using System;
using System.Threading;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.ServerWide
{
    public class RavenTransaction : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenTransaction>("Server");

        public Transaction InnerTransaction;

        public RavenTransaction(Transaction transaction)
        {
            InnerTransaction = transaction;
        }

        public virtual void BeforeCommit()
        {

        }

        public void Commit()
        {
            BeforeCommit();
            InnerTransaction.Commit();
        }

        public void EndAsyncCommit()
        {
            InnerTransaction.EndAsyncCommit();
        }

        public bool Disposed;

        public virtual void Dispose()
        {
            if (Disposed)
                return;

            Disposed = true;

            var committed = InnerTransaction.LowLevelTransaction.Committed;

            InnerTransaction?.Dispose();
            InnerTransaction = null;

            if (committed)
                AfterCommit();
        }

        protected virtual void RaiseNotifications()
        {
        }

        protected virtual bool ShouldRaiseNotifications()
        {
            return false;
        }

        protected static void ThrowInvalidTransactionUsage()
        {
            throw new InvalidOperationException("There is a different transaction in context.");
        }

        private void AfterCommit()
        {
            if (ShouldRaiseNotifications() == false)
                return;

            ThreadPool.QueueUserWorkItem(state =>
            {
                try
                {
                    ((RavenTransaction)state).RaiseNotifications();
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Failed to raise notifications", e);
                }
            }, this);
        }
    }
}
