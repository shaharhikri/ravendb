using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Replication)]
    public class ReplicationConfiguration : ConfigurationCategory
    {
        [Description("Threshold under which an incoming replication connection is considered active. If an incoming connection receives messages within this time-span, new connection coming from the same source would be rejected (as the existing connection is considered active)")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Replication.ActiveConnectionTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting ActiveConnectionTimeout { get; set; }
        
        [Description("Minimal time in seconds before sending another heartbeat")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Replication.ReplicationMinimalHeartbeatInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting ReplicationMinimalHeartbeat { get; set; }

        [Description("If the replication failed, we try to replicate again after the specified time elapsed.")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Replication.RetryReplicateAfterInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting RetryReplicateAfter { get; set; }

        [Description("Max retry timeout in seconds on replication failure.")]
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Replication.RetryMaxTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting RetryMaxTimeout { get; set; }

        [Description("Maximum number of items replication will send in single batch, null means we will not cut the batch by number of items")]
        [DefaultValue(16*1024)]
        [ConfigurationEntry("Replication.MaxItemsCount", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int? MaxItemsCount { get; set; }

        [Description("Maximum number of data size replication will send in single batch, null means we will not cut the batch by the size")]
        [DefaultValue(64)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Replication.MaxSizeToSendInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size? MaxSizeToSend { get; set; }

        [Description("Maximum number of data size to load from storage to memory before sending it")]
        [DefaultValue(1024)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Replication.MaxSizeToLoadFromStorage", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size? MaxSizeToLoadFromStorage { get; set; }
    }
}
