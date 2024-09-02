using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Operations.Revisions
{
    public sealed class ConfigureRevisionsBinCleanerOperationResult
    {
        public long? RaftCommandIndex { get; set; }
    }
}
