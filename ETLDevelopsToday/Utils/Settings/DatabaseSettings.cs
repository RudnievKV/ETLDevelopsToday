using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLDevelopsToday.Utils.Settings
{
    public class DatabaseSettings
    {
        public required string ConnectionString { get; set; }
        public required string DatabaseName { get; set; }
        public required string TableName { get; set; }
        public int BulkCopyBatchSize { get; set; }
        public int BulkCopyTimeout { get; set; }
    }
}
