namespace ETLDevelopsToday.Utils.Settings
{
    public class DatabaseSettings
    {
        public string ConnectionString { get; set; } = null!;
        public string DatabaseName { get; set; } = null!;
        public string TableName { get; set; } = null!;
        public int BulkCopyBatchSize { get; set; }
        public int BulkCopyTimeout { get; set; }
    }
}
