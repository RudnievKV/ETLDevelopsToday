using ETLDevelopsToday.Models.Entity;
using ETLDevelopsToday.Services.Abstract;
using ETLDevelopsToday.Utils.Settings;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Text;

namespace ETLDevelopsToday.Services
{
    public class DbService : IDbService
    {
        private readonly ILogger<DbService> _logger;
        private readonly DatabaseSettings _databaseSettings;

        public DbService(DatabaseSettings databaseSettings, ILogger<DbService> logger)
        {
            _logger = logger;
            _databaseSettings = databaseSettings;
        }

        public async Task BulkInsert(IEnumerable<TaxiTrip> records, CancellationToken cancellationToken = default)
        {
            var dataTable = BuildDataTable(records);

            using var conn = new SqlConnection(_databaseSettings.ConnectionString);
            await conn.OpenAsync(cancellationToken);

            var options = SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.TableLock;
            using var bulkCopy = new SqlBulkCopy(conn, options, null)
            {
                DestinationTableName = $"dbo.{_databaseSettings.TableName}",
                BatchSize = _databaseSettings.BulkCopyBatchSize,
                BulkCopyTimeout = _databaseSettings.BulkCopyTimeout,
                EnableStreaming = true
            };

            bulkCopy.ColumnMappings.Add("tpep_pickup_datetime", "tpep_pickup_datetime");
            bulkCopy.ColumnMappings.Add("tpep_dropoff_datetime", "tpep_dropoff_datetime");
            bulkCopy.ColumnMappings.Add("passenger_count", "passenger_count");
            bulkCopy.ColumnMappings.Add("trip_distance", "trip_distance");
            bulkCopy.ColumnMappings.Add("store_and_fwd_flag", "store_and_fwd_flag");
            bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
            bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
            bulkCopy.ColumnMappings.Add("fare_amount", "fare_amount");
            bulkCopy.ColumnMappings.Add("tip_amount", "tip_amount");

            _logger.LogInformation("Starting SqlBulkCopy for {Rows} rows...", dataTable.Rows.Count);
            await bulkCopy.WriteToServerAsync(dataTable, cancellationToken);
            _logger.LogInformation("SqlBulkCopy completed.");
        }

        public async Task EnsureDatabaseAndTable(CancellationToken cancellationToken = default)
        {
            await EnsureDatabase(cancellationToken);
            await EnsureTable(cancellationToken);
        }
        private async Task EnsureDatabase(CancellationToken cancellationToken)
        {
            var builder = new SqlConnectionStringBuilder(_databaseSettings.ConnectionString);
            var targetDb = _databaseSettings.DatabaseName;
            var originalDb = builder.InitialCatalog;

            builder.InitialCatalog = "master";
            using var conn = new SqlConnection(builder.ToString());
            await conn.OpenAsync(cancellationToken);

            var sql = $"IF DB_ID(@db) IS NULL CREATE DATABASE [{targetDb}]";
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.Add(new SqlParameter("@db", SqlDbType.NVarChar, 128) { Value = targetDb });
            await cmd.ExecuteNonQueryAsync(cancellationToken);

            // Restore InitialCatalog for subsequent operations
            builder.InitialCatalog = originalDb;
        }

        private async Task EnsureTable(CancellationToken cancellationToken)
        {
            using var conn = new SqlConnection(_databaseSettings.ConnectionString);
            await conn.OpenAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($@"
                IF OBJECT_ID('dbo.{_databaseSettings.TableName}', 'U') IS NULL
                BEGIN
                    CREATE TABLE dbo.{_databaseSettings.TableName} (
                        Id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_{_databaseSettings.TableName} PRIMARY KEY CLUSTERED,
                        tpep_pickup_datetime  datetimeoffset(4) NOT NULL,
                        tpep_dropoff_datetime datetimeoffset(4) NOT NULL,
                        passenger_count       TINYINT NOT NULL,
                        trip_distance         DECIMAL(9,3) NOT NULL,
                        store_and_fwd_flag    VARCHAR(10) NOT NULL,
                        PULocationID          INT NOT NULL,
                        DOLocationID          INT NOT NULL,
                        fare_amount           DECIMAL(10,2) NOT NULL,
                        tip_amount            DECIMAL(10,2) NOT NULL
                    );

                    CREATE NONCLUSTERED INDEX IX_{_databaseSettings.TableName}_PULocationID_Tip ON dbo.{_databaseSettings.TableName}(PULocationID) INCLUDE (tip_amount);
                    CREATE NONCLUSTERED INDEX IX_{_databaseSettings.TableName}_TripDistance ON dbo.{_databaseSettings.TableName}(trip_distance DESC);
                    CREATE NONCLUSTERED INDEX IX_{_databaseSettings.TableName}_Duration ON {_databaseSettings.TableName}(tpep_pickup_datetime, tpep_dropoff_datetime);
                    CREATE NONCLUSTERED INDEX IX_{_databaseSettings.TableName}_PULocationID ON {_databaseSettings.TableName}(PULocationID);
                END
                ");
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        private DataTable BuildDataTable(IEnumerable<TaxiTrip> records)
        {
            var table = new DataTable();
            table.Columns.Add("tpep_pickup_datetime", typeof(DateTimeOffset));
            table.Columns.Add("tpep_dropoff_datetime", typeof(DateTimeOffset));
            table.Columns.Add("passenger_count", typeof(byte));
            table.Columns.Add("trip_distance", typeof(decimal));
            table.Columns.Add("store_and_fwd_flag", typeof(string));
            table.Columns.Add("PULocationID", typeof(int));
            table.Columns.Add("DOLocationID", typeof(int));
            table.Columns.Add("fare_amount", typeof(decimal));
            table.Columns.Add("tip_amount", typeof(decimal));

            foreach (var r in records)
            {
                var row = table.NewRow();
                row["tpep_pickup_datetime"] = r.PickupDatetime;
                row["tpep_dropoff_datetime"] = r.DropoffDatetime;
                row["passenger_count"] = (byte)Math.Clamp(r.PassengerCount, 0, 255);
                row["trip_distance"] = r.TripDistance;
                row["store_and_fwd_flag"] = r.StoreAndFwdFlag;
                row["PULocationID"] = r.PULocationID;
                row["DOLocationID"] = r.DOLocationID;
                row["fare_amount"] = r.FareAmount;
                row["tip_amount"] = r.TipAmount;
                table.Rows.Add(row);
            }

            return table;
        }
    }
}
