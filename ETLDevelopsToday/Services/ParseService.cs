using CsvHelper;
using CsvHelper.Configuration;
using ETLDevelopsToday.Models.Entity;
using ETLDevelopsToday.Services.Abstract;
using ETLDevelopsToday.Utils.Settings;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Text;

namespace ETLDevelopsToday.Services
{
    public class ParseService : IParseService
    {
        private readonly ILogger<ParseService> _logger;
        private readonly IDbService _dbService;
        private readonly DatabaseSettings _databaseSettings;
        private readonly TimeZoneInfo _estZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
        public ParseService(ILogger<ParseService> logger, IDbService dbService, DatabaseSettings databaseSettings)
        {
            _logger = logger;
            _dbService = dbService;
            _databaseSettings = databaseSettings;
        }


        public async Task ProcessCsvAndIngest(
            string filePath,
            string? duplicatesOutputPath = null,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("filePath is required", nameof(filePath));
            if (!File.Exists(filePath))
                throw new FileNotFoundException("CSV file not found", filePath);


            await _dbService.EnsureDatabaseAndTable(cancellationToken);

            var originalRowKeys = new HashSet<string>(StringComparer.Ordinal);
            var errors = 0;
            var inserted = 0;
            var duplicates = 0;

            var duplicateFilePath = duplicatesOutputPath ??
                          Path.Combine(AppContext.BaseDirectory, "duplicates.csv");

            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(duplicateFilePath))!);

            await using var duplicateFileWriter = new StreamWriter(duplicateFilePath, append: false, Encoding.UTF8);
            await duplicateFileWriter.WriteLineAsync("tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,store_and_fwd_flag,PULocationID,DOLocationID,fare_amount,tip_amount");

            var batch = new List<TaxiTrip>(capacity: Math.Min(_databaseSettings.BulkCopyBatchSize, 10000));

            using var reader = new StreamReader(filePath, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
            var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                Delimiter = ",",
                TrimOptions = TrimOptions.Trim,
                IgnoreBlankLines = true,
                BadDataFound = null,
                MissingFieldFound = null,
                HeaderValidated = null
            };
            using var csv = new CsvReader(reader, csvConfig);

            if (!await csv.ReadAsync())
            {
                _logger.LogInformation("No data found in file {File}. Nothing to process.", filePath);
                return;
            }

            csv.ReadHeader();
            var headers = csv.HeaderRecord ?? Array.Empty<string>();
            var map = BuildHeaderMap(headers);

            while (await csv.ReadAsync())
            {
                cancellationToken.ThrowIfCancellationRequested();

                string[]? fieldsForLog = null;
                try
                {
                    fieldsForLog = csv.Context.Parser.Record;

                    var trip = MapToEntity(csv, map);
                    if (trip == null)
                    {
                        errors++;
                        continue;
                    }

                    var key = trip.GetDeduplicationKey();
                    if (originalRowKeys.Add(key))
                    {
                        batch.Add(trip);
                        if (batch.Count >= _databaseSettings.BulkCopyBatchSize)
                        {
                            await _dbService.BulkInsert(batch, cancellationToken);
                            inserted += batch.Count;
                            batch.Clear();
                        }
                    }
                    else
                    {
                        duplicates++;
                        await WriteToDuplicatesFile(duplicateFileWriter, trip);
                    }
                }
                catch (Exception ex)
                {
                    errors++;
                    _logger.LogWarning(ex, "Skipping malformed row: {Row}", fieldsForLog is { Length: > 0 } ? string.Join("|", fieldsForLog) : "(null)");
                }
            }

            // Flush remaining rows
            if (batch.Count > 0)
            {
                await _dbService.BulkInsert(batch, cancellationToken);
                inserted += batch.Count;
                batch.Clear();
            }

            _logger.LogInformation("Completed processing. Inserted={Inserted}, Duplicates={Duplicates}, Errors={Errors}.", inserted, duplicates, errors);
        }

        private async Task WriteToDuplicatesFile(StreamWriter dupWriter, TaxiTrip trip)
        {
            var line =
                $"{trip.PickupDatetime:yyyy-MM-dd HH:mm:ss}," +
                $"{trip.DropoffDatetime:yyyy-MM-dd HH:mm:ss}," +
                $"{trip.PassengerCount}," +
                $"{trip.TripDistance}," +
                $"{Escape(trip.StoreAndFwdFlag)}," +
                $"{trip.PULocationID}," +
                $"{trip.DOLocationID}," +
                $"{trip.FareAmount}," +
                $"{trip.TipAmount}";
            await dupWriter.WriteLineAsync(line);
        }

        private string Escape(string value)
        {
            if (value.Contains(',') || value.Contains('"'))
                return $"\"{value.Replace("\"", "\"\"")}\"";
            return value;
        }

        private TaxiTrip? MapToEntity(CsvReader csv, Dictionary<string, int> _)
        {
            string Get(string name) => csv.TryGetField<string>(name, out var v) ? v?.Trim() ?? "" : "";

            var pickupStr = Get("tpep_pickup_datetime");
            var dropoffStr = Get("tpep_dropoff_datetime");

            if (!TryParseUsDateTime(pickupStr, out var pickupLocal) || !TryParseUsDateTime(dropoffStr, out var dropoffLocal))
                return null;

            var pickupUtc = TimeZoneInfo.ConvertTimeToUtc(pickupLocal, _estZone);
            var dropoffUtc = TimeZoneInfo.ConvertTimeToUtc(dropoffLocal, _estZone);

            if (dropoffUtc < pickupUtc)
                return null;

            var storeFlag = NormalizeStoreAndFwdFlag(Get("store_and_fwd_flag"));

            return new TaxiTrip
            {
                PickupDatetime = pickupUtc,
                DropoffDatetime = dropoffUtc,
                PassengerCount = ParseInt(Get("passenger_count")),
                TripDistance = ParseDecimal(Get("trip_distance")),
                StoreAndFwdFlag = storeFlag,
                PULocationID = ParseInt(Get("PULocationID")),
                DOLocationID = ParseInt(Get("DOLocationID")),
                FareAmount = ParseDecimal(Get("fare_amount")),
                TipAmount = ParseDecimal(Get("tip_amount"))
            };
        }

        private Dictionary<string, int> BuildHeaderMap(string[] headers)
        {
            var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < headers.Length; i++)
            {
                var h = headers[i].Trim();
                map[h] = i;
            }

            return map;
        }

        private bool TryParseUsDateTime(string value, out DateTime dt)
        {
            var formats = new[]
            {
                "MM/dd/yyyy hh:mm:ss tt"
            };
            return DateTime.TryParseExact(value?.Trim(), "MM/dd/yyyy hh:mm:ss tt", CultureInfo.GetCultureInfo("en-US"), DateTimeStyles.None, out dt);
        }

        private int ParseInt(string s) => int.TryParse(s, out var v) ? v : 0;
        private decimal ParseDecimal(string s) => decimal.TryParse(s, CultureInfo.InvariantCulture, out var v) ? v : 0;

        private string NormalizeStoreAndFwdFlag(string s)
        {
            var t = (s ?? "").Trim();
            if (t.Equals("N", StringComparison.OrdinalIgnoreCase)) return "No";
            if (t.Equals("Y", StringComparison.OrdinalIgnoreCase)) return "Yes";
            return t;
        }
    }
}
