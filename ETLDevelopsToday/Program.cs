using ETLDevelopsToday.Services;
using ETLDevelopsToday.Services.Abstract;
using ETLDevelopsToday.Utils.Settings;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using System.Diagnostics;

namespace ETLDevelopsToday
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var isDebug = false;
            var appsettings = "appsettings.json";
            IsDebugCheck(ref isDebug);

            if (isDebug)
            {
                appsettings = "appsettings.Development.json";
            }

            IConfiguration config = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile(appsettings, optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var logPath = config["LogFile"];
            if (string.IsNullOrWhiteSpace(logPath))
            {
                logPath = Path.Combine(AppContext.BaseDirectory, "Logs", "log-.txt");
            }

            // Configure Serilog
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .WriteTo.Console()
                .WriteTo.File(
                    logPath,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: 31,
                    fileSizeLimitBytes: 100_000_000,    // 100 MB
                    rollOnFileSizeLimit: true,
                    shared: true
                )
                .CreateLogger();



            var databaseSettings = config.GetSection("DatabaseSettings").Get<DatabaseSettings>();
            if (databaseSettings is null ||
                string.IsNullOrEmpty(databaseSettings.ConnectionString) ||
                string.IsNullOrEmpty(databaseSettings.DatabaseName) ||
                string.IsNullOrEmpty(databaseSettings.TableName))
            {
                Log.Error("Database settings are not properly configured in appsettings.json.");
                return;
            }



            var serviceCollection = new ServiceCollection()
                .AddSingleton(config)
                .AddSingleton<DatabaseSettings>(databaseSettings)
                .AddScoped<IDbService, DbService>()
                .AddScoped<IParseService, ParseService>()
                .AddLogging(builder =>
                    builder
                    .ClearProviders()
                    .AddSerilog(Log.Logger, dispose: true)
                    .SetMinimumLevel(LogLevel.Information));

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var scope = serviceProvider.CreateScope();
            var parseService = scope.ServiceProvider.GetRequiredService<IParseService>();
            var dbService = scope.ServiceProvider.GetRequiredService<IDbService>();

            string csvFilePath = args.Length > 0 ? args[0] : "SampleData\\sample-cab-data.csv";

            await parseService.ProcessCsvAndIngest(csvFilePath);
        }
        [Conditional("DEBUG")]
        private static void IsDebugCheck(ref bool isDebug)
        {
            isDebug = true;
        }
    }
}
