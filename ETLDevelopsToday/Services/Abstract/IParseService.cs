namespace ETLDevelopsToday.Services.Abstract
{
    public interface IParseService
    {
        Task ProcessCsvAndIngest(
            string filePath,
            string? duplicatesOutputPath = null,
            CancellationToken cancellationToken = default);
    }
}
