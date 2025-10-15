using ETLDevelopsToday.Models.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLDevelopsToday.Services.Abstract
{
    public interface IDbService
    {
        Task EnsureDatabaseAndTable(CancellationToken cancellationToken = default);
        Task BulkInsert(IEnumerable<TaxiTrip> records, CancellationToken cancellationToken = default);
    }
}
