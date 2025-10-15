using ETLDevelopsToday.Models.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLDevelopsToday.Models.Dto
{
    public class ParsedResult
    {
        public List<TaxiTrip> Records { get; set; } = new List<TaxiTrip>();
        public List<TaxiTrip> Duplicates { get; set; } = new List<TaxiTrip>();
    }
}
