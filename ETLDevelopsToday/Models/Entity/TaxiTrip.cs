namespace ETLDevelopsToday.Models.Entity
{
    public class TaxiTrip
    {
        public DateTimeOffset PickupDatetime { get; set; }
        public DateTimeOffset DropoffDatetime { get; set; }
        public int PassengerCount { get; set; }
        public decimal TripDistance { get; set; }
        public string StoreAndFwdFlag { get; set; } = null!;
        public int PULocationID { get; set; }
        public int DOLocationID { get; set; }
        public decimal FareAmount { get; set; }
        public decimal TipAmount { get; set; }


        public string GetDeduplicationKey()
        {
            return $"{PickupDatetime:yyyy-MM-dd HH:mm:ss}|{DropoffDatetime:yyyy-MM-dd HH:mm:ss}|{PassengerCount}";
        }
    }
}
