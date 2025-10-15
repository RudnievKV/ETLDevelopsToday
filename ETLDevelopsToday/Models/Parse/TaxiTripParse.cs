namespace ETLDevelopsToday.Models.Parse
{
    public class TaxiTripParse
    {
        public string? VendorID { get; set; }
        public string? TpepPickupDatetime { get; set; }
        public string? TpepDropoffDatetime { get; set; }
        public string? PassengerCount { get; set; }
        public string? TripDistance { get; set; }
        public string? RatecodeID { get; set; }
        public string? StoreAndFwdFlag { get; set; }
        public string? PULocationID { get; set; }
        public string? DOLocationID { get; set; }
        public string? PaymentType { get; set; }
        public string? FareAmount { get; set; }
        public string? Extra { get; set; }
        public string? MtaTax { get; set; }
        public string? TipAmount { get; set; }
        public string? TollsAmount { get; set; }
        public string? ImprovementSurcharge { get; set; }
        public string? TotalAmount { get; set; }
        public string? CongestionSurcharge { get; set; }
    }
}
