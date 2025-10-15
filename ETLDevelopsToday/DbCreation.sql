IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TaxiData')
BEGIN
    CREATE DATABASE TaxiDataDB;
END
GO

USE TaxiDataDB;
GO

-- ============================================
-- Create Table
-- ============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TaxiTrips')
BEGIN
    CREATE TABLE TaxiTrips (
        Id BIGINT IDENTITY(1,1) PRIMARY KEY,
        TpepPickupDatetime datetimeoffset(4) NOT NULL,    
        TpepDropoffDatetime datetimeoffset(4) NOT NULL,     
        PassengerCount INT NOT NULL,                   
        TripDistance DECIMAL(10,2) NOT NULL,            
        StoreAndFwdFlag VARCHAR(10) NOT NULL,           
        PULocationID INT NOT NULL,                      
        DOLocationID INT NOT NULL,                      
        FareAmount DECIMAL(10,2) NOT NULL,             
        TipAmount DECIMAL(10,2) NOT NULL   
    );
END
GO

-- ============================================
-- Create Indexes
-- ============================================

CREATE NONCLUSTERED INDEX IX_TaxiTrips_PULocationID_TipAmount 
    ON TaxiTrips(PULocationID) 
    INCLUDE (tip_amount);
GO


CREATE NONCLUSTERED INDEX IX_TaxiTrips_TripDistance 
    ON TaxiTrips(trip_distance DESC);
GO


CREATE NONCLUSTERED INDEX IX_TaxiTrips_Duration 
    ON TaxiTrips(tpep_pickup_datetime, tpep_dropoff_datetime);
GO


CREATE NONCLUSTERED INDEX IX_TaxiTrips_PULocationID 
    ON TaxiTrips(PULocationID);
GO