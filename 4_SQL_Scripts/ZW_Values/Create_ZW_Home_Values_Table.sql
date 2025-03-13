DROP TABLE dbo.ZW_HOME_VALUES; 

CREATE TABLE dbo.ZW_HOME_VALUES (
    RegionID INT,
    SizeRank INT,
    Month_End_Date DATE,
    Value INT,
    Data_Type NVARCHAR(255),
    Date_Pulled DATE
);

SELECT COUNT(*) FROM dbo.ZW_HOME_VALUES

--INSERT INTO dbo.ZW_Home_Values (RegionID, SizeRank, RegionName, RegionType, StateName, State, City, Metro, CountyName, Month_End_Date, Value, Data_Type, Date_Pulled) VALUES 
--(58710, 248, '2301', 'zip', 'MA', 'MA', 'Brockton', 'Boston-Cambridge-Newton, MA-NH', 'Plymouth County', '2000-01-31', 126909.15, 'All Homes (SFR, Condo/Co-op) Time Series, Smoothed, Seasonally Adjusted', '2024-10-06'),
--(58660, 314, '2148', 'zip', 'MA', 'MA', 'Malden', 'Boston-Cambridge-Newton, MA-NH', 'Middlesex County', '2000-01-31', 166669.02, 'All Homes (SFR, Condo/Co-op) Time Series, Smoothed, Seasonally Adjusted', '2024-10-06'),
--(58666, 429, '2155', 'zip', 'MA', 'MA', 'Medford', 'Boston-Cambridge-Newton, MA-NH', 'Middlesex County', '2000-01-31', 211805.48, 'All Homes (SFR, Condo/Co-op) Time Series, Smoothed, Seasonally Adjusted', '2024-10-06');
