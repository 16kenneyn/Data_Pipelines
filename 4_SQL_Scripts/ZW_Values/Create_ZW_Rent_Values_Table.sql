DROP TABLE dbo.ZW_RENT_VALUES; 

CREATE TABLE dbo.ZW_RENT_VALUES (
    RegionID INT,
    SizeRank INT,
    Month_End_Date DATE,
    Value INT,
    Data_Type NVARCHAR(255),
    Date_Pulled DATE
);