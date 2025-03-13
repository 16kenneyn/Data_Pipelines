
DROP TABLE dbo.LKP_ZW_DATA_TYPE

CREATE TABLE dbo.LKP_ZW_DATA_TYPE (
	LOOKUP_KEY INT PRIMARY KEY,
	DATA_TYPE NVARCHAR(255) UNIQUE,
	DATASET_TYPE VARCHAR(255)
);

INSERT INTO dbo.LKP_ZW_DATA_TYPE (LOOKUP_KEY, DATA_TYPE, DATASET_TYPE) VALUES
(1, 'Condo/Co-op Time Series', 'Home_Values')
,(2, '4-Bedroom Time Series', 'Home_Values')
,(3, '2-Bedroom Time Series', 'Home_Values')
,(4, '5+ Bedroom Time Series', 'Home_Values')
,(5, '3-Bedroom Time Series', 'Home_Values')
,(6, 'All Homes (SFR, Condo/Co-op) Time Series, Smoothed, Seasonally Adjusted', 'Home_Values')
,(7, 'Single-Family Homes Time Series', 'Home_Values')
,(8, '1-Bedroom Time Series', 'Home_Values')
,(9, 'Smoothed All Homes Plus Multifamily Time Series', 'Rent_Values')
,(10, 'Smoothed (Seasonally Adjusted) All Homes Plus Multifamily Time Series', 'Rent_Values')


SELECT * FROM dbo.LKP_ZW_DATA_TYPE

SELECT * 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME = 'LKP_ZW_DATA_TYPE';