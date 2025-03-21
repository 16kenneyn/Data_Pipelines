-- ********** CREATE REALTOR MONTHLY INVENTORY & BASE DATA **********
-- DROP TABLE dbo.REALTOR_MNTHLY_HOT_ZIP;

CREATE TABLE dbo.REALTOR_MNTHLY_HOT_ZIP (

	month_date_yyyymm INT
	,zip_code VARCHAR(200)
	,zip_name VARCHAR(200)
	,household_count_rank INT
	,hotness_rank INT
	,hotness_score DECIMAL(12, 4)
	,supply_score DECIMAL(12, 4)
	,demand_score DECIMAL(12, 4)
	,median_days_on_market DECIMAL(12, 4)
	,page_view_count_per_property_mm DECIMAL(12, 4)
	,page_view_count_per_property_yy DECIMAL(12, 4)
	,page_view_count_per_property_vs_us DECIMAL(12, 4)
	,median_listing_price INT
	,quality_flag INT
	,hotness_rank_lm INT
	,hotness_rank_ly INT
	,median_days_on_market_ly DECIMAL(12, 4)
	,median_days_on_market_lm DECIMAL(12, 4)
	,median_days_on_market_us DECIMAL(12, 4)
	,median_listing_price_lm INT
	,median_listing_price_ly INT
	,median_listing_price_us INT
	,Update_Date DATETIME
);

--DELETE FROM [DataMining].[dbo].[REALTOR_MNTHLY_HOT_ZIP]
--WHERE month_date_yyyymm = 202502