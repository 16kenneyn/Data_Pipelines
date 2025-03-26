-- ********** CREATE REDFIN MONTHLY HOUSING DATA **********
-- DROP TABLE dbo.REDFIN_MNTHLY_HOUSING_NGHBORHD;

--state
--metro
--county
--city
--zip
--neighborhood

CREATE TABLE REDFIN_MNTHLY_HOUSING_NGHBORHD (
    period_begin DATE,
    period_end DATE,
    period_duration INT,
    region_type VARCHAR(50),
    region_type_id INT,
    table_id INT,
    is_seasonally_adjusted VARCHAR(1), -- 't' or 'f'
    region VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    state_code VARCHAR(100),
    property_type VARCHAR(100),
    property_type_id INT,
    median_sale_price INT,
    median_list_price INT,
    median_ppsf INT,
    median_list_ppsf INT,
    homes_sold INT,
    pending_sales INT,
    new_listings INT,
    inventory INT,
    months_of_supply FLOAT,
    median_dom INT,
    avg_sale_to_list FLOAT,
    sold_above_list INT,
    sold_price_drops INT,
    sold_off_market_in_two_weeks INT,
    median_sqft_off_sale_price INT,
    median_sqft_off_list_price INT,
    median_sale_price_lm INT,
    median_list_price_lm INT,
    median_ppsf_lm INT,
    median_list_ppsf_lm INT,
    homes_sold_lm INT,
    pending_sales_lm INT,
    new_listings_lm INT,
    inventory_lm INT,
    months_of_supply_lm FLOAT,
    median_dom_lm INT,
    avg_sale_to_list_lm FLOAT,
    sold_above_list_lm INT,
    sold_price_drops_lm INT,
    sold_off_market_in_two_weeks_lm INT,
    median_sqft_off_sale_price_lm INT,
    median_sqft_off_list_price_lm INT,
    median_sale_price_ly INT,
    median_list_price_ly INT,
    median_ppsf_ly INT,
    median_list_ppsf_ly INT,
    homes_sold_ly INT,
    pending_sales_ly INT,
    new_listings_ly INT,
    inventory_ly INT,
    months_of_supply_ly FLOAT,
    median_dom_ly INT,
    avg_sale_to_list_ly FLOAT,
    sold_above_list_ly INT,
    sold_price_drops_ly INT,
    sold_off_market_in_two_weeks_ly INT,
    median_sqft_off_sale_price_ly INT,
    median_sqft_off_list_price_ly INT,
    parent_metro_region VARCHAR(100),
    parent_metro_region_metro_code VARCHAR(100),
    last_updated DATETIME,
    nk_update_date VARCHAR(16) -- Format: YYYY-MM-DD HH:MM
);

--DELETE FROM [DataMining].[dbo].[REDFIN_MNTHLY_HOUSING_NGHBORHD]
--WHERE period_begin = '2025-02-01'