import pandas as pd
import duckdb
pd.set_option('display.max_columns', None) # Don't truncate columns for dataframes
from sqlalchemy import create_engine

# Step 1: Load the Excel file into a Pandas DataFrame
pandas_df = pd.read_excel('C:\\Users\\nickk\\PycharmProjects\\Data_Pipelines\\2_Test\\Data_Files\\RedFin\\test_redfin_sample03202025.xlsx', sheet_name='us_national_market_tracker_test')

# Step 2: Create a DuckDB connection (in-memory)
con = duckdb.connect()

# Step 3: Register the Pandas DataFrame as a DuckDB table
con.register('my_table', pandas_df)

# Step 4: Run SQL transformations with date casting
# Replace 'date_column' with your actual column name and 'MM/DD/YYYY' with your date format
query = """

    SELECT 
    CAST(period_begin AS DATE) AS period_begin,
    CAST(period_end AS DATE) AS period_end,
    COALESCE(period_duration, 0) AS period_duration,
    COALESCE(TRIM(region_type), 'NA') AS region_type,
    COALESCE(region_type_id, 0) AS region_type_id,
    COALESCE(table_id, 0) AS table_id,
    COALESCE(TRIM(is_seasonally_adjusted), 'NA') AS is_seasonally_adjusted,
    COALESCE(TRIM(region), 'NA') AS region,
    COALESCE(TRIM(city), 'NA') AS city,
    REPLACE(COALESCE(state, 'NA'), 'U.S.', 'US') AS state,
    COALESCE(TRIM(state_code), 'NA') AS state_code,
    COALESCE(TRIM(property_type), 'NA') AS property_type,
    COALESCE(ABS(CAST(property_type_id AS INT)), 0) AS property_type_id,
    COALESCE(median_sale_price, 0) AS median_sale_price, 
    COALESCE(median_list_price, 0) AS median_list_price, 
    COALESCE(median_ppsf, 0) AS median_ppsf, 
    COALESCE(median_list_ppsf, 0) AS median_list_ppsf, 
    COALESCE(homes_sold, 0) AS homes_sold, 
    COALESCE(pending_sales, 0) AS pending_sales, 
    COALESCE(new_listings, 0) AS new_listings, 
    COALESCE(inventory, 0) AS inventory, 
    COALESCE(months_of_supply, 0) AS months_of_supply, 
    COALESCE(median_dom, 0) AS median_dom, 
    COALESCE(avg_sale_to_list, 0) AS avg_sale_to_list, 
    COALESCE(sold_above_list, 0) AS sold_above_list, 
    COALESCE(price_drops, 0) AS price_drops, 
    COALESCE(off_market_in_two_weeks, 0) AS off_market_in_two_weeks, 
    
    COALESCE(median_sale_price_mom, 0) AS median_sale_price_mom,
    COALESCE(median_list_price_mom, 0) AS median_list_price_mom,
    COALESCE(median_ppsf_mom, 0) AS median_ppsf_mom,
    COALESCE(median_list_ppsf_mom, 0) AS median_list_ppsf_mom,
    COALESCE(homes_sold_mom, 0) AS homes_sold_mom,
    COALESCE(pending_sales_mom, 0) AS pending_sales_mom,
    COALESCE(new_listings_mom, 0) AS new_listings_mom,
    COALESCE(inventory_mom, 0) AS inventory_mom,
    COALESCE(months_of_supply_mom, 0) AS months_of_supply_mom,
    COALESCE(median_dom_mom, 0) AS median_dom_mom,
    COALESCE(avg_sale_to_list_mom, 0) AS avg_sale_to_list_mom,
    COALESCE(sold_above_list_mom, 0) AS sold_above_list_mom,
    COALESCE(price_drops_mom, 0) AS price_drops_mom,
    COALESCE(off_market_in_two_weeks_mom, 0) AS off_market_in_two_weeks_mom,
    
    COALESCE(median_sale_price_yoy, 0) AS median_sale_price_yoy,
    COALESCE(median_list_price_yoy, 0) AS median_list_price_yoy,
    COALESCE(median_ppsf_yoy, 0) AS median_ppsf_yoy,
    COALESCE(median_list_ppsf_yoy, 0) AS median_list_ppsf_yoy,
    COALESCE(homes_sold_yoy, 0) AS homes_sold_yoy,
    COALESCE(pending_sales_yoy, 0) AS pending_sales_yoy,
    COALESCE(new_listings_yoy, 0) AS new_listings_yoy,
    COALESCE(inventory_yoy, 0) AS inventory_yoy,
    COALESCE(months_of_supply_yoy, 0) AS months_of_supply_yoy,
    COALESCE(median_dom_yoy, 0) AS median_dom_yoy,
    COALESCE(avg_sale_to_list_yoy, 0) AS avg_sale_to_list_yoy,
    COALESCE(sold_above_list_yoy, 0) AS sold_above_list_yoy,
    COALESCE(price_drops_yoy, 0) AS price_drops_yoy,
    COALESCE(off_market_in_two_weeks_yoy, 0) AS off_market_in_two_weeks_yoy,
    
    COALESCE(CAST(parent_metro_region AS TEXT), 'NA') AS parent_metro_region,
    COALESCE(CAST(parent_metro_region_metro_code AS TEXT), 'NA') AS parent_metro_region_metro_code,
    CAST(last_updated AS DATETIME) AS last_updated
    
    FROM pandas_df
    WHERE period_begin >= '2023-01-01'


"""
result_df = con.execute(query).fetchdf()

query_v2 = """
    SELECT 
    period_begin,
    period_end,
    period_duration,
    region_type,
    region_type_id,
    table_id,
    is_seasonally_adjusted,
    region,
    city,
    state,
    state_code,
    property_type,
    property_type_id,
    median_sale_price,
    median_list_price,
    median_ppsf,
    median_list_ppsf,
    homes_sold,
    pending_sales,
    new_listings,
    inventory,
    months_of_supply,
    median_dom,
    avg_sale_to_list,
    sold_above_list,
    price_drops,
    off_market_in_two_weeks,
    
    median_sale_price / (1 + median_sale_price_mom) AS median_sale_price_lm,
    median_list_price / (1 + median_list_price_mom) AS median_list_price_lm,
    median_ppsf / (1 + median_ppsf_mom) AS median_ppsf_lm,
    median_list_ppsf / (1 + median_list_ppsf_mom) AS median_list_ppsf_lm,
    homes_sold / (1 + homes_sold_mom) AS homes_sold_lm,
    pending_sales / (1 + pending_sales_mom) AS pending_sales_lm,
    new_listings / (1 + new_listings_mom) AS new_listings_lm,
    inventory / (1 + inventory_mom) AS inventory_lm,
    months_of_supply - months_of_supply_mom AS months_of_supply_lm,
    median_dom + median_dom_mom AS median_dom_lm,
    avg_sale_to_list - avg_sale_to_list_mom AS avg_sale_to_list_lm,
    sold_above_list - sold_above_list_mom AS sold_above_list_lm,
    price_drops - price_drops_mom AS price_drops_lm,
    off_market_in_two_weeks - off_market_in_two_weeks_mom AS off_market_in_two_weeks_lm,
    
    
    median_sale_price / (1 + median_sale_price_yoy) AS median_sale_price_ly,
    median_list_price / (1 + median_list_price_yoy) AS median_list_price_ly,
    median_ppsf / (1 + median_ppsf_yoy) AS median_ppsf_ly,
    median_list_ppsf / (1 + median_list_ppsf_yoy) AS median_list_ppsf_ly,
    homes_sold / (1 + homes_sold_yoy) AS homes_sold_ly,
    pending_sales / (1 + pending_sales_yoy) AS pending_sales_ly,
    new_listings / (1 + new_listings_yoy) AS new_listings_ly,
    inventory / (1 + inventory_yoy) AS inventory_ly,
    months_of_supply - months_of_supply_yoy AS months_of_supply_ly,
    median_dom + median_dom_yoy AS median_dom_ly,
    avg_sale_to_list - avg_sale_to_list_yoy AS avg_sale_to_list_ly,
    sold_above_list - sold_above_list_yoy AS sold_above_list_ly,
    price_drops - price_drops_yoy AS price_drops_ly,
    off_market_in_two_weeks - off_market_in_two_weeks_yoy AS off_market_in_two_weeks_ly,
    
    parent_metro_region,
    parent_metro_region_metro_code,
    last_updated
    
    FROM result_df
"""
result_df_V2 = con.execute(query_v2).fetchdf()

query_v3 = """
SELECT 
    period_begin,
    period_end,
    period_duration,
    region_type,
    region_type_id,
    table_id,
    is_seasonally_adjusted,
    region,
    city,
    state,
    state_code,
    property_type,
    property_type_id,
    median_sale_price,
    median_list_price,
    median_ppsf,
    median_list_ppsf,
    homes_sold,
    pending_sales,
    new_listings,
    inventory,
    months_of_supply,
    median_dom,
    avg_sale_to_list,
    sold_above_list * homes_sold AS sold_above_list,
    price_drops * homes_sold AS sold_price_drops,
    off_market_in_two_weeks * homes_sold AS sold_off_market_in_two_weeks,
    CASE WHEN median_ppsf = 0 THEN 0 ELSE median_sale_price / median_ppsf END AS median_sqft_off_sale_price,
    CASE WHEN median_list_ppsf = 0 THEN 0 ELSE median_list_price / median_list_ppsf END AS median_sqft_off_list_price,
    
    median_sale_price_lm,
    median_list_price_lm,
    median_ppsf_lm,
    median_list_ppsf_lm,
    homes_sold_lm,
    pending_sales_lm,
    new_listings_lm,
    inventory_lm,
    months_of_supply_lm,
    median_dom_lm,
    avg_sale_to_list_lm,
    sold_above_list_lm * homes_sold_lm AS sold_above_list_lm,
    price_drops_lm * homes_sold_lm AS sold_price_drops_lm,
    off_market_in_two_weeks_lm * homes_sold_lm AS sold_off_market_in_two_weeks_lm,
    
    CASE WHEN median_ppsf_lm = 0 THEN 0 ELSE median_sale_price_lm / median_ppsf_lm END AS median_sqft_off_sale_price_lm,
    CASE WHEN median_list_ppsf_lm = 0 THEN 0 ELSE median_list_price_lm / median_list_ppsf_lm END AS median_sqft_off_list_price_lm,
    
    
    median_sale_price_ly,
    median_list_price_ly,
    median_ppsf_ly,
    median_list_ppsf_ly,
    homes_sold_ly,
    pending_sales_ly,
    new_listings_ly,
    inventory_ly,
    months_of_supply_ly,
    median_dom_ly,
    avg_sale_to_list_ly, -- NEEDS TO ALWAYS STAY AS A DECIMAL, REST SHOULD BE FLOATS
    sold_above_list_ly * homes_sold_ly AS sold_above_list_ly,
    price_drops_ly * homes_sold_ly AS sold_price_drops_ly,
    off_market_in_two_weeks_ly * homes_sold_ly AS sold_off_market_in_two_weeks_ly,

    CASE WHEN median_ppsf_ly = 0 THEN 0 ELSE median_sale_price_ly / median_ppsf_ly END AS median_sqft_off_sale_price_ly,
    CASE WHEN median_list_ppsf_ly = 0 THEN 0 ELSE median_list_price_ly / median_list_ppsf_ly END AS median_sqft_off_list_price_ly,
    
    parent_metro_region,
    parent_metro_region_metro_code,
    last_updated,
    strftime(date_trunc('minute', CURRENT_TIMESTAMP), '%Y-%m-%d %H:%M') AS nk_update_date 
    
    FROM result_df_V2
"""

result_df_V3 = con.execute(query_v3).fetchdf()

print(result_df_V3.head())

result_df_V3.to_excel('C:\\Users\\nickk\\PycharmProjects\\Data_Pipelines\\2_Test\\Data_Files\\RedFin\\test_redfin_sample03202025_duckdb.xlsx', index=False)


# Step 6: Close the DuckDB connection
con.close()