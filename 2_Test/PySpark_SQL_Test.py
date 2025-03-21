import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

# grok 3 conversation on how to set this up: https://grok.com/chat/385eed5a-a118-437b-9f88-31c23f79f6ab

"""
Explanation of Changes
SparkSession Configuration:
.master("local[*]"): Runs Spark locally using all available CPU cores.
spark.driver.host and spark.driver.bindAddress: Forces Spark to use localhost and 127.0.0.1, avoiding hostname issues.
HADOOP_HOME: Set as a system variable instead of in the script for reliability.
SQL Query: Uncommented and made generic (SELECT *). Replace it with your specific query based on your DataFrame’s columns.
"""

# Step 1: Initialize SparkSession with hostname fix
spark = SparkSession.builder \
    .appName("Pandas to PySpark ETL") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Step 2: Create a sample Pandas DataFrame
pandas_df = pd.read_excel('C:\\Users\\nickk\\PycharmProjects\\Data_Pipelines\\2_Test\\Data_Files\\RedFin\\test_redfin_sample03202025.xlsx')

# Step 3: Convert to PySpark DataFrame
spark_df = spark.createDataFrame(pandas_df)

# Step 4: Register as temporary table and run SQL query
spark_df.createOrReplaceTempView("my_table")
# Adjust the query based on your DataFrame's columns
result_df = spark.sql("SELECT * FROM my_table")  # Example: Replace with "SELECT col1, col2 FROM my_table WHERE col1 > 1" if applicable

result_df_2 = spark.sql("""
SELECT 
period_begin
period_end
region_type
region_type_id
region_name
region_id
duration
adjusted_average_new_listings
--adjusted_average_new_listings_yoy
average_pending_sales_listing_updates
-- average_pending_sales_listing_updates_yoy
off_market_in_two_weeks
-- off_market_in_two_weeks_yoy
adjusted_average_homes_sold
-- adjusted_average_homes_sold_yoy
median_new_listing_price
-- median_new_listing_price_yoy
median_sale_price
-- median_sale_price_yoy
median_days_to_close
-- median_days_to_close_yoy
median_new_listing_ppsf
-- median_new_listing_ppsf_yoy
active_listings
-- active_listings_yoy
median_days_on_market
-- median_days_on_market_yoy
percent_active_listings_with_price_drops
-- percent_active_listings_with_price_drops_yoy
age_of_inventory
-- age_of_inventory_yoy
months_of_supply
-- months_of_supply_yoy
median_pending_sqft
-- median_pending_sqft_yoy
average_sale_to_list_ratio
-- average_sale_to_list_ratio_yoy
median_sale_ppsf
-- median_sale_ppsf_yoy
 
FROM my_table
WHERE region_type = 'county'

""")

print(result_df_2.limit(5))

# Step 5: Convert back to Pandas DataFrame
# pandas_result_df = result_df.toPandas()

# Step 7: Clean up - Stop the SparkSession
spark.stop()