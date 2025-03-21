import pandas as pd
from datetime import datetime as dt
import requests
from io import StringIO
import gzip
# from pandas.conftest import index
import traceback  # Need this to return complete error message for logging except section
import time
from _utils import DataPipelineLogger
import numpy as np
from _utils import sqlalchemy_engine  # engine used to connect to SQL Server, to be used in pandas read_sql_query & to_sql
pd.set_option('display.max_columns', None) # Don't truncate columns for dataframes
from sqlalchemy import text # Need this for get_latest_yearmonth
from pyspark.sql import SparkSession # Needed to convert pyspark

# Links to data extracts

national_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/us_national_market_tracker.tsv000.gz'
metro_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/redfin_metro_market_tracker.tsv000.gz'
state_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/state_market_tracker.tsv000.gz'
county_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/county_market_tracker.tsv000.gz'
city_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz'
zip_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz'
neighborhood_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/neighborhood_market_tracker.tsv000.gz'


""" ****** INITIALIZE GLOBAL VARIABLES ****** """
logger = DataPipelineLogger("Redfin_Monthly_Housing_")

def get_url(url: str):
    try:
        # Fetch the data from the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad HTTP status codes

        # Decompress the gzipped content and decode to string
        content = gzip.decompress(response.content).decode('utf-8')
        lines = content.splitlines()

        # Find the header line by looking for known column names
        header_line = None
        for i, line in enumerate(lines):
            if 'period_begin' in line and 'period_end' in line:  # Common Redfin column names
                header_line = i
                break
        if header_line is None:
            raise ValueError("Header line not found in the file")

        # Read the data starting from the header line
        data = pd.read_csv(StringIO('\n'.join(lines[header_line:])), sep='\t')
        return data

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching the URL: {e}")
        return None
    except ValueError as e:
        print(f"Error processing the file: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def dataframe_etl(dataframe):




    # df = dataframe
    #
    # df['hotness_rank_lm'] = df['hotness_rank_lm'].astype(str).apply(lambda x: str(x).strip())
    #
    # period_begin
    # period_end
    # period_duration
    # region_type
    # region_type_id
    # table_id
    # is_seasonally_adjusted
    # region
    # city
    # state
    # state_code
    # property_type
    # property_type_id
    # median_sale_price
    # median_sale_price_mom
    # median_sale_price_yoy
    # median_list_price
    # median_list_price_mom
    # median_list_price_yoy
    # median_ppsf
    # median_ppsf_mom
    # median_ppsf_yoy
    # median_list_ppsf
    # median_list_ppsf_mom
    # median_list_ppsf_yoy
    # homes_sold
    # homes_sold_mom
    # homes_sold_yoy
    # pending_sales
    # pending_sales_mom
    # pending_sales_yoy
    # new_listings
    # new_listings_mom
    # new_listings_yoy
    # inventory
    # inventory_mom
    # inventory_yoy
    # months_of_supply
    # months_of_supply_mom
    # months_of_supply_yoy
    # median_dom
    # median_dom_mom
    # median_dom_yoy
    # avg_sale_to_list
    # avg_sale_to_list_mom
    # avg_sale_to_list_yoy
    # sold_above_list
    # sold_above_list_mom
    # sold_above_list_yoy
    # price_drops
    # price_drops_mom
    # price_drops_yoy
    # off_market_in_two_weeks
    # off_market_in_two_weeks_mom
    # off_market_in_two_weeks_yoy
    # parent_metro_region
    # parent_metro_region_metro_code
    # last_updated

    # Filter out row with quality_flag = 1:  year-over-year figures may be impacted in month_date_yyyymm column
    df = df[df['month_date_yyyymm'] != 'quality_flag = 1:  year-over-year figures may be impacted']

    # Converting change columns to LY, LM, US for aggregation in PBI
    df['hotness_rank_lm'] = df['hotness_rank'] + df['hotness_rank_mm']
    df['hotness_rank_ly'] = df['hotness_rank'] - df['hotness_rank_yy']
    df['median_days_on_market_ly'] = df['median_days_on_market'] - df['median_dom_yy_day']
    df['median_days_on_market_lm'] = df['median_days_on_market'] - df['median_dom_mm_day']
    df['median_days_on_market_us'] = df['median_days_on_market'] - df['median_dom_vs_us']

    # Median listing price vs lm and ly
    df['median_listing_price_lm'] = df['median_listing_price'] / (1 + df['median_listing_price_mm'])
    df['median_listing_price_ly'] = df['median_listing_price'] / (1 + df['median_listing_price_yy'])
    df['median_listing_price_us'] = df['median_listing_price'] / (1 + df['median_listing_price_vs_us'])

    # Round these columns to 2 decimal places
    df['hotness_rank'] = df['hotness_rank'].round(2)
    df['demand_score'] = df['demand_score'].round(2)
    df['hotness_rank_lm'] = df['hotness_rank_lm'].round(2)
    df['hotness_rank_ly'] = df['hotness_rank_ly'].round(2)
    df['median_listing_price_lm'] = df['median_listing_price_lm'].round(2)
    df['median_listing_price_ly'] = df['median_listing_price_ly'].round(2)
    df['median_listing_price_us'] = df['median_listing_price_us'].round(2)

    # Round these columns to 4 decimal places
    df['median_days_on_market_ly'] = df['median_days_on_market_ly'].round(4)
    df['median_days_on_market_lm'] = df['median_days_on_market_lm'].round(4)
    df['median_days_on_market_us'] = df['median_days_on_market_us'].round(4)
    df['hotness_score'] = df['hotness_score'].round(4)
    df['supply_score'] = df['supply_score'].round(4)
    df['demand_score'] = df['demand_score'].round(4)

    df['page_view_count_per_property_mm'] = df['page_view_count_per_property_mm'].round(4)
    df['page_view_count_per_property_yy'] = df['page_view_count_per_property_yy'].round(4)
    df['page_view_count_per_property_vs_us'] = df['page_view_count_per_property_vs_us'].round(4)

    # Drop unnecessary columns after the addition of new columns above
    df.drop(
        columns=['hotness_rank_mm', 'hotness_rank_yy', 'median_dom_yy_day', 'median_dom_mm_day', 'median_dom_vs_us',
                 'median_listing_price_mm', 'median_listing_price_yy', 'median_listing_price_vs_us',
                 'median_days_on_market_mm',
                 'median_days_on_market_yy'], inplace=True)

    # Rename columns
    df.rename(columns={'hh_rank': 'household_count_rank'}, inplace=True)
    df['zip_code'] = df['postal_code'].astype(str) # MAKING SURE ZIP_CODE IS STR BEFORE PADDING
    df['zip_code'] = df['zip_code'].apply(lambda x: str(x).zfill(5)) # LEFT PAD WITH 0S TO ENSURE ALL 5 CHARACTER LENGTH
    df.drop(columns=['postal_code'], inplace=True)

    # Adding update date
    df['Update_Date'] = dt.today().strftime("%Y-%m-%d %H:%M:%S")

    return df


# if __name__ == '__main__':

    # logger.write_to_log('******************************************')
    # start_time = time.time()
    #
    # try:
    #     main_run()
    # except Exception as e:
    #     error_trace = traceback.format_exc()
    #     logger.write_error_to_log(f"An error occurred: {e}\n{error_trace}")
    #     print(f"An error occurred: {e}")
    # finally:
    #     end_time = time.time()
    #     elapsed_time = end_time - start_time
    #     minutes, seconds = divmod(elapsed_time, 60)
    #     logger.write_to_log(f"Main Function took {minutes:.0f} minutes and {seconds:.2f} seconds to run.")
    #     logger.write_to_log('******************************************')




df = get_url(national_url)
if df is not None:
    print("Data loaded successfully!")
    print(df.head())
else:
    print("Failed to load the data.")

print(len(df))