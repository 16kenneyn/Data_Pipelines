import pandas as pd
from datetime import datetime as dt
import requests
from io import StringIO
# from pandas.conftest import index
import traceback  # Need this to return complete error message for logging except section
import time
from _utils import DataPipelineLogger
import numpy as np
from _utils import sqlalchemy_engine  # engine used to connect to SQL Server, to be used in pandas read_sql_query & to_sql
pd.set_option('display.max_columns', None) # Don't truncate columns for dataframes
from sqlalchemy import text # Need this for get_latest_yearmonth

# Links to data extracts
metro_history_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Hotness/RDC_Inventory_Hotness_Metrics_Metro_History.csv'
county_history_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Hotness/RDC_Inventory_Hotness_Metrics_County_History.csv'
zip_history_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Hotness/RDC_Inventory_Hotness_Metrics_Zip_History.csv'

""" ****** INITIALIZE GLOBAL VARIABLES ****** """
logger = DataPipelineLogger("Realtor_Monthly_Hotness_Zip")

def get_url(url: str):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP request errors
        # Read the content of the response into a pandas DataFrame
        data = pd.read_csv(StringIO(response.text), encoding='utf-8-sig')  # encoding='utf-8-sig' is used to handle csv format declaration in first column name. Ex. Ã¯Â»Â¿month_date_yyyymm
        data.rename(columns=lambda x: x.strip().replace("ï»¿", ""), inplace=True)
        logger.write_to_log(f"Successfully requested and received data from: {url}")
        return data
    except requests.exceptions.RequestException as e:
        error_trace = traceback.format_exc()
        logger.write_to_log(f"An error occurred during the request realtor monthly inventory state file.: {e}\n{error_trace}")
        print(f"An error occurred: {e}")
        return None

def get_latest_yearmonth(table_name:str):
    # Define the SQL query using text()
    get_max_date = text(f"""
    SELECT MAX(month_date_yyyymm) FROM [DataMining].[dbo].[{table_name}]
    """)

    # Use a context manager to handle the connection
    with sqlalchemy_engine().connect() as connection:
        result = connection.execute(get_max_date)
        max_date = result.scalar()  # Fetch the single value
        if max_date: # Error handling to determine if None is returned or a string
            max_date = int(max_date)
        else:
            print(max_date)
        connection.close() # Close the connection after execution to avoid timeout issues.
    logger.write_to_log(f"Get Latest Yearmonth called, results for max date is {max_date} for table {table_name}")
    print(f"Get Latest Yearmonth called, results for max date is {max_date} for table {table_name}")

    return max_date

def dataframe_etl(dataframe):
    """
            This converts LM, LY, & vs US metrics that are the % chg or net chg into the actual LM, LY, & vs US metrics.
            It then rounds these columns to either 2 or 4 past the decimal place to save space in sql.
            Finally it then renames a few of the columns to be more descriptive.
            :param input_dataframe:
            :return: df
            """

    df = dataframe

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

def fill_na(dataframe):
    # Columns to fill null values with zero
    columns_to_fill = [
        'month_date_yyyymm',
        'household_count_rank',
        'hotness_rank',
        'hotness_score',
        'supply_score',
        'demand_score',
        'median_days_on_market',
        'page_view_count_per_property_mm',
        'page_view_count_per_property_yy',
        'page_view_count_per_property_vs_us',
        'median_listing_price',
        'quality_flag',
        'hotness_rank_lm',
        'hotness_rank_ly',
        'median_days_on_market_ly',
        'median_days_on_market_lm',
        'median_days_on_market_us',
        'median_listing_price_lm',
        'median_listing_price_ly',
        'median_listing_price_us'
    ]

    # Fill null values with zero
    dataframe[columns_to_fill] = dataframe[columns_to_fill].fillna(0)
    dataframe["zip_code"] = dataframe["zip_code"].fillna("")
    dataframe["zip_name"] = dataframe["zip_name"].fillna("")

    # Select numeric columns
    numeric_cols = dataframe.select_dtypes(include=[np.number])

    # Replace inf values with 0
    dataframe[numeric_cols.columns] = dataframe[numeric_cols.columns].replace([np.inf, -np.inf], 0)

    # Replace NaN values with 0 (in case you have any)
    dataframe.fillna(0, inplace=True)

    return dataframe

def chg_data_type(dataframe):
    # Ensuring correct column orders
    temp_df = dataframe

    sql_columns = [
        'month_date_yyyymm',
        'zip_code',
        'zip_name',
        'household_count_rank',
        'hotness_rank',
        'hotness_score',
        'supply_score',
        'demand_score',
        'median_days_on_market',
        'page_view_count_per_property_mm',
        'page_view_count_per_property_yy',
        'page_view_count_per_property_vs_us',
        'median_listing_price',
        'quality_flag',
        'hotness_rank_lm',
        'hotness_rank_ly',
        'median_days_on_market_ly',
        'median_days_on_market_lm',
        'median_days_on_market_us',
        'median_listing_price_lm',
        'median_listing_price_ly',
        'median_listing_price_us',
        'Update_Date'
    ]

    # Align columns and types
    temp_df = temp_df[sql_columns]

    # Set data types for each column
    temp_df['month_date_yyyymm'] = temp_df['month_date_yyyymm'].astype(int)
    temp_df['zip_code'] = temp_df['zip_code'].astype(str)
    temp_df['zip_name'] = temp_df['zip_name'].astype(str)
    temp_df['household_count_rank'] = temp_df['household_count_rank'].astype(int)
    temp_df['hotness_rank'] = temp_df['hotness_rank'].astype(int)
    temp_df['hotness_score'] = temp_df['hotness_score'].astype(float)
    temp_df['supply_score'] = temp_df['supply_score'].astype(float)
    temp_df['demand_score'] = temp_df['demand_score'].astype(float)
    temp_df['median_days_on_market'] = temp_df['median_days_on_market'].astype(float)
    temp_df['page_view_count_per_property_mm'] = temp_df['page_view_count_per_property_mm'].astype(float)
    temp_df['page_view_count_per_property_yy'] = temp_df['page_view_count_per_property_yy'].astype(float)
    temp_df['page_view_count_per_property_vs_us'] = temp_df['page_view_count_per_property_vs_us'].astype(float)
    temp_df['median_listing_price'] = temp_df['median_listing_price'].astype(int)
    temp_df['quality_flag'] = temp_df['quality_flag'].astype(int)
    temp_df['hotness_rank_lm'] = temp_df['hotness_rank_lm'].astype(int)
    temp_df['hotness_rank_ly'] = temp_df['hotness_rank_ly'].astype(int)
    temp_df['median_days_on_market_ly'] = temp_df['median_days_on_market_ly'].astype(float)
    temp_df['median_days_on_market_lm'] = temp_df['median_days_on_market_lm'].astype(float)
    temp_df['median_days_on_market_us'] = temp_df['median_days_on_market_us'].astype(float)
    temp_df['median_listing_price_lm'] = temp_df['median_listing_price_lm'].astype(int)
    temp_df['median_listing_price_ly'] = temp_df['median_listing_price_ly'].astype(int)
    temp_df['median_listing_price_us'] = temp_df['median_listing_price_us'].astype(int)

    return temp_df

def main_run():
    conn = sqlalchemy_engine()  # Connecting to SQL Server
    sql_table_name = 'REALTOR_MNTHLY_HOT_ZIP'
    sql_table_max_date = get_latest_yearmonth(sql_table_name)
    if sql_table_max_date: # Determining if max date is not None, if it is, the else statement will kick off the history pull.
        redfin_df = get_url(zip_history_url)
        redfin_df_max_date = max(redfin_df['month_date_yyyymm']) # Extract max date from current extract.
        if redfin_df_max_date > sql_table_max_date: # Determining if current extract is newer data than we already have in SQL table.

            # Filtering DataFrame to latest year month to append to SQL table
            redfin_df_v0 = redfin_df[redfin_df['month_date_yyyymm'] == redfin_df_max_date]

            # Base ETL Transformations
            redfin_df_v1 = dataframe_etl(redfin_df_v0)
            redfin_df_v2 = fill_na(redfin_df_v1)
            redfin_df_v3 = chg_data_type(redfin_df_v2)

            logger.write_to_log(f"DataFrame head:\n{redfin_df_v3.head().to_string()}")

            # Writing data to SQL table as append
            print(f"Writing current month data to {sql_table_name}.")
            redfin_df_v3.to_sql(sql_table_name, con=conn, if_exists='append', index=False)

        else:
            print(f"Current data extract YearPeriod is already in SQL table {sql_table_name}.")
            print(f"SQL table max date = {str(sql_table_max_date)} ---- Current data extract max date = {str(redfin_df_max_date)}")
            logger.write_to_log(f"Current data extract YearPeriod is already in SQL table {sql_table_name}.")
            logger.write_to_log(f"SQL table max date = {str(sql_table_max_date)} ---- Current data extract max date = {str(redfin_df_max_date)}")

    else: # None was returned
        redfin_df = get_url(zip_history_url) # Use history load url since no data was returned from sql tale.

        # Base ETL Transformations
        redfin_df_v1 = dataframe_etl(redfin_df)
        redfin_df_v2 = fill_na(redfin_df_v1)
        redfin_df_v3 = chg_data_type(redfin_df_v2)

        logger.write_to_log(f"DataFrame head:\n{redfin_df_v3.head().to_string()}")

        print(f"Writing historical data to {sql_table_name}.")
        # Writing data to SQL table as replace
        logger.write_to_log(f"Writing historical data to {sql_table_name}.")
        with conn.connect() as connection:
            # Drop the table if it exists
            connection.execute(text(f"DROP TABLE IF EXISTS {sql_table_name}"))
            # Insert data into a new table
            redfin_df_v3.to_sql(sql_table_name, con=connection, if_exists='fail', index=False)
            connection.commit()  # Explicitly commit the transaction


        # redfin_df_v3.to_sql(sql_table_name, con=conn, if_exists='replace', index=False)


if __name__ == '__main__':

    logger.write_to_log('******************************************')
    start_time = time.time()

    try:
        main_run()
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.write_error_to_log(f"An error occurred: {e}\n{error_trace}")
        print(f"An error occurred: {e}")
    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes, seconds = divmod(elapsed_time, 60)
        logger.write_to_log(f"Main Function took {minutes:.0f} minutes and {seconds:.2f} seconds to run.")
        logger.write_to_log('******************************************')


    """" *********** ERROR HANDLING AND QA NON PRODUCTION *********** """
    # redfin_df_v1 = get_url(zip_history_url)
    # redfin_df_v0 = redfin_df_v1[redfin_df_v1['month_date_yyyymm'] == 202502]
    #
    # redfin_df_v2 = dataframe_etl(redfin_df_v0)
    # refin_df_v2_0 = fill_na(redfin_df_v2)
    # redfin_df_v3 = chg_data_type(refin_df_v2_0)
    #
    # # Adding the date time stamp
    #
    # redfin_df_v3.to_excel('C:/Users/nickk/PycharmProjects/Data_Pipelines/2_Test/Data_Files/Realtor_Hotness/zip_hotness_current_test.xlsx', index=False)
