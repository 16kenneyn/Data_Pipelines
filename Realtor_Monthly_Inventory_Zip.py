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
state_current_month_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_State.csv'
state_history_month_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_State_History.csv'
metro_current_month_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Metro.csv'
metro_historical_month_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Metro_History.csv'
county_current_month_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_County.csv'
county_historical_month_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_County_History.csv'
zip_current_month_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Zip.csv'
zip_historical_data_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Zip_History.csv'

""" ****** INITIALIZE GLOBAL VARIABLES ****** """
logger = DataPipelineLogger("Realtor_Monthly_Zip")

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
    df = dataframe

    # Filter out row with quality_flag = 1:  year-over-year figures may be impacted in month_date_yyyymm column
    df = df[df['month_date_yyyymm'] != 'quality_flag = 1:  year-over-year figures may be impacted']

    #  *********** LAST YEAR COLUMNS ***********
    df['median_listing_price_ly'] = df['median_listing_price'] / (1 + df['median_listing_price_yy'])
    df['active_listing_count_ly'] = df['active_listing_count'] / (1 + df['active_listing_count_yy'])
    df['median_days_on_market_ly'] = df['median_days_on_market'] / (1 + df['median_days_on_market_yy'])
    df['new_listing_count_ly'] = df['new_listing_count'] / (1 + df['new_listing_count_yy'])
    df['price_increased_count_ly'] = df['price_increased_count'] / (1 + df['price_increased_count_yy'])
    df['price_reduced_count_ly'] = df['price_reduced_count'] / (1 + df['price_reduced_count_yy'])
    df['pending_listing_count_ly'] = df['pending_listing_count'] / (1 + df['pending_listing_count_yy'])
    df['median_listing_price_per_square_foot_ly'] = df['median_listing_price_per_square_foot'] / (
                1 + df['median_listing_price_per_square_foot_yy'])
    df['median_square_feet_ly'] = df['median_square_feet'] / (1 + df['median_square_feet_yy'])
    df['average_listing_price_ly'] = df['average_listing_price'] / (1 + df['average_listing_price_yy'])
    df['total_listing_count_ly'] = df['total_listing_count'] / (1 + df['total_listing_count_yy'])
    df['pending_ratio_ly'] = df['pending_ratio'] / (1 + df['pending_ratio_yy'])

    #  *********** LAST MONTH COLUMNS ***********
    df['median_listing_price_lm'] = df['median_listing_price'] / (1 + df['median_listing_price_mm'])
    df['active_listing_count_lm'] = df['active_listing_count'] / (1 + df['active_listing_count_mm'])
    df['median_days_on_market_lm'] = df['median_days_on_market'] / (1 + df['median_days_on_market_mm'])
    df['new_listing_count_lm'] = df['new_listing_count'] / (1 + df['new_listing_count_mm'])
    df['price_increased_count_lm'] = df['price_increased_count'] / (1 + df['price_increased_count_mm'])
    df['price_reduced_count_lm'] = df['price_reduced_count'] / (1 + df['price_reduced_count_mm'])
    df['pending_listing_count_lm'] = df['pending_listing_count'] / (1 + df['pending_listing_count_mm'])
    df['median_listing_price_per_square_foot_lm'] = df['median_listing_price_per_square_foot'] / (
                1 + df['median_listing_price_per_square_foot_mm'])
    df['median_square_feet_lm'] = df['median_square_feet'] / (1 + df['median_square_feet_mm'])
    df['average_listing_price_lm'] = df['average_listing_price'] / (1 + df['average_listing_price_mm'])
    df['total_listing_count_lm'] = df['total_listing_count'] / (1 + df['total_listing_count_mm'])
    df['pending_ratio_lm'] = df['pending_ratio'] / (1 + df['pending_ratio_mm'])

    # Drop columns that are not needed
    df.drop(columns=['median_listing_price_yy', 'active_listing_count_yy', 'median_days_on_market_yy',
                     'new_listing_count_yy', 'price_increased_count_yy', 'price_reduced_count_yy',
                     'pending_listing_count_yy', 'median_listing_price_per_square_foot_yy', 'median_square_feet_yy',
                     'average_listing_price_yy', 'total_listing_count_yy', 'pending_ratio_yy',
                     'median_listing_price_mm', 'active_listing_count_mm', 'median_days_on_market_mm',
                     'new_listing_count_mm', 'price_increased_count_mm', 'price_reduced_count_mm',
                     'pending_listing_count_mm', 'median_listing_price_per_square_foot_mm', 'median_square_feet_mm',
                     'average_listing_price_mm', 'total_listing_count_mm', 'pending_ratio_mm'], inplace=True)

    # Fill zipcodes with leading zeros
    df['zip_code'] = df['postal_code'].apply(lambda x: str(x).zfill(5))
    df.drop(columns=['postal_code'], inplace=True)
    df['state'] = df['zip_name'].apply(lambda x: str(x).split(", ")[-1])
    return df

def fill_na(dataframe):
    # Columns to fill null values with zero
    columns_to_fill = [
        'median_listing_price', 'active_listing_count', 'median_days_on_market', 'new_listing_count',
        'price_increased_count', 'price_reduced_count', 'pending_listing_count', 'median_listing_price_per_square_foot',
        'median_square_feet', 'average_listing_price', 'total_listing_count', 'pending_ratio', 'quality_flag',
        'median_listing_price_ly', 'active_listing_count_ly', 'median_days_on_market_ly', 'new_listing_count_ly',
        'price_increased_count_ly', 'price_reduced_count_ly', 'pending_listing_count_ly',
        'median_listing_price_per_square_foot_ly',
        'median_square_feet_ly', 'average_listing_price_ly', 'total_listing_count_ly', 'pending_ratio_ly',
        'median_listing_price_lm', 'active_listing_count_lm', 'median_days_on_market_lm', 'new_listing_count_lm',
        'price_increased_count_lm', 'price_reduced_count_lm', 'pending_listing_count_lm',
        'median_listing_price_per_square_foot_lm',
        'median_square_feet_lm', 'average_listing_price_lm', 'total_listing_count_lm', 'pending_ratio_lm'
    ]
    # Fill null values with zero
    dataframe[columns_to_fill] = dataframe[columns_to_fill].fillna(0)
    dataframe["zip_name"] = dataframe["zip_name"].fillna("")

    # Select numeric columns
    numeric_cols = dataframe.select_dtypes(include=[np.number])

    # Replace inf values with 0
    dataframe[numeric_cols.columns] = dataframe[numeric_cols.columns].replace([np.inf, -np.inf], 0)

    # Replace NaN values with 0 (in case you have any)
    dataframe.fillna(0, inplace=True)

    return dataframe

def columns_to_round(dataframe):
    # Columns to round to 2 decimal places
    columns_to_round = [
        'median_listing_price_ly', 'active_listing_count_ly', 'median_days_on_market_ly', 'new_listing_count_ly',
        'price_increased_count_ly', 'price_reduced_count_ly', 'pending_listing_count_ly',
        'median_listing_price_per_square_foot_ly',
        'median_square_feet_ly', 'average_listing_price_ly', 'total_listing_count_ly', 'pending_ratio_ly',
        'median_listing_price_lm', 'active_listing_count_lm', 'median_days_on_market_lm', 'new_listing_count_lm',
        'price_increased_count_lm', 'price_reduced_count_lm', 'pending_listing_count_lm',
        'median_listing_price_per_square_foot_lm',
        'median_square_feet_lm', 'average_listing_price_lm', 'total_listing_count_lm'
    ]
    # Round to 2 decimal places
    dataframe[columns_to_round] = dataframe[columns_to_round].round(0)
    # dataframe[columns_to_round] = dataframe[columns_to_round].astype(int)
    return dataframe

def main_run():
    conn = sqlalchemy_engine()  # Connecting to SQL Server
    sql_table_name = 'REALTOR_MNTHLY_ZIP'
    sql_table_max_date = get_latest_yearmonth(sql_table_name)
    if sql_table_max_date: # Determining if max date is not None, if it is, the else statement will kick off the history pull.
        redfin_df = get_url(zip_current_month_url)
        redfin_df_max_date = max(redfin_df['month_date_yyyymm']) # Extract max date from current extract.
        if redfin_df_max_date > sql_table_max_date: # Determining if current extract is newer data than we already have in SQL table.

            # Base ETL Transformations
            redfin_df_v1 = dataframe_etl(redfin_df)
            redfin_df_v2 = fill_na(redfin_df_v1)
            redfin_df_v3 = columns_to_round(redfin_df_v2)

            # Ensuring correct column orders
            sql_columns = [
                'month_date_yyyymm', 'zip_code', 'state', 'median_listing_price',
                'active_listing_count', 'median_days_on_market', 'new_listing_count', 'price_increased_count',
                'price_reduced_count', 'pending_listing_count', 'median_listing_price_per_square_foot',
                'median_square_feet', 'average_listing_price', 'total_listing_count', 'pending_ratio',
                'quality_flag', 'median_listing_price_ly', 'active_listing_count_ly', 'median_days_on_market_ly',
                'new_listing_count_ly', 'price_increased_count_ly', 'price_reduced_count_ly',
                'pending_listing_count_ly', 'median_listing_price_per_square_foot_ly', 'median_square_feet_ly',
                'average_listing_price_ly', 'total_listing_count_ly', 'pending_ratio_ly',
                'median_listing_price_lm', 'active_listing_count_lm', 'median_days_on_market_lm',
                'new_listing_count_lm', 'price_increased_count_lm', 'price_reduced_count_lm',
                'pending_listing_count_lm', 'median_listing_price_per_square_foot_lm', 'median_square_feet_lm',
                'average_listing_price_lm', 'total_listing_count_lm', 'pending_ratio_lm', 'Update_Date'
            ]

            # Align columns and types
            redfin_df_v3 = redfin_df_v3[sql_columns]
            redfin_df_v3['month_date_yyyymm'] = redfin_df_v3['month_date_yyyymm'].astype(int)
            redfin_df_v3['zip_code'] = redfin_df_v3['zip_code'].astype(int)
            redfin_df_v3['state'] = redfin_df_v3['state'].astype(str)
            redfin_df_v3['median_listing_price'] = redfin_df_v3['median_listing_price'].astype(int)
            # Add other type conversions as needed

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
        redfin_df = get_url(zip_historical_data_url) # Use history load url since no data was returned from sql tale.

        # Base ETL Transformations
        redfin_df_v1 = dataframe_etl(redfin_df)
        redfin_df_v2 = fill_na(redfin_df_v1)
        redfin_df_v3 = columns_to_round(redfin_df_v2)
        redfin_df_v3['Update_Date'] = dt.today().strftime("%Y-%m-%d %H:%M:%S")

        # Ensuring correct column orders
        sql_columns = [
            'month_date_yyyymm', 'zip_code', 'state', 'median_listing_price',
            'active_listing_count', 'median_days_on_market', 'new_listing_count', 'price_increased_count',
            'price_reduced_count', 'pending_listing_count', 'median_listing_price_per_square_foot',
            'median_square_feet', 'average_listing_price', 'total_listing_count', 'pending_ratio',
            'quality_flag', 'median_listing_price_ly', 'active_listing_count_ly', 'median_days_on_market_ly',
            'new_listing_count_ly', 'price_increased_count_ly', 'price_reduced_count_ly',
            'pending_listing_count_ly', 'median_listing_price_per_square_foot_ly', 'median_square_feet_ly',
            'average_listing_price_ly', 'total_listing_count_ly', 'pending_ratio_ly',
            'median_listing_price_lm', 'active_listing_count_lm', 'median_days_on_market_lm',
            'new_listing_count_lm', 'price_increased_count_lm', 'price_reduced_count_lm',
            'pending_listing_count_lm', 'median_listing_price_per_square_foot_lm', 'median_square_feet_lm',
            'average_listing_price_lm', 'total_listing_count_lm', 'pending_ratio_lm', 'Update_Date'
        ]

        # Align columns and types
        redfin_df_v3 = redfin_df_v3[sql_columns]
        redfin_df_v3['month_date_yyyymm'] = redfin_df_v3['month_date_yyyymm'].astype(int)
        redfin_df_v3['zip_code'] = redfin_df_v3['zip_code'].astype(str)
        redfin_df_v3['state'] = redfin_df_v3['state'].astype(str)
        redfin_df_v3['median_listing_price'] = redfin_df_v3['median_listing_price'].astype(int)
        # Add other type conversions as needed

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
    # redfin_df = get_url(zip_current_month_url)
    #
    # redfin_df_v1 = dataframe_etl(redfin_df)
    # redfin_df_v2 = fill_na(redfin_df_v1)
    # redfin_df_v3 = columns_to_round(redfin_df_v2)
    #
    # redfin_df_v3['Update_Date'] = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    #
    # # Ensuring correct column orders
    # sql_columns = [
    #     'month_date_yyyymm', 'zip_code', 'state', 'median_listing_price',
    #     'active_listing_count', 'median_days_on_market', 'new_listing_count', 'price_increased_count',
    #     'price_reduced_count', 'pending_listing_count', 'median_listing_price_per_square_foot',
    #     'median_square_feet', 'average_listing_price', 'total_listing_count', 'pending_ratio',
    #     'quality_flag', 'median_listing_price_ly', 'active_listing_count_ly', 'median_days_on_market_ly',
    #     'new_listing_count_ly', 'price_increased_count_ly', 'price_reduced_count_ly',
    #     'pending_listing_count_ly', 'median_listing_price_per_square_foot_ly', 'median_square_feet_ly',
    #     'average_listing_price_ly', 'total_listing_count_ly', 'pending_ratio_ly',
    #     'median_listing_price_lm', 'active_listing_count_lm', 'median_days_on_market_lm',
    #     'new_listing_count_lm', 'price_increased_count_lm', 'price_reduced_count_lm',
    #     'pending_listing_count_lm', 'median_listing_price_per_square_foot_lm', 'median_square_feet_lm',
    #     'average_listing_price_lm', 'total_listing_count_lm', 'pending_ratio_lm', 'Update_Date'
    # ]
    #
    # # Align columns and types
    # redfin_df_v3 = redfin_df_v3[sql_columns]
    #
    # # Adding the date time stamp
    #
    # redfin_df_v3.to_excel('C:/Users/nickk/PycharmProjects/Data_Pipelines/2_Test/Data_Files/Realtor_Inventory/zip_current_test.xlsx', index=False)
