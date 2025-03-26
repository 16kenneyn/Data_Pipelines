import pandas as pd
import requests
from io import StringIO
import gzip # Used to unzip Redfin data (come in the form of a zip file)
import traceback  # Need this to return complete error message for logging except section
import time
from _utils import DataPipelineLogger
from _utils import sqlalchemy_engine  # engine used to connect to SQL Server, to be used in pandas read_sql_query & to_sql
pd.set_option('display.max_columns', None) # Don't truncate columns for dataframes
from sqlalchemy import text # Need this for get_latest_start_date
import duckdb # Needed if you want to run sql on pandas dataframes

# URLs for Redfin data
national_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/us_national_market_tracker.tsv000.gz'
state_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/state_market_tracker.tsv000.gz'
metro_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/redfin_metro_market_tracker.tsv000.gz'
county_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/county_market_tracker.tsv000.gz'
city_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz'
zip_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz'
neighborhood_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/neighborhood_market_tracker.tsv000.gz'

""" ****** INITIALIZE GLOBAL VARIABLES ****** """
logger = DataPipelineLogger("Redfin_Monthly_Housing_State")
con = duckdb.connect()  # Create a DuckDB connection (in-memory)
universal_url_string = state_url
sql_table_name = 'REDFIN_MNTHLY_HOUSING_STATE'  # SQL table name

def get_url(url: str):
    """
    Fetches the data from the given URL, decompresses it, and returns a Pandas DataFrame.
    :param url:
    :return: Decompressed data as a Pandas DataFrame
    """
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
        logger.write_to_log(f"Data successfully fetched from {url}")
        return data
    except requests.exceptions.RequestException as e:
        error_trace = traceback.format_exc()
        logger.write_to_log(
            f"An error occurred during the request redfin monthly inventory for url {url}.: {e}\n{error_trace}")
        print(f"An error occurred: {e}")
        return None

    except ValueError as e:
        error_trace = traceback.format_exc()
        logger.write_to_log(
            f"An error occurred during the request redfin monthly inventory for url {url}.: {e}\n{error_trace}")
        print(f"Error processing the file: {e}")
        return None
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.write_to_log(
            f"An error occurred during the request redfin monthly inventory for url {url}.: {e}\n{error_trace}")
        print(f"Unexpected error: {e}")
        return None

def get_latest_start_date(table_name:str):
    # Define the SQL query using text()
    get_max_date = text(f"""
    SELECT MAX(period_begin) FROM [DataMining].[dbo].[{table_name}]
    """)

    # Use a context manager to handle the connection
    with sqlalchemy_engine().connect() as connection:
        result = connection.execute(get_max_date)
        max_date = result.scalar()  # Fetch the single value
        if max_date: # Error handling to determine if None is returned or a string
            print(f"Max date of table is {max_date} and the data type is: {type(max_date)}")
            logger.write_to_log(f"")
            max_date = max_date
        else:
            print(max_date)
        connection.close() # Close the connection after execution to avoid timeout issues.
    logger.write_to_log(f"get_latest_start_date, results for max date is {max_date} for table {table_name}")
    print(f"Get Latest start_date called, results for max date is {max_date} for table {table_name}")
    return max_date

def rdm_transformations(df: pd.DataFrame):
    con.register('rdm_table', df) # Register the Pandas DataFrame as a DuckDB table
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

    FROM rdm_table
    -- WHERE period_begin >= '2023-01-01'
    """
    try:
        result_df = con.execute(query).fetchdf()
        logger.write_to_log(f"Data successfully transformed in the RDM layer query.")
        return result_df
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.write_error_to_log(f"An error occurred during the RDM transformation: {e}\n{error_trace}")

def sdm_tranformations(df: pd.DataFrame):
    con.register('sdm_table', df) # Register the Pandas DataFrame as a DuckDB table
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
    FROM sdm_table
    """
    try:
        result_df_v2 = con.execute(query_v2).fetchdf()
        logger.write_to_log(f"Data successfully transformed in the SDM layer query.")
        return result_df_v2
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.write_error_to_log(f"An error occurred during the SDM transformation: {e}\n{error_trace}")

def cdm_transformation(df: pd.DataFrame):
    con.register('cdm_table', df) # Register the Pandas DataFrame as a DuckDB table
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
    ROUND(median_sale_price, 0) AS median_sale_price,
    ROUND(median_list_price, 0) AS median_list_price,
    ROUND(median_ppsf, 0) AS median_ppsf,
    ROUND(median_list_ppsf, 0) AS median_list_ppsf,
    ROUND(homes_sold, 0) AS homes_sold,
    ROUND(pending_sales, 0) AS pending_sales,
    ROUND(new_listings, 0) AS new_listings,
    ROUND(inventory, 0) AS inventory,
    ROUND(months_of_supply, 4) AS months_of_supply,
    ROUND(median_dom, 0) AS median_dom,
    ROUND(avg_sale_to_list, 4) AS avg_sale_to_list,
    ROUND(sold_above_list * homes_sold, 0) AS sold_above_list,
    ROUND(price_drops * homes_sold, 0) AS sold_price_drops,
    ROUND(off_market_in_two_weeks * homes_sold, 0) AS sold_off_market_in_two_weeks,
    CASE WHEN median_ppsf = 0 THEN 0 ELSE ROUND(median_sale_price / median_ppsf, 0) END AS median_sqft_off_sale_price, -- SQFT OFF SALE PRICE CALCULATION 
    CASE WHEN median_list_ppsf = 0 THEN 0 ELSE ROUND(median_list_price / median_list_ppsf, 0) END AS median_sqft_off_list_price, -- SQFT OFF LIST PRICE CALCULATION

    -- LM METRICS
    ROUND(median_sale_price_lm, 0) AS median_sale_price_lm,
    ROUND(median_list_price_lm, 0) AS median_list_price_lm,
    ROUND(median_ppsf_lm, 0) AS median_ppsf_lm,
    ROUND(median_list_ppsf_lm, 0) AS median_list_ppsf_lm,
    ROUND(homes_sold_lm, 0) AS homes_sold_lm,
    ROUND(pending_sales_lm, 0) AS pending_sales_lm,
    ROUND(new_listings_lm, 0) AS new_listings_lm,
    ROUND(inventory_lm, 0) AS inventory_lm,
    ROUND(months_of_supply_lm, 4) AS months_of_supply_lm,
    ROUND(median_dom_lm, 0) AS median_dom_lm,
    ROUND(avg_sale_to_list_lm, 4) AS avg_sale_to_list_lm,
    ROUND(sold_above_list_lm * homes_sold_lm, 0) AS sold_above_list_lm,
    ROUND(price_drops_lm * homes_sold_lm, 0) AS sold_price_drops_lm,
    ROUND(off_market_in_two_weeks_lm * homes_sold_lm, 0) AS sold_off_market_in_two_weeks_lm,
    CASE WHEN median_ppsf_lm = 0 THEN 0 ELSE ROUND(median_sale_price_lm / median_ppsf_lm, 0) END AS median_sqft_off_sale_price_lm, -- SQFT OFF SALE PRICE CALCULATION
    CASE WHEN median_list_ppsf_lm = 0 THEN 0 ELSE ROUND(median_list_price_lm / median_list_ppsf_lm, 0) END AS median_sqft_off_list_price_lm, -- SQFT OFF LIST PRICE CALCULATION

    -- LY METRICS
    ROUND(median_sale_price_ly, 0) AS median_sale_price_ly,
    ROUND(median_list_price_ly, 0) AS median_list_price_ly,
    ROUND(median_ppsf_ly, 0) AS median_ppsf_ly,
    ROUND(median_list_ppsf_ly, 0) AS median_list_ppsf_ly,
    ROUND(homes_sold_ly, 0) AS homes_sold_ly,
    ROUND(pending_sales_ly, 0) AS pending_sales_ly,
    ROUND(new_listings_ly, 0) AS new_listings_ly,
    ROUND(inventory_ly, 0) AS inventory_ly,
    ROUND(months_of_supply_ly, 4) AS months_of_supply_ly,
    ROUND(median_dom_ly, 0) AS median_dom_ly,
    ROUND(avg_sale_to_list_ly, 4) AS avg_sale_to_list_ly, -- NEEDS TO ALWAYS STAY AS A DECIMAL, REST SHOULD BE FLOATS
    ROUND(sold_above_list_ly * homes_sold_ly, 0) AS sold_above_list_ly,
    ROUND(price_drops_ly * homes_sold_ly, 0) AS sold_price_drops_ly,
    ROUND(off_market_in_two_weeks_ly * homes_sold_ly, 0) AS sold_off_market_in_two_weeks_ly,
    CASE WHEN median_ppsf_ly = 0 THEN 0 ELSE ROUND(median_sale_price_ly / median_ppsf_ly, 0) END AS median_sqft_off_sale_price_ly, -- SQFT OFF SALE PRICE CALCULATION
    CASE WHEN median_list_ppsf_ly = 0 THEN 0 ELSE ROUND(median_list_price_ly / median_list_ppsf_ly, 0) END AS median_sqft_off_list_price_ly, -- SQFT OFF LIST PRICE CALCULATION
    
    parent_metro_region,
    parent_metro_region_metro_code,
    last_updated,
    strftime(date_trunc('minute', CURRENT_TIMESTAMP), '%Y-%m-%d %H:%M') AS nk_update_date -- ADDING SQL UPDATE DATETIME
    FROM cdm_table
    """
    try:
        result_df_v3 = con.execute(query_v3).fetchdf()
        logger.write_to_log(f"Data successfully transformed in the CDM layer query.")
        return result_df_v3
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.write_error_to_log(f"An error occurred during the CDM transformation: {e}\n{error_trace}")

def main_run():

    conn = sqlalchemy_engine()  # Connecting to SQL Server
    sql_table_max_date = get_latest_start_date(sql_table_name)
    if sql_table_max_date: # Determining if max date is not None, if it is, the else statement will kick off the history pull.
        redfin_df = get_url(universal_url_string)
        redfin_df['period_begin'] = pd.to_datetime(redfin_df['period_begin'])
        redfin_df_max_date = max(redfin_df['period_begin']) # Extract max date from current extract.
        print(f"Max date of extract is {redfin_df_max_date} and the data type is: {type(redfin_df_max_date)}")
        if redfin_df_max_date.date() > sql_table_max_date: # Determining if current extract is newer data than we already have in SQL table.

            # Filtering DataFrame to latest start_date to append to SQL table
            redfin_df_v0 = redfin_df[redfin_df['period_begin'] == redfin_df_max_date]

            # Base ETL Transformations
            redfin_df_v1 = rdm_transformations(redfin_df_v0)
            redfin_df_v2 = sdm_tranformations(redfin_df_v1)
            redfin_df_v3 = cdm_transformation(redfin_df_v2)

            logger.write_to_log(f"DataFrame head:\n{redfin_df_v3.head().to_string()}")

            # Writing data to SQL table as append
            print(f"Writing current month data to {sql_table_name}.")
            logger.write_to_log(f"Writing current month data to {sql_table_name}.")
            redfin_df_v3.to_sql(sql_table_name, con=conn, if_exists='append', index=False)

        else:
            print(f"Current data extract period_begin is already in SQL table {sql_table_name}.")
            print(f"SQL table max date = {str(sql_table_max_date)} ---- Current data extract max date = {str(redfin_df_max_date)}")
            logger.write_to_log(f"Current data extract period_begin is already in SQL table {sql_table_name}.")
            logger.write_to_log(f"SQL table max date = {str(sql_table_max_date)} ---- Current data extract max date = {str(redfin_df_max_date)}")

    else:  # Table doesn’t exist, perform initial load
        redfin_df = get_url(universal_url_string)
        # Apply ETL transformations
        redfin_df_v1 = rdm_transformations(redfin_df)
        redfin_df_v2 = sdm_tranformations(redfin_df_v1)
        redfin_df_v3 = cdm_transformation(redfin_df_v2)
        logger.write_to_log(f"DataFrame head:\n{redfin_df_v3.head().to_string()}")
        print(f"Writing historical data to {sql_table_name}.")
        logger.write_to_log(f"Writing historical data to {sql_table_name}.")

        with conn.connect() as connection:
            # Drop the table if it exists
            connection.execute(text(f"DROP TABLE IF EXISTS {sql_table_name}"))
            # Create the table with the correct schema
            connection.execute(text(f"""
            CREATE TABLE {sql_table_name} (
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
                )
        
                    """))

            # Insert data into the new table

            redfin_df_v3.to_sql(sql_table_name, con=connection, if_exists='append', index=False)

            connection.commit()  # Explicitly commit the transaction
                # redfin_df_v3.to_sql(sql_table_name, con=conn, if_exists='replace', index=False)

            conn.dispose()  # Close the connection to SQL Server
if __name__ == '__main__':
    logger.write_to_log('******************************************')
    start_time = time.time()

    try:
        print("Starting main function.")
        logger.write_to_log("Starting main function.")
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
        print(f"Main Function took {minutes:.0f} minutes and {seconds:.2f} seconds to run.")

    # redfin_df = get_url(universal_url_string)
    # redfin_df.to_excel('C:\\Users\\nickk\\PycharmProjects\\Data_Pipelines\\2_Test\\Data_Files\\RedFin\\state_monthly_redfin_housing.xlsx', index=False)


    # test_df = get_url(national_url)
    # test_df_v2 = rdm_transformations(test_df)
    # test_df_v3 = sdm_tranformations(test_df_v2)
    # test_df_v4 = cdm_transformation(test_df_v3)
    #
    # test_df_v4.to_excel('C:\\Users\\nickk\\PycharmProjects\\Data_Pipelines\\2_Test\\Data_Files\\RedFin\\national_monthly_redfin_housing.xlsx', index=False)
