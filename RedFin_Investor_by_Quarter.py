import pandas as pd
from datetime import datetime as dt
import traceback
import time
from _utils import DataPipelineLogger
import numpy as np
from _utils import sqlalchemy_engine
from sqlalchemy.types import Date

# Initialize logger
logger = DataPipelineLogger("RedFin_Investor_by_Quarter")

def quarter_to_date(quarter_str: str) -> pd.Timestamp:
    """Convert a quarter string (e.g., '2024 Q4') to the quarter's start date (e.g., '2024-10-01')."""
    try:
        year, q = quarter_str.split()
        quarter_num = int(q[1])  # Extract number from 'Q4' -> 4
        month = (quarter_num - 1) * 3 + 1  # Map to start month: Q1:1, Q2:4, Q3:7, Q4:10
        return pd.to_datetime(f"{year}-{month:02d}-01")
    except Exception as e:
        logger.write_error_to_log(f"Error parsing quarter {quarter_str}: {e}")
        raise

def dataframe_etl(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Apply transformations to calculate new columns and add start_quarter_date."""
    df = dataframe.copy()

    # Fill down all quarter merged cells - file merges the quarter column for all records, only first record will get picked up
    # Drop completely empty rows (if any)
    df = df.dropna(how='all')

    # Forward fill the "Quarter" column to propagate quarter values
    df['Quarter'] = df['Quarter'].ffill()

    # Calculate total_purchases, avoiding division by zero
    df['total_purchases'] = np.where(
        df['Investor Market Share'] == 0,
        0,
        df['Investor Purchases'] / df['Investor Market Share']
    )
    # Calculate non_investor_purchases
    df['non_investor_purchases'] = df['total_purchases'] - df['Investor Purchases']
    # Calculate Investor_Purchases_LY, handling YoY of -1
    df['Investor_Purchases_LY'] = np.where(
        df['Investor Purchases YoY'] == -1,
        0,
        df['Investor Purchases'] / (1 + df['Investor Purchases YoY'])
    )
    # Add start_quarter_date by converting Quarter column
    df['start_quarter_date'] = df['Quarter'].apply(quarter_to_date)
    return df

def fill_na(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Handle null and infinite values in the dataframe."""
    df = dataframe.copy()
    numeric_cols = [
        'Investor Market Share', 'Investor Purchases', 'Investor Purchases YoY',
        'total_purchases', 'non_investor_purchases', 'Investor_Purchases_LY'
    ]
    # Fill nulls: 0 for numeric, empty string for text
    df[numeric_cols] = df[numeric_cols].fillna(0)
    df['Quarter'] = df['Quarter'].fillna('')
    df['Redfin Metro'] = df['Redfin Metro'].fillna('')
    # Replace infinite values with 0
    df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], 0)
    return df


def chg_data_type(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Set correct data types, round specific columns, and define column order."""
    df = dataframe.copy()

    # Set data types
    df['Quarter'] = df['Quarter'].astype(str)
    df['start_quarter_date'] = pd.to_datetime(df['start_quarter_date'])
    df['Redfin Metro'] = df['Redfin Metro'].astype(str)
    df['Investor Market Share'] = df['Investor Market Share'].astype(float)
    df['Investor Purchases'] = df['Investor Purchases'].astype(float)
    df['Investor Purchases YoY'] = df['Investor Purchases YoY'].astype(float)
    df['total_purchases'] = df['total_purchases'].astype(float)
    df['non_investor_purchases'] = df['non_investor_purchases'].astype(float)
    df['Investor_Purchases_LY'] = df['Investor_Purchases_LY'].astype(float)

    # Round specific columns to 4 decimal places
    columns_to_round = [
        'Investor Market Share',
        'Investor Purchases YoY',
        'total_purchases',
        'Investor_Purchases_LY',
        'non_investor_purchases'
    ]
    for col in columns_to_round:
        df[col] = df[col].round(4)

    df['Update_Date'] = dt.today().strftime("%Y-%m-%d %H:%M:%S")

    # Define column order
    columns = [
        'Quarter', 'start_quarter_date', 'Redfin Metro', 'Investor Market Share',
        'Investor Purchases', 'Investor Purchases YoY', 'total_purchases',
        'non_investor_purchases', 'Investor_Purchases_LY', 'Update_Date'
    ]
    return df[columns]

def main_run():
    """Process the Excel file and overwrite the SQL table."""
    conn = sqlalchemy_engine()
    sql_table_name = 'REDFIN_INVESTOR_BY_QUARTER'
    excel_file_path = '7_Manual_Data_Uploads/Redfin/dowload_investor_purchases_by_metro.xlsx'

    # Read the full Excel file
    df = pd.read_excel(excel_file_path)
    logger.write_to_log(f"Read Excel file from {excel_file_path} with {len(df)} rows.")

    # Apply transformations without dropping rows prematurely
    df = dataframe_etl(df)
    df = fill_na(df)
    df = chg_data_type(df)

    # Log a preview of the transformed data
    logger.write_to_log(f"DataFrame head:\n{df.head().to_string()}")

    # Overwrite the SQL table with specified dtype for start_quarter_date
    print(f"Overwriting data in {sql_table_name}.")
    df.to_sql(
        sql_table_name,
        con=conn,
        if_exists='replace',
        index=False,
        dtype={'start_quarter_date': Date()}
    )
    logger.write_to_log(f"Overwrote {sql_table_name} with {len(df)} rows.")

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