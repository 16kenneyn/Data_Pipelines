import pandas as pd
from datetime import datetime as dt
import requests
from io import StringIO
from _utils import DataPipelineLogger
import numpy as np
from _utils import sqlalchemy_engine  # engine used to connect to SQL Server, to be used in pandas read_sql_query & to_sql
pd.set_option('display.max_columns', None) # Don't truncate columns for dataframes

conn = sqlalchemy_engine()

county_current_month_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_County.csv'
count_historical_month_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_County_History.csv'
zip_current_month_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Zip.csv'
zip_historical_data_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Zip_History.csv'

only_update_current = True

"""Overview
1. Get the current month data for zip codes
    a. Have a switch to only update current data and append to sql server
2. Get the historical data for zip codes
    a. Have a switch to replace sql server with historical data
    b. Basically have a switch for only append current month, or replace with historical and 
3. ETL the data
4. Upload the data to SQL Server
5. Take out pending ratio - don't need it.

"""

def get_url(url: str):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP request errors
        # Read the content of the response into a pandas DataFrame
        data = pd.read_csv(StringIO(response.text), encoding='utf-8-sig')  # encoding='utf-8-sig' is used to handle csv format declaration in first column name. Ex. Ã¯Â»Â¿month_date_yyyymm
        data.rename(columns=lambda x: x.strip().replace("ï»¿", ""), inplace=True)
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def dataframe_etl(dataframe):
    df = dataframe

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
    # Get the current month data for zip codes
    zip_current_df = get_url(zip_current_month_url)
    zip_current_df = dataframe_etl(zip_current_df)
    print(zip_current_df.head(1000))
    print(zip_current_df.columns)

    # Get the historical data for zip codes
    # zip_history_df = get_url(zip_historical_data_url)
    # zip_history_df = dataframe_etl(zip_history_df)
    # print(zip_history_df.head(1000))
    # print(zip_history_df.columns)
    #
    # # Append the current month data to the historical data
    # zip_history_df = pd.concat([zip_history_df, zip_current_df], ignore_index=True)
    # print(zip_history_df.head(1000))
    # print(zip_history_df.columns)
    #
    # # Upload the data to SQL Server
    # zip_history_df.to_sql('realtor_zip_data', con=sqlalchemy_engine, if_exists='replace', index=False)
    #
    # # Check if the data was uploaded
    # check_df = pd.read_sql_query('SELECT * FROM realtor_zip_data', con=sqlalchemy_engine)
    # print(check_df.head(1000))
    # print(check_df.columns)

    return zip_current_df

if __name__ == '__main__':
    redfin_df = get_url(zip_historical_data_url)
    redfin_df_v1 = dataframe_etl(redfin_df)
    redfin_df_v2 = fill_na(redfin_df_v1)
    redfin_df_v3 = columns_to_round(redfin_df_v2)

    redfin_df_v3 = redfin_df_v3.astype({
        'month_date_yyyymm': 'int',
        'zip_name': 'str',
        'median_listing_price': 'float',
        'active_listing_count': 'int',
        'median_days_on_market': 'int',
        'new_listing_count': 'int',
        'price_increased_count': 'int',
        'price_reduced_count': 'int',
        'pending_listing_count': 'int',
        'median_listing_price_per_square_foot': 'float',
        'median_square_feet': 'int',
        'average_listing_price': 'float',
        'total_listing_count': 'int',
        'pending_ratio': 'float',
        'quality_flag': 'int',
        'median_listing_price_ly': 'float',
        'active_listing_count_ly': 'int',
        'median_days_on_market_ly': 'int',
        'new_listing_count_ly': 'int',
        'price_increased_count_ly': 'int',
        'price_reduced_count_ly': 'int',
        'pending_listing_count_ly': 'int',
        'median_listing_price_per_square_foot_ly': 'float',
        'median_square_feet_ly': 'int',
        'average_listing_price_ly': 'float',
        'total_listing_count_ly': 'int',
        'pending_ratio_ly': 'float',
        'median_listing_price_lm': 'float',
        'active_listing_count_lm': 'int',
        'median_days_on_market_lm': 'int',
        'new_listing_count_lm': 'int',
        'price_increased_count_lm': 'int',
        'price_reduced_count_lm': 'int',
        'pending_listing_count_lm': 'int',
        'median_listing_price_per_square_foot_lm': 'float',
        'median_square_feet_lm': 'int',
        'average_listing_price_lm': 'float',
        'total_listing_count_lm': 'int',
        'pending_ratio_lm': 'float',
        'zip_code': 'str'
    })

        # redfin_df_v3.drop(columns=['inf'], inplace=True)
    # redfin_df_v3.to_csv("2_Test/Data_Files/Refin_Period_Zip_10302024.csv")
    redfin_df_v3.to_sql('REALTOR_MNTHLY_ARCHIVE', con=conn, if_exists='replace', index=False)
    # check_df.to_csv('check_df.csv', index=False)
    # print(check_df.head(1000))
    # print(check_df.columns)