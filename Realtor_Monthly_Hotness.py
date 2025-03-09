import pandas as pd
from datetime import datetime as dt
import requests
from io import StringIO
from _utils import DataPipelineLogger
import numpy as np
from _utils import sqlalchemy_engine  # engine used to connect to SQL Server, to be used in pandas read_sql_query & to_sql
pd.set_option('display.max_columns', None) # Don't truncate columns for dataframes

class Realtor_Hotness:
    def __init__(self):
        self.conn = sqlalchemy_engine()
        self.metro_history_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Hotness/RDC_Inventory_Hotness_Metrics_Metro_History.csv'
        self.county_history_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Hotness/RDC_Inventory_Hotness_Metrics_County_History.csv'
        self.zip_history_url = 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Hotness/RDC_Inventory_Hotness_Metrics_Zip_History.csv'
        self.testing = True
        self.test_file_metro_history = 'C:\\Users\\nickk\\PycharmProjects\\Data_Pipelines\\2_Test\\Data_Files\\Realtor_Hotness_Sample_Files\\RDC_Inventory_Hotness_Metrics_Metro_History.csv'

    def get_url(self, url: str):
        """
        If you set the testing parameter to True, this function will import a sample file from the test folder as the
        source.  If it's False, then it will request the live file directly from Realtor.com as intended in production.
        :param url:
        :return:
        """

        if self.testing:
            data = pd.read_csv(self.test_file_metro_history)
            return
        else:
            try:
                response = requests.get(url)
                response.raise_for_status()  # Check for HTTP request errors
                # Read the content of the response into a pandas DataFrame
                data = pd.read_csv(StringIO(response.text),
                                   encoding='utf-8-sig')  # encoding='utf-8-sig' is used to handle csv format declaration in first column name. Ex. Ã¯Â»Â¿month_date_yyyymm
                data.rename(columns=lambda x: x.strip().replace("ï»¿", ""), inplace=True)
                return data
            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {e}")
                return None

    def dataframe_etl(self, input_dataframe):
        """
        This converts LM, LY, & vs US metrics that are the % chg or net chg into the actual LM, LY, & vs US metrics.
        It then rounds these columns to either 2 or 4 past the decimal place to save space in sql.
        Finally it then renames a few of the columns to be more descriptive.
        :param input_dataframe:
        :return: df
        """

        df = input_dataframe
        # Converting change columns to LY, LM, US for aggregation in PBI
        df['hotness_rank_lm'] = df['hotness_rank'] - df['hotness_rank_mm']
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
        return df

    

# ToDO: Can't figure out how to convert page view count per property change to current and ly or lm.

# df.drop(columns=['hotness_rank_mm', 'hotness_rank_yy', 'median_dom_yy_day', 'median_dom_mm_day', 'median_dom_vs_us'], inplace=True)



