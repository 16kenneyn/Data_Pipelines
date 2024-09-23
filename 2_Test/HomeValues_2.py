import pandas as pd
import requests
from io import StringIO
import datetime as dt
from _utils import sql_server_query

'''
TO-DO:
1. Finish up DB insert function for class
    - Can this be dynamic?  Or do we need an individual function for each pull?
2. Setup main run function for both home values & rental
3. Is it possible to make the get all URL function dynamic where it can pull all states or one state?
4. Will need to re-org directory of project
    - Zillow --- zillow main
5. 
'''

class ZillowValues:
    def __init__(self):
        self.All_Homes_Seasonally_Adjusted = 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1698188357'
        self.home_values = [
            {'All Homes (SFR, Condo/Co-op) Time Series, Smoothed, Seasonally Adjusted': 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1698188357'},
            {'Single-Family Homes Time Series': 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_month.csv?t=1698188357'},
            {'Condo/Co-op Time Series': 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_condo_tier_0.33_0.67_sm_sa_month.csv?t=1698188357'},
            {'1-Bedroom Time Series': 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1698188357'},
            {'2-Bedroom Time Series': 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1698188357'},
            {'3-Bedroom Time Series': 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1698188357'},
            {'4-Bedroom Time Series': 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_4_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1698188357'},
            {'5+ Bedroom Time Series': 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_5_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1698188357'}
        ]
        self.home_value_forecasts = [
            {'All Homes (SFR, Condo/Co-op), Smoothed, Seasonally Adjusted, Mid-Tier': 'https://files.zillowstatic.com/research/public_csvs/zhvf_growth/Zip_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1698188357'},
            {'All Homes (SFR, Condo/Co-op), Raw, Mid-Tier': 'https://files.zillowstatic.com/research/public_csvs/zhvf_growth/Zip_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_month.csv?t=1698188357'}
        ]
        self.rentals = [
            {'Smoothed All Homes Plus Multifamily Time Series': 'https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv?t=1705256896'},
            {'Smoothed (Seasonally Adjusted) All Homes Plus Multifamily Time Series': 'https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_sa_month.csv?t=1705256896'}
        ]

    def request_zillow_url(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Check for HTTP request errors

            # Read the content of the response into a pandas DataFrame
            data = pd.read_csv(StringIO(response.text))
            return data
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    def get_all_urls(self, url_list, state_restriction=True):

        # Creating local main dataframe to append data too & return from function
        main_df = pd.DataFrame()

        # Looping through all URLs in list provided in function parameter
        for i in url_list:

            # Temp dataframe of current URL from list - taking value of dict that is i as url input of request_zillow_url
            temp_df = self.request_zillow_url(list(i.values())[0])

            # Filter down dataset to MA so it is more manageable - 76k rows vs 7.5M rows

            if state_restriction:
                filtered_data = temp_df.loc[temp_df['StateName'] == 'MA']
                print('Filtering data down to MA only.')
            else:
                filtered_data = temp_df
                print('Pulling all states data')
            print(f'Getting URL for model: {list(i.keys())[0]}')

            # QC Headers being exported
            # print(list(filtered_data.columns.values))
            # print(filtered_data.head())

            # Melt the filtered data - pivot the data so values & date are converted to rows instead of columns
            melted_data = filtered_data.melt(
                id_vars=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName', 'State', 'City', 'Metro',
                         'CountyName'],
                var_name='Month_End_Date',
                value_name='Value')

            # # Convert current date column to DateTime column
            # melted_data['date_column'] = pd.to_datetime(melted_data['Date'])

            # Add a new column with the key from the zillow_list - add in data type meaning model type
            key = list(i.keys())[0]
            melted_data['Data_Type'] = key
            print(melted_data.head())

            # Append the melted data to the main DataFrame
            main_df = pd.concat([main_df, melted_data], ignore_index=True)

        # Add a unique_id column by concatenating RegionName and Date
        # main_df['unique_id'] = main_df['RegionName'].astype(str) + ' - ' + main_df['Date']

        # Add a 'Data_Pulled' column with the current date
        current_date = str(dt.date.today())
        main_df['Date_Pulled'] = current_date

        return main_df

    def insert_homevalues_db(self, dataframe, state_restriction=True):

        cursor = sql_server_query()
        # cursor.execute("SELECT * FROM [DataMining].[dbo].[Combined_Building_Permit_Dataset]")
        # tables = cursor.fetchall()
        # for table in tables:
        #     print(table)

        # copilot write insert
        # Establish connection

        # Insert DataFrame to SQL Server
        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO ZW_home_values (RegionID, SizeRank, RegionName, RegionType, StateName, State, City, Metro, CountyName, Date, Value, Data_Type, unique_id, Date_Pulled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row.RegionID, row.SizeRank, row.RegionName, row.RegionType, row.StateName, row.State, row.City,
                           row.Metro, row.CountyName, row.Date, row.Value, row.Data_Type, row.unique_id,
                           row.Date_Pulled)

        # Commit the transaction
        conn.commit()

        # Close connection
        cursor.close()
        conn.close()


def run_home_values():
    # Setting universal object variable
    home_values = ZillowValues()
    # Pulling & inserting in database only MA data
    feed_df = home_values.get_all_urls(url_list=home_values.home_values, state_restriction=True)
    feed_df.to_csv('temp_home_values.csv', index=False)
    # home_values.insert_homevalues_db(dataframe=feed_df, state_restriction=True)
    # Pulling & inserting in database on all states
    # feed_df = home_values.get_all_urls(url_list=home_values.home_values, state_restriction=False)
    # home_values.insert_homevalues_db(dataframe=feed_df, state_restriction=False)


def run_home_rentals():
    # Setting universal object variable
    rental = ZillowValues()
    # Pulling & inserting in database only MA data
    feed_df = rental.get_all_urls(url_list=rental.rentals, state_restriction=True)
    feed_df.to_csv('temp_home_rentals.csv', index=False)
    # rental.insert_rentvalues_db(dataframe=feed_df, state_restriction=True)
    # Pulling & inserting in database on all states
    # feed_df = home_values.get_all_urls(url_list=home_values.rentals, state_restriction=False)
    # home_values.insert_rentvalues_db(dataframe=feed_df)


run_home_values()
run_home_rentals()