import pandas as pd
import requests
from io import StringIO  # Need this for requesting zillow data
import datetime as dt
import time
import traceback  # Need this to return complete error message for logging except section
from _utils import sqlalchemy_engine  # engine used to connect to SQL Server, to be used in pandas read_sql_query & to_sql
from _utils import DataPipelineLogger

class ZillowRentValues:
    """
    This class is used to pull in Zillow Rent Values data from the Zillow Research website.
    The data is then cleaned up and inserted into a SQL Server database.
    The ZW_Home_Values file has all URL links to the data that is needed for all datasets
    """
    def __init__(self):
        self.rentals = [
            {'Smoothed All Homes Plus Multifamily Time Series': 'https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv?t=1705256896'},
            {'Smoothed (Seasonally Adjusted) All Homes Plus Multifamily Time Series': 'https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_sa_month.csv?t=1705256896'}
        ]

        # Create sql engine used for dataframes to connect to sql server
        self.engine = sqlalchemy_engine()

        # Initialize logger
        self.logger_file_name = 'ZW_Rent_Values'
        self.logger = DataPipelineLogger(self.logger_file_name)

        self.export_to_csv = False
        self.current_date = str(dt.date.today())

        # Filters
        self.state_restriction = False

    def request_zillow_url(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Check for HTTP request errors

            # Read the content of the response into a pandas DataFrame
            data = pd.read_csv(StringIO(response.text))
            return data
        except requests.exceptions.RequestException as e:
            error_trace = traceback.format_exc()
            self.logger.write_to_log(f"An error occurred during the request_zillow_url function: {e}\n{error_trace}")
            print(f"An error occurred: {e}")
            return None

    def get_all_urls(self, url_list):

        # Creating local main dataframe to append data too & return from function
        main_df = pd.DataFrame()

        # Looping through all URLs in list provided in function parameter
        for i in url_list:
            # Temp dataframe of current URL from list - taking value of dict that is i as url input of request_zillow_url
            temp_df = self.request_zillow_url(list(i.values())[0])

            # Filter down dataset to MA so it is more manageable - 76k rows vs 7.5M rows
            if self.state_restriction:
                filtered_data = temp_df.loc[temp_df['State'] == 'MA']
                self.logger.write_to_log('Filtering data down to MA only.')
                print('Filtering data down to MA only.')
            else:
                filtered_data = temp_df
                self.logger.write_to_log('Pulling all states data.')
                print('Pulling all states data')
            self.logger.write_to_log(f'Getting URL for model: {list(i.keys())[0]}')
            print(f'Getting URL for model: {list(i.keys())[0]}')

            filtered_data = filtered_data.drop(columns=['StateName', 'RegionType'])

            # Melt the filtered data - pivot the data so values & date are converted to rows instead of columns
            melted_data = filtered_data.melt(
                id_vars=['RegionID', 'SizeRank', 'RegionName', 'State', 'City', 'Metro',
                         'CountyName'],
                var_name='Month_End_Date',
                value_name='Value')

            #  Convert month end date column to DateTime column
            melted_data['Month_End_Date'] = pd.to_datetime(melted_data['Month_End_Date'])

            # Filling zipcodes that are 4 digits with a leading 0
            melted_data['Zip_Code'] = melted_data['RegionName'].apply(lambda x: str(x).zfill(5))
            melted_data.drop(columns=['RegionName'], inplace=True)

            # Add a new column with the key from the zillow_list - add in data type meaning model type
            key = list(i.keys())[0]
            melted_data['Data_Type'] = key
            # print(melted_data.head())

            # Append the melted data to the main DataFrame (Append the temp df to the main df and move on to the next)
            main_df = pd.concat([main_df, melted_data], ignore_index=True)

        # Add and convert Date_Pulled column to current date
        main_df['Date_Pulled'] = self.current_date
        main_df['Value'] = round(main_df['Value'], 0)

        # Cleanup null values in columns
        main_df['RegionID'] = main_df['RegionID'].fillna(0)
        main_df['SizeRank'] = main_df['SizeRank'].fillna(0)
        main_df['Data_Type'] = main_df['Data_Type'].fillna('')
        main_df['State'] = main_df['State'].fillna('')
        main_df['City'] = main_df['City'].fillna('')
        main_df['Metro'] = main_df['Metro'].fillna('')
        main_df['CountyName'] = main_df['CountyName'].fillna('')
        main_df['Zip_Code'] = main_df['Zip_Code'].fillna('')
        # main_df['Month_End_Date'] = main_df['Month_End_Date'].fillna('')
        main_df['Value'] = main_df['Value'].fillna(0)

        # Setting up the data type lookup table
        df_zw_lkp_zip = main_df[
            ['RegionID', 'State', 'City', 'Metro', 'CountyName', 'Zip_Code']].drop_duplicates()
        df_zw_lkp_zip['Report_Date'] = self.current_date
        df_zw_lkp_zip['Dataset_Type'] = "Rent_Values"
        self.logger.write_to_log('Creating LKP_ZW_ZIP_lOCATION table succeeded.')

        # Insert data into SQL Server - LKP_ZW_DATA_TYPE
        df_zw_lkp_zip.to_sql('LKP_ZW_ZIP_lOCATION_ARCHIVE', self.engine, if_exists='append', index=False)
        self.logger.write_to_log('Inserting append data into LKP_ZW_ZIP_lOCATION succeeded.')

        # Drop columns from df_zq_lkp_zip as they are mainly text columns
        main_df.drop(columns=['State', 'City', 'Metro', 'CountyName', 'Zip_Code'], inplace=True)

        # Query the data type lookup table from SQL Server to merge with the main_df
        df_data_type_lkp = pd.read_sql('LKP_ZW_DATA_TYPE', self.engine)

        # Merge the main_df with the data type lookup table to get the LOOKUP_KEY and then drop the Data_Type column and then to sql server in table ZW_Rent_Values
        main_df = main_df.merge(df_data_type_lkp, left_on='Data_Type', right_on='DATA_TYPE', how='left')
        main_df.drop(columns=['Data_Type', 'DATA_TYPE', 'DATASET_TYPE'], inplace=True)
        main_df.to_sql('ZW_RENT_VALUES', self.engine, if_exists='replace', index=False, chunksize=10000)
        self.logger.write_to_log('Inserting data into ZW_RENT_VALUES succeeded.')

        if self.export_to_csv:
            main_df.to_csv('Data_Files/Rent_Values.csv', index=False)

    def main_run(self):
        self.logger.write_to_log('******************************************')
        start_time = time.time()
        try:
            self.get_all_urls(url_list=self.rentals)
        except Exception as e:
            error_trace = traceback.format_exc()
            self.logger.write_error_to_log(f"An error occurred: {e}\n{error_trace}")
            print(f"An error occurred: {e}")
        finally:
            end_time = time.time()
            elapsed_time = end_time - start_time
            minutes, seconds = divmod(elapsed_time, 60)
            self.logger.write_to_log(f"Main Function took {minutes:.0f} minutes and {seconds:.2f} seconds to run.")
            self.logger.write_to_log('******************************************')

# Temp Main Function
if __name__ == '__main__':
    # Setting universal object variable
    rent_values = ZillowRentValues()
    rent_values.main_run()