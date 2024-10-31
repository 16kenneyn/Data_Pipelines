import pandas as pd
import requests
from io import StringIO
import datetime as dt
from _utils import DataPipelineLogger
pd.set_option('display.max_columns', None) # Don't truncate columns for dataframes

logger = DataPipelineLogger('RedFin_Weekly')

current_date = str(dt.date.today())

# Global variables and settings
entire_dataset_link = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_covid19/weekly_housing_market_data_most_recent.tsv000'
                       https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Zip.csv
test_path = '2_Test/Data_Files/weekly_housing_market_data_most_recent.tsv000'

def get_url(url: str):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP request errors

        # Read the content of the response into a pandas DataFrame
        data = pd.read_csv(StringIO(response.text))
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

df = get_url(entire_dataset_link)
# df = pd.read_csv(test_path, delimiter='\t')
df['state'] = df['region_name'].apply(lambda x: x.split(", ")[-1].replace(' metro area', ''))

df.round(4)

# Getting LY Columns

# # Take out ' weeks' from duration column and then turn it into an integer column
df['duration_weeks'] = df['duration'].str.replace(' weeks', '').astype(int)
df.drop(columns='duration', inplace=True)

df['adjusted_average_new_listings_LY'] = df['adjusted_average_new_listings'] / (1 + df['adjusted_average_new_listings_yoy'])

# print all columns and there datatypes


