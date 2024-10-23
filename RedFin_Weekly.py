import pandas as pd
import requests
from io import StringIO

# Global variables and settings
pd.set_option('display.max_columns', None) # Don't truncate columns for dataframes
entire_dataset_link = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_covid19/weekly_housing_market_data_most_recent.tsv000'
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

# df = get_url(entire_dataset_link)
df = pd.read_csv(test_path, delimiter='\t')

# # Take out ' weeks' from duration column and then turn it into an integer column
df['duration_weeks'] = df['duration'].str.replace(' weeks', '').astype(int)
df.drop(columns='duration', inplace=True)

# print all columns and there datatypes


