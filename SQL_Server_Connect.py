from _utils import sql_server_query
import requests
import pandas as pd
from io import StringIO
import os
from datetime import datetime as dt

# sql_server_query("""SELECT * FROM [DataMining].[dbo].[Combined_Building_Permit_Dataset]""")

home_values = [
    {
        'All Homes (SFR, Condo/Co-op) Time Series, Smoothed, Seasonally Adjusted': 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1698188357'}

]

def request_zillow_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP request errors

        # Read the content of the response into a pandas DataFrame
        data = pd.read_csv(StringIO(response.text))
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

x = request_zillow_url('https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1698188357')
print(x)

def write_text():
    # Create temp text file that contains "Hello, World!"
    with open('hello.txt', 'w') as f:
        f.write(f'Hello, World! - {dt.now()}')

if __name__ == '__main__':
    write_text()
    print('File written successfully!')

# Testing if this reflexts in commit and push
