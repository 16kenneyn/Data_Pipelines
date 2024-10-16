import pandas as pd
import requests
from io import StringIO
import datetime as dt

test_dict = {'All Homes (SFR, Condo/Co-op) Time Series, Smoothed, Seasonally Adjusted': 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1698188357'}

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

def main():
    temp_df = get_url(list(test_dict.values())[0])

    # Filter down dataset to MA so it is more manageable - 76k rows vs 7.5M rows
    filtered_data = temp_df.loc[temp_df['StateName'] == 'MA']

    # Melt the filtered data - pivot the data so values & date are converted to rows instead of columns
    melted_data = filtered_data.melt(
        id_vars=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName', 'State', 'City', 'Metro',
                 'CountyName'],
        var_name='Month_End_Date',
        value_name='Value')

    # # Convert current date column to DateTime column
    # melted_data['date_column'] = melted_data['Date'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d'))

    # Add a new column with the key from the zillow_list - add in data type meaning model type
    key = list(test_dict.keys())[0]
    melted_data['Data_Type'] = key
    print(melted_data)
    return melted_data


dataframe_temp = main()

print(dataframe_temp.columns)