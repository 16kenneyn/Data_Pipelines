import pandas as pd
pd.set_option('display.max_columns', None)
# specify your file path
file_path = 'C:\\Users\\nickk\\Downloads\\weekly_housing_market_data_most_recent.tsv000'

# read the TSV file into a DataFrame
df = pd.read_csv(file_path, delimiter='\t')

# display the first few rows of the DataFrame
# print(df.head())
print(df.columns)