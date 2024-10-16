from _utils import sql_server_query
from _utils import sqlalchemy_engine
import pandas as pd
import datetime as dt
pd.set_option('display.max_columns', None)

sql_cursor, conn = sql_server_query()

test_path = 'C:\\Users\\nickk\\PycharmProjects\\Data_Pipelines\\2_Test\\temp_home_values.csv'

df_v1 = pd.read_csv(test_path)

""" CREATE LOOKUP TABLE OUT OF THE DATAFRAME """
def insert_home_value_lookup():
    df_lookup_v1 = df_v1[['RegionID', 'State', 'City', 'Metro', 'CountyName', 'Zip_Code']].drop_duplicates()
    df_lookup_v1.fillna('', inplace=True)

    # Insert data into SQL Server
    for index, row in df_lookup_v1.iterrows():
        sql_cursor.execute(
            "INSERT INTO dbo.LKP_ZIP_ZW_HOME_VALUES (RegionID, State, City, Metro, CountyName, Zip_Code) VALUES (%s, %s, %s, %s, %s, %s)",
            (row['RegionID'], row['State'], row['City'], row['Metro'], row['CountyName'], row['Zip_Code'])
        )

    # Commit the transaction
    conn.commit()

def insert_home_value_data():
    df_v1['Date_Pulled'] = str(dt.date.today())
    df_v1['Value'] = df_v1['Value'].fillna(0)
    df_v1['SizeRank'] = df_v1['SizeRank'].fillna(0)
    df_v1['Date_Pulled'] = df_v1['Date_Pulled'].fillna('')
    df_v1['Data_Type'] = df_v1['Data_Type'].fillna('')
    df_data_v1 = df_v1[['RegionID', 'SizeRank', 'Month_End_Date', 'Value', 'Data_Type', 'Date_Pulled']].drop_duplicates()

    df_data_v1.to_sql('ZW_HOME_VALUES', sqlalchemy_engine, if_exists='replace', index=False)


    # for index, row in df_data_v1.iterrows():
    #     sql_cursor.execute(
    #         "INSERT INTO dbo.ZW_HOME_VALUES (RegionID, SizeRank, Month_End_Date, Value, Data_Type, Date_Pulled) "
    #         "VALUES (%s, %s, %s, %s, %s, %s)",
    #         (row['RegionID'], row['SizeRank'], row['Month_End_Date'], row['Value'], row['Data_Type'], row['Date_Pulled'])
    #
    #     )

def query_zw_lookup_data():
    df_lookup_query = pd.read_sql_query("SELECT * FROM [DataMining].[dbo].[LKP_ZIP_ZW_HOME_VALUES]", conn)

# query_zw_lookup_data()

# insert_home_value_lookup()
insert_home_value_data()


