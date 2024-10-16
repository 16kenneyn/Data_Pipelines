import pymssql
import datetime as dt
import pandas as pd
from sqlalchemy import create_engine
pd.set_option('display.max_columns', None)

# Define a function to query SQL Server
def sql_server_query():
    """
    Establish a connection to SQL Server
    :return: sql_cursor, conn - SQL Server cursor and connection objects
    """
    # Define your connection parameters
    server = 'DESKTOP-UDT3OLM'  # \SQLEXPRESS
    user = 'sa'
    password = 'Masters11'
    database = 'DataMining'  # Use master database to list all databases

    # Establish connection
    conn = pymssql.connect(server=server, user=user, password=password, database=database)
    sql_cursor = conn.cursor()

    return sql_cursor, conn

def sqlalchemy_engine():
    server = 'DESKTOP-UDT3OLM'  # \SQLEXPRESS
    user = 'sa'
    password = 'Masters11'
    database = 'DataMining'  # Use master database to list all databases

    # Create an engine
    engine = create_engine(f'mssql+pymssql://{user}:{password}@{server}/{database}')
    return engine


# temp_engine = sqlalchemy_engine()
#
# dataframe = pd.read_sql_table('LKP_ZW_DATA_TYPE', temp_engine)
# dataframe_v2 = dataframe
#
# dataframe_v3 = dataframe.merge(dataframe_v2, on='LOOKUP_KEY', how='left')
#
# print(dataframe_v3.head())



    # Query all tables
    # cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")

    # Fetch and print all table names
#     tables = cursor.fetchall()
#     for table in tables:
#         print(table[0])

    # Close connection
#     cursor.close()
#     conn.close()
