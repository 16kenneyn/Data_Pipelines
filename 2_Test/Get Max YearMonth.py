import pandas as pd
from datetime import datetime as dt
import requests
from io import StringIO
from _utils import DataPipelineLogger
import numpy as np
from _utils import sqlalchemy_engine  # engine used to connect to SQL Server, to be used in pandas read_sql_query & to_sql
pd.set_option('display.max_columns', None) # Don't truncate columns for dataframes

from sqlalchemy import text
# conn = sqlalchemy_engine().connect()  # Connecting to SQL Server

# # Define the SQL query using text()
# get_max_date = text("""
# SELECT MAX(month_date_yyyymm) FROM [DataMining].[dbo].[REALTOR_MNTHLY_STATE]
# """)
#
# # Use a context manager to handle the connection
# with sqlalchemy_engine().connect() as connection:
#     result = connection.execute(get_max_date)
#     max_date = result.scalar()  # Fetch the single value
#     print(max_date)

def get_latest_yearmonth(table_name:str):
    # Define the SQL query using text() ----------- ToDo: UPDATE THIS QUERY ONCE THE FINAL TABLE IS CREATED
    get_max_date = text(f"""
    SELECT MAX(month_date_yyyymm) FROM [DataMining].[dbo].[{table_name}]
    """)

    # Use a context manager to handle the connection
    with sqlalchemy_engine().connect() as connection:
        result = connection.execute(get_max_date)
        max_date = result.scalar()  # Fetch the single value
        if max_date: # Error handling to determine if None is returned or a string
            max_date = int(max_date)

        else:
            print(max_date)
        connection.close() # Close the connection after execution to avoid timeout issues.
    return max_date

get_latest_yearmonth('REALTOR_MNTHLY_STATE')
