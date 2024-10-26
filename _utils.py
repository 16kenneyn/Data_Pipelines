import pymssql
import datetime as dt
import pandas as pd
from sqlalchemy import create_engine
import logging
import time
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

class DataPipelineLogger:
    def __init__(self, log_file_name):
        self.log_file_name = "3_Logging/" + log_file_name + ".log"
        # Configure logging
        logging.basicConfig(
            filename=self.log_file_name,
            level=logging.INFO,  # Change to ERROR if you only want to log errors
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def run_with_logging(self, func):
        start_time = time.time()
        func_name = func.__name__ if hasattr(func, '__name__') else str(func)
        try:
            logging.info(f"{func_name} started.")
            func()
            logging.info(f"{func_name} completed successfully.")
        except Exception as e:
            logging.error(f"An error occurred in {func_name}", exc_info=True)
        finally:
            end_time = time.time()
            elapsed_time = end_time - start_time
            minutes, seconds = divmod(elapsed_time, 60)
            logging.info(f"{func_name} took {minutes:.0f} minutes and {seconds:.2f} seconds to run.")

    def write_to_log(self, input_string: str):
        logging.info(input_string)

    def write_error_to_log(self, input_string: str):
        logging.error(input_string)




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
