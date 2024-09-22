import pymssql

def sql_server_query(input_query: str):
    # Define your connection parameters
    server = 'DESKTOP-UDT3OLM'  # \SQLEXPRESS
    user = 'sa'
    password = 'Masters11'
    database = 'DataMining'  # Use master database to list all databases

    # Establish connection
    conn = pymssql.connect(server=server, user=user, password=password, database=database)
    cursor = conn.cursor()

    # Query all tables
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")

    # Fetch and print all table names
    tables = cursor.fetchall()
    for table in tables:
        print(table[0])

    # Close connection
    cursor.close()
    conn.close()