Complete Guide to Setting Up PySpark on Windows
This guide walks you through installing and configuring PySpark on a Windows machine, addressing errors encountered during the process and ensuring compatibility with your ETL workflow. Follow each step carefully, and you’ll have a working PySpark environment.

Step 1: Install Java 11
Why This Step Is Necessary
PySpark requires a compatible Java Development Kit (JDK) to run. Initially, you had Java 21 installed, which caused the error:

java.lang.UnsupportedOperationException: getSubject is not supported
This error occurred because Java 21 introduces stricter module and security restrictions incompatible with the Hadoop libraries used by PySpark (Spark 3.x). PySpark officially supports Java 8, 11, and 17. We chose Java 11 for its broad compatibility and stability with Spark.

Instructions
Download Java 11:
Visit Eclipse Adoptium.
Select JDK 11 (e.g., version 11.0.15+10).
Download the Windows installer (.msi or .exe), such as jdk-11.0.15_windows-x64_bin.exe.
Install Java 11:
Run the installer and follow the prompts.
Note the installation path (e.g., C:\Program Files\Eclipse Adoptium\jdk-11.0.15.10-hotspot).
Set JAVA_HOME Environment Variable:
Right-click This PC or Computer in File Explorer and select Properties.
Click Advanced system settings on the left.
In the System Properties window, click Environment Variables.
Under System variables, click New (or Edit if it exists):
Variable name: JAVA_HOME
Variable value: C:\Program Files\Eclipse Adoptium\jdk-11.0.15.10-hotspot (adjust to your installation path).
Click OK.
Update PATH Environment Variable:
In the System variables section, find the Path variable and click Edit.
Click New and add %JAVA_HOME%\bin.
Optionally, use Move Up to prioritize this version over others.
Click OK to save.
Verify Java Installation:
Open a new Command Prompt and run:
text

Collapse

Wrap

Copy
java -version
Expected output:
text

Collapse

Wrap

Copy
openjdk version "11.0.15" 2022-04-19
OpenJDK Runtime Environment Temurin-11.0.15+10 (build 11.0.15+10)
OpenJDK 64-Bit Server VM Temurin-11.0.15+10 (build 11.0.15+10, mixed mode)
Details
Version to Select: JDK 11 (e.g., 11.0.15+10).
Link: Eclipse Adoptium.
Error Resolved: java.lang.UnsupportedOperationException: getSubject is not supported.
Step 2: Set Up winutils.exe for Windows
Why This Step Is Necessary
PySpark relies on Hadoop libraries, which require winutils.exe on Windows. Without it, you encountered:

Did not find winutils.exe: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset
This error indicates that winutils.exe was missing, and the HADOOP_HOME environment variable wasn’t set.

Instructions
Download winutils.exe:
Go to this GitHub repository.
Navigate to hadoop-3.3.0/bin/winutils.exe and download it.
Set Up Directory:
Create a folder, e.g., C:\winutils.
Inside it, create a bin subfolder: C:\winutils\bin.
Place winutils.exe in C:\winutils\bin.
Set HADOOP_HOME Environment Variable:
Right-click This PC or Computer in File Explorer and select Properties.
Click Advanced system settings.
Click Environment Variables.
Under System variables, click New:
Variable name: HADOOP_HOME
Variable value: C:\winutils (use single backslashes).
Edit the Path variable, add %HADOOP_HOME%\bin if not already present, and click OK.
Verify winutils.exe:
Open a Command Prompt and run:
text

Collapse

Wrap

Copy
C:\winutils\bin\winutils.exe
If it runs (you may see a usage message), it’s working. If it fails, check permissions:
Right-click C:\winutils\bin\winutils.exe > Properties > Security > Grant your user Full control.
Details
Version to Select: Hadoop 3.3.0.
Link: winutils.exe for Hadoop 3.3.0.
Error Resolved: Did not find winutils.exe: java.io.FileNotFoundException.
Step 3: Configure SparkSession to Use localhost
Why This Step Is Necessary
Spark attempted to use your machine’s hostname, resulting in:

org.apache.spark.SparkException: Invalid Spark URL: spark://HeartbeatReceiver@Nicks_Dell.myfiosgateway.com:50249
This error arose due to Windows hostname resolution issues. Forcing Spark to use localhost bypasses this problem.

Instructions
Update SparkSession Configuration:
In your Python script, configure the SparkSession as follows:
python

Collapse

Wrap

Copy
spark = SparkSession.builder \
    .appName("Pandas to PySpark ETL") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()
Details
Error Resolved: org.apache.spark.SparkException: Invalid Spark URL.
Step 4: Install Required Python Packages
Why This Step Is Necessary
Your script requires specific Python libraries. You encountered:

ModuleNotFoundError: No module named 'distutils'
This error occurred in Python 3.12 because distutils is no longer included by default, requiring setuptools.

Instructions
Install Python Packages:
Open a Command Prompt and run:
bash

Collapse

Wrap

Copy
pip install pandas pyspark sqlalchemy pyodbc setuptools
Details
Links:
pandas
pyspark
sqlalchemy
pyodbc
setuptools
Error Resolved: ModuleNotFoundError: No module named 'distutils'.
Step 5: Restart Terminal and PyCharm
Why This Step Is Necessary
Environment variable changes (e.g., JAVA_HOME, HADOOP_HOME) don’t take effect until the terminal and applications like PyCharm are restarted.

Instructions
Close and Reopen Terminal:
Close all Command Prompt or terminal windows.
Open a new one.
Restart PyCharm:
Close PyCharm completely.
Reopen it and load your project.
Details
Ensures the updated environment variables are applied.
Step 6: Update Hosts File (Optional)
Why This Step Is Necessary
If hostname issues persist, manually mapping your hostname to 127.0.0.1 can resolve:

org.apache.spark.SparkException: Invalid Spark URL
Instructions
Edit Hosts File:
Open C:\Windows\System32\drivers\etc\hosts in a text editor (run as administrator).
Add:
text

Collapse

Wrap

Copy
127.0.0.1 Nicks_Dell.myfiosgateway.com
Save and close.
Details
Error Resolved: org.apache.spark.SparkException: Invalid Spark URL (if still occurring).
Step 7: Test PySpark Setup
Why This Step Is Necessary
To confirm PySpark is working before running your ETL script.

Instructions
Run a Test Script:
Use this code:
python

Collapse

Wrap

Copy
import os
os.environ['HADOOP_HOME'] = 'C:\\winutils'

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
df.show()
spark.stop()
If it runs and displays the DataFrame, your setup is successful.
Step 8: Run Your ETL Script
Why This Step Is Necessary
This is your goal: running an ETL script with PySpark. You mentioned issues converting back to a Pandas DataFrame, but the setup works for PySpark operations.

Instructions
Update Your Script:
Use this template, adjusting paths and credentials:
python

Collapse

Wrap

Copy
import os
os.environ['HADOOP_HOME'] = 'C:\\winutils'

import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Pandas to PySpark ETL") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Load Pandas DataFrame
pandas_df = pd.read_excel('C:\\path\\to\\your\\file.xlsx')

# Convert to PySpark DataFrame
spark_df = spark.createDataFrame(pandas_df)

# Run SQL query
spark_df.createOrReplaceTempView("my_table")
result_df = spark.sql("SELECT * FROM my_table")

# (Optional) Convert back to Pandas and export
pandas_result_df = result_df.toPandas()
connection_string = 'mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(connection_string)
pandas_result_df.to_sql('table_name', con=engine, if_exists='replace', index=False)

# Stop SparkSession
spark.stop()
Details
Error Resolved: java.net.SocketException: Connection reset by peer (related to hostname, fixed with localhost).
Additional Notes
Performance: Converting between Pandas and PySpark adds overhead. For small datasets, Pandas alone may suffice; PySpark excels with large datasets.
Java Updates: If you update Java later, adjust JAVA_HOME and Path accordingly.
Troubleshooting: If new errors arise, note the exact messages and share them for further assistance.
This documentation should enable you to set up PySpark on your other Windows virtual machines successfully. Let me know if you need help with specific steps or new issues!