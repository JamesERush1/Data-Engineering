import pyodbc
import pandas as pd
from sqlalchemy import create_engine

from pyspark.sql import SparkSession
from pyspark.sql import Row
from requests_func import api_extract,api_cleanse,access_key

# Define connection parameters
server = 'HOME\MSSQLSERVER01'  # Example: 'localhost\SQLEXPRESS'
database = 'PythonTestDB'


# If using Windows Authentication (Trusted Connection)
connection_string = f"mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(connection_string)


exchange_rate_df = api_cleanse(api_extract('latest',access_key,'USD,EUR,AUD,GBP'))


try:
    exchange_rate_df = exchange_rate_df.reset_index(drop=True)
except:
    print(exchange_rate_df.columns)



csv_file = r"C:\Users\tim39\Desktop\Loan Prediction.csv"
df = pd.read_csv(csv_file)


df.to_sql("PYTHON_TEST", con=engine, if_exists="replace", index=False)
exchange_rate_df.to_sql('CURRENCY_RATES',con=engine, if_exists='append')




