import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
from dotenv import load_dotenv
import os

load_dotenv()


user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
database = os.getenv('SNOWFLAKE_DATABASE')
schema = os.getenv('SNOWFLAKE_SCHEMA')
role = os.getenv('SNOWFLAKE_ROLE')
table = os.getenv('SNOWFLAKE_TABLE')


if not table:
    raise ValueError("❗ Environment variable SNOWFLAKE_TABLE is missing or empty.")

def load(df):
    try:
        logging.info('DATA LOADING\n')
        logging.info('Connecting to Snowflake')

        conn = snowflake.connector.connect(
            user = user,
            password = password,
            account = account,
            warehouse = warehouse,
            database = database,
            schema = schema,
            role = role,
            insecure_mode = True
        )


        logging.info('Snowflake Connected')

        df = df.reset_index(drop = True)

        success,nchunks,nrows,_ = write_pandas(
            conn = conn,
            df = df,
            table_name= table,
            schema= schema,
            database= database,
            auto_create_table=True
        )

        if success:
            logging.info(f"Successfully {nrows} loaded data into Snowflake : {table.upper()}\n")
        
        else:
            logging.error('Data Upload Failed\n')

    
    except Exception as e:
        logging.error(f"Failed to Load Data into Snowflake : {e}\n")


    finally:
        logging.info('--------------------------------------------')
        logging.info('DATA LOADING COMPLETED :)')
        logging.info('--------------------------------------------\n')

