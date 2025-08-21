from etl_python.extract import extract_csv
from etl_python.transform import transform 
from etl_python.load import load
import os
import logging

def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    raw_data = os.path.join(BASE_DIR, 'data', 'raw_data.csv')

    data = extract_csv(raw_data)
    df = transform(data)
    load(df)

    logging.info("ETL PIPELINE FINISHED SUCCESFULLY :)")


if __name__ == "__main__":
    main()
