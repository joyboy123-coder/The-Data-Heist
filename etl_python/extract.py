import pandas as pd
import logging


logging.basicConfig(
    filename="log_file.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def extract_csv(raw_data):
    try:
        logging.info("------------------------------------------------------")
        logging.info("EXTRACTING DATA\n")

        logging.info(f"Starting the Data Extraction from {raw_data}\n")

        df = pd.read_csv(raw_data)

        return df
    
    except Exception as e:
        logging.error(f"Extraction Failed: {e}\n")
        return None
    
    finally:
        logging.info("DATA EXTRACTED SUCCESSFULLY")
        logging.info("--------------------------------------------------------\n")

        

    
