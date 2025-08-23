import pandas as pd
import logging
import random
import numpy as np

def transform(df):

    try:
        logging.info("DATA CLEANING STARTED...\n")


        df.columns = df.columns.str.upper()
        logging.info("Standardized column names to uppercase.")

       
        df['CUSTOMER_NAME'] = df['CUSTOMER_NAME'].str.replace(r'[^a-zA-Z\s]', '', regex=True)
        df['CUSTOMER_NAME'] = df['CUSTOMER_NAME'].str.strip().str.title()
        df[['FIRST_NAME', 'LAST_NAME']] = df['CUSTOMER_NAME'].str.split(' ', n=1, expand=True)
        df = df[df['LAST_NAME'].notna()].copy()
        df['FIRST_NAME'] = df['FIRST_NAME'].str.strip()
        df['LAST_NAME'] = df['LAST_NAME'].str.strip()
        logging.info("Cleaned CUSTOMER_NAME, split into FIRST_NAME and LAST_NAME, dropped rows with missing LAST_NAME.")
        
        df = df[["FIRST_NAME","LAST_NAME","EMAIL","CITY","REGION","PRODUCT_ID","PRODUCT_NAME","CATEGORY","PURCHASE_DATE","AMOUNT","STATUS","PAYMENT_TYPE"]]
        
        df['EMAIL'] = df['EMAIL'].str.lower()
        df['EMAIL'] = df['EMAIL'].str.replace("_mail.com", "@gmail.com")

        email_domain = ["gmail.com", "yahoo.in", "hotmail.com", "outlook.com"]

        df['EMAIL'] = df['EMAIL'].apply(lambda x: f"{x.split('@')[0]}@{random.choice(email_domain)}" if pd.notnull(x) else x)
        df.drop_duplicates(subset=['EMAIL'], inplace=True)
        logging.info("Standardized EMAIL column, fixed domains, dropped duplicates.")

     
        df['CITY'] = df['CITY'].str.title().str.strip()
        df['CITY'] = df['CITY'].replace({"Los Angeles": "Gujarat", "New York": "Punjab", "Bangalore": "Patna"})
        df['REGION'] = df['REGION'].str.title().str.strip()

        state_region = {
            'Hyderabad': "South",
            'Delhi': "North",
            'Punjab': "North",
            'Gujarat': "West",
            'Mumbai': "West",
            'Chennai': "South",
            'Kolkata': "East",
            'Patna': "East"
        }
        df['REGION'] = df['CITY'].map(state_region)
        logging.info("Cleaned CITY names, mapped REGION values.")


        df['PRODUCT_ID'] = df['PRODUCT_ID'].astype(int)
        df['PRODUCT_NAME'] = df['PRODUCT_NAME'].str.strip().str.title()
        df['CATEGORY'] = df['CATEGORY'].str.title().str.strip()

        product_category = {
            'Sports Shoe': 'Sports',
            'Mobile': 'Electronics',
            'Head Phones': 'Accessories',
            'Tablet': 'Electronics',
            'Laptop': 'Electronics',
            'Bag': 'Fashion',
            'Watch': 'Fashion',
            'Television': 'Electronics'
        }
        df['CATEGORY'] = df['PRODUCT_NAME'].map(product_category)
        logging.info("Cleaned PRODUCT_NAME and CATEGORY mapping applied.")

     
        df['STATUS'] = df['STATUS'].str.title().str.strip()
        df['PAYMENT_TYPE'] = df['PAYMENT_TYPE'].str.title().str.strip()
        logging.info("Normalized STATUS and PAYMENT_TYPE values.")

        df['AMOUNT'] = df['AMOUNT'].str.replace('$', '')
        df['AMOUNT'] = df['AMOUNT'].astype(str).str.extract(r'(\d+\.?\d*)')
        df['AMOUNT'] = df['AMOUNT'].astype(float)

        product_price_range = {
            'Sports Shoe': random.randint(1500, 8000),
            'Mobile': random.randint(5000, 80000),
            'Head Phones': random.randint(500, 20000),
            'Tablet': random.randint(8000, 60000),
            'Laptop': random.randint(25000, 150000),
            'Bag': random.randint(700, 10000),
            'Watch': random.randint(500, 50000),
            'Television': random.randint(10000, 200000)
        }
        df['AMOUNT'] = df['PRODUCT_NAME'].map(product_price_range)
        logging.info("Cleaned AMOUNT values and mapped realistic product prices.")

        
        df['PURCHASE_DATE'] = pd.to_datetime(df['PURCHASE_DATE'], errors='coerce')
        null_count = df['PURCHASE_DATE'].isna().sum()
        start_date = pd.to_datetime('2020-01-02')
        end_date = pd.to_datetime('2025-08-04')
        random_dates = pd.to_datetime(np.random.uniform(start_date.value, end_date.value, size=null_count))
        df.loc[df['PURCHASE_DATE'].isna(), 'PURCHASE_DATE'] = random_dates
        df['PURCHASE_DATE'] = df['PURCHASE_DATE'].dt.date
        logging.info("Cleaned PURCHASE_DATE, filled missing values with random dates between 2020–2025.")
        
        return df
    

    except Exception as e:
        logging.error(f'Exception Failed: {e}\n')
        return None
    
    finally:
        logging.info('------------------------------------------------------------')
        logging.info('DATA CLEANING ALL COLUMNS DONE - DATE IS PURE :)')
        logging.info('------------------------------------------------------------\n')
