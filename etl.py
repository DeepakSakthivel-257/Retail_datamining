# etl.py
import pandas as pd
from sqlalchemy import create_engine
import os

# DB connection - update user/password if needed
USER = 'root'
PASSWORD = '22052006' 
HOST = '127.0.0.1'
PORT = '3306'
DB = 'retail_dw'

engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}', echo=False)

def load_csv_to_table(csv_file, table_name):
    df = pd.read_csv(csv_file)
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(f"Loaded {len(df)} rows into {table_name}")

if __name__ == "__main__":
    # ensure empty tables or run once
    for f, t in [
        ('DimProduct.csv', 'DimProduct'),
        ('DimCustomer.csv', 'DimCustomer'),
        ('DimStore.csv', 'DimStore'),
        ('DimDate.csv', 'DimDate'),
        ('FactSales.csv', 'FactSales'),
    ]:
        if not os.path.exists(f):
            raise FileNotFoundError(f"Missing file {f}. Run generate_data.py first.")
        load_csv_to_table(f, t)
    print("ETL completed.")
