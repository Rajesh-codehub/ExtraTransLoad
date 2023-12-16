
import pandas as pd
from io import StringIO
import json
from sqlalchemy import create_engine


"""\Reading Data From A CSV Or Delimitted File/"""

def read_from_csv_file(input_path,delimeter):
    df = pd.read_csv(input_path,sep=delimeter)
    return df
def read_from_excel_file(input_path):
    df = pd.read_excel(input_path)
    return df
def read_from_parquet_file(input_path):
    df = pd.read_parquet(input_path)
    return df

def read_from_json_file(input_path,orient):
    df = pd.read_json(input_path,lines=True,orient=orient)
    return df
def read_from_sql_file(input_path,root):
    with open(input_path,'r') as jsondata:
        creds = json.load(jsondata)
        try:
            engine = create_engine(f"{root}://{creds['user']}:{creds['password']}@{creds['host']}/{creds['database']}")
            table_name = creds['table_name']
            df = pd.read_sql(f"SELECT * FROM {table_name}",con=engine)
        except Exception as e:
            print(f"Error: {e}")

    return df,table_name
        
