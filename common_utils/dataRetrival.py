
import pandas as pd
from io import StringIO

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
