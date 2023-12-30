
import pandas as pd
import boto3
import json
from sqlalchemy import create_engine
from datetime import datetime
# from logger_config import configure_logger
# log = configure_logger()



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
def read_from_cloud(input_path):
    with open(input_path,'r') as jsonData:
        creds = json.load(jsonData)
        s3 = boto3.resource(service_name = creds['service_name'],region_name = creds['region_name'],
                             aws_access_key_id = creds['Access_key'],aws_secret_access_key= creds['Secret_key'])
        
        obj = s3.Bucket(creds['bucket_name']).Object(creds['file_path']).get()

        if creds['file_format'] == 'csv': 
            df = pd.read_csv(obj['Body'],index_col=0)
            
        else:
            print("kindly check your config")
    return df
def handle_nunValues(df):
    for col in df.columns:
        if df[col].dtype == 'int64' or df[col].dtype == "float64":
            
            df[col].fillna(df[col].mean(),inplace=True)
        else:
            df = df[df[col].notna()]
    return df
def dataType_formating(df):
    for col in df.columns:
        value = df[col][0]

        # Try converting to int
        try:
            int_value = int(value)
            #print("Converted to int:", int_value)
            df[col] = df[col].astype('int64')
        except ValueError:
            # Try converting to float
            try:
                float_value = float(value)
                #print("Converted to float:", float_value)
                df[col] = df[col].astype('float64')
            except ValueError:
                # Try converting to date
                try:
                    date_object = datetime.strptime(value, "%m-%d-%Y")
                    #print("Converted to date:", date_object)
                    df[col] = df[col].astype('datetime64[ns]')
                except ValueError:
                    try:
                        date_object = datetime.strptime(value, "%Y-%m-%d")
                        df[col] = df[col].astype('datetime64[ns]')
                    except ValueError:
                        print("found Target")
                   # log.info("found object data ")

    return df
def data_cleaning(df):

    df.drop_duplicates(inplace=True)
    df = handle_nunValues(df)
    df = dataType_formating(df)
    return df
def write_to_csv(df,file_path,file_name):
    df.to_csv(file_path+file_name,index = False)
    prof_name = "prof_"+file_name.split(".")[0]+".csv"
    prof_df = df.describe()
    prof_df.index.name = "Parameter"
    prof_df.to_csv(file_path+prof_name,index=True)
    return True
def write_to_delimeted(df,file_path,file_name,prof_name,sep):
    df.to_csv(file_path+file_name,index=False,sep = sep)
    prof_df = df.describe()
    prof_df.index.name = "Parameter"
    prof_df.to_csv(file_path+prof_name,index=True,sep = sep)
    return True
def write_to_excel(df,file_path,file_name,prof_name):
    df.to_excel(file_path+file_name,index=False)
    prof_df = df.describe()
    prof_df.index.name = "Parameter"
    prof_df.to_excel(file_path+prof_name,index=True)
    return True
def write_to_parquet(df,file_path,file_name,prof_name):
    df.to_parquet(file_path+file_name,index=False)
    prof_df = df.describe()
    prof_df.index.name = "Parameter"
    prof_df.to_parquet(file_path+prof_name,index=True)
    return True
        
        
