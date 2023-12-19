from common_utils import dataRetrival as Dr
from common_utils import logger_config

# Call the function to get the logger
log = logger_config.configure_logger()


config_path = "/home/razz/Rajesh/ExtraTransLoad/config/config_info.csv"
config_df = Dr.read_from_csv_file(config_path,",")
source_locations = config_df['source_location'].to_list()
types = config_df['type'].to_list()
addOns = config_df['addOn'].to_list()


for source_location,type,addOn in zip(source_locations,types,addOns):

    if type == 'csv':
        df = Dr.read_from_csv_file(source_location,delimeter=',')
        log.info('successfully loaded csv file')
        file_name = source_location.split("/")[-1]
        log.info(f"output file name {file_name}")

    elif type == 'delimited':
        df = Dr.read_from_csv_file(source_location,delimeter=addOn)
        log.info('successfully loaded delimited file') 
        file_name = source_location.split("/")[-1]
        log.info(f"output file name {file_name}")

    elif type == 'excel':
        df = Dr.read_from_excel_file(source_location)
        log.info('successfully loaded excel file') 
        file_name = source_location.split("/")[-1]
        log.info(f"output file name {file_name}")

    elif type == 'parquet':
        df = Dr.read_from_parquet_file(source_location)
        log.info('successfully loaded parquet file')
        file_name = source_location.split("/")[-1]
        log.info(f"output file name {file_name}")

    elif type == 'json':
        df = Dr.read_from_json_file(source_location,orient=addOn)
        log.info('successfully loaded json file')
        file_name = source_location.split("/")[-1]
        log.info(f"output file name {file_name}")
    elif type == 'sql': 
        if addOn == 'mysql':
            root = "mysql+mysqlconnector"
            df,file_name = Dr.read_from_sql_file(source_location,root)
            log.info('successfully loaded dataframe from sql;mysql')
            log.info(f"output file name {file_name}")

        elif addOn == 'postgres':
            root = "postgresql+psycopg2"
            df,file_name = Dr.read_from_sql_file(source_location,root)
            log.info('successfully loaded dataframe from sql;postgres')
            log.info(f"output file name {file_name}")

        else:
            print("kindly check your config file!")
    elif type == 'cloud':
        df = Dr.read_from_cloud(source_location)
        log.info('successfully loaded dataframe from cloud;aws')

    else:
        print("kindly check your config file!")

