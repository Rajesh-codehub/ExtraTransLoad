from common_utils import dataRetrival as Dr
from common_utils import logger_config
import pandas as pd
import streamlit as st
import base64

# Call the function to get the logger
log = logger_config.configure_logger()
@st.cache_data
def get_readme_download_link(readme):
    b64 = base64.b64encode(readme.encode()).decode()
    href = f'<a href="data:file/txt;base64,{b64}" download="readme.txt">README</a>'
    return href

readme_text = """this application works for ETL Jobs,like Extracting data from different \
sources(excel,csv,delimeted,json,sql(mysql,postgres),cloud services (aws)) \
after colleting datasets from sources it starts transforming the dataset \
transformatino includes (remove duplicates,handling null values, data type formating) \
in basic edition after transforming the dataset into correct format it loads dyanamically \
into whatever the location you mentioned in the config file available services \
load dataset into  (excel,csv,delimited,sql(mysql,postgres), cloud services (aws)) \
let me tell how to use this application first you need to have a config file, in that \
you have to mentioned whatever the details and information, for demo we provied demo config \
csv file you can download that file for better clarification after that you have to upload the \
config file in the dropbox then you can see the changes in the datframe. once you are ok with \
the dataset information you can clock on load the data its gonna store into the liked location \
Thank YOU\U0001F604"""

st.set_page_config(page_title='ETL Pipe Line',layout='wide')


st.markdown('<p style = "text-align: right; font-size:18px;"> @basicEdition</p>',unsafe_allow_html=True)

c1, c2 = st.columns([3, 16])
with c1:
    st.title("ETL Pipe Line")
with c2:
    st.markdown(get_readme_download_link(readme_text), unsafe_allow_html=True)



col1, col2, col3 = st.columns([1, 4, 1]) 

with col1:
   st.write("")

with col2:

   demo_button = st.button('Demo config')

   uploaded_file = st.file_uploader("Upload Config File", type=['csv'], key='datafile')
   
with col3:
   st.write("")

   
# Rest of code to download demo CSV
def get_csv_download_link(csv):
    b64 = base64.b64encode(csv.encode()).decode() 
    href = f'<a href="data:file/csv;base64,{b64}" download="demo.csv">Download Demo CSV File</a>'
    return href

if demo_button:
    demo_csv = '''source_location,type,addOn,output_path,file_name,type2
/home/razz/Rajesh/ExtraTransLoad/Credentials/mysql.json,sql,mysql,/home/razz/Rajesh/ExtraTransLoad/output/csv/,mockdata.csv,csv
    '''
    st.markdown(get_csv_download_link(demo_csv), unsafe_allow_html=True)


# def get_csv_download_link(csv):
#     b64 = base64.b64encode(csv.encode()).decode() 
#     href = f'<a href="data:file/csv;base64,{b64}" download="demo.csv">Download Demo CSV File</a>'
#     return href

# if st.button('Demo config'):
#     demo_csv = '''
#     source_location,type,addOn,output_path,file_name,type2
#     /home/razz/Rajesh/ExtraTransLoad/Credentials/mysql.json,sql,mysql,/home/razz/Rajesh/ExtraTransLoad/output/csv/,mockdata.csv,csv
#     '''
#     st.markdown(get_csv_download_link(demo_csv), unsafe_allow_html=True)

# col1, col2, col3 = st.columns([1,4,1])
# with col1:
#     st.write("")
# with col2:
#     st.markdown(""" <style> 
#         .box {
#           background-color: #fff;
#           border-radius: 5px;
#           box-shadow: 0px 0px 5px 0px rgba(0,0,0,0.75);
#           padding: 20px; 
#         }
#         </style>  
#         """, unsafe_allow_html=True)

#     # Button inside box  
#     uploaded_file = st.file_uploader("upload Config File", type=['csv'], key='datafile')

#     st.markdown("""</div>""", unsafe_allow_html=True)
# with col3:
#     st.write("")



if uploaded_file is not None:
    #config_path = "/home/razz/Rajesh/ExtraTransLoad/config/config_info.csv"
    config_df = Dr.read_from_csv_file(uploaded_file, ",")
    source_locations = config_df['source_location'].to_list()
    types = config_df['type'].to_list()
    addOns = config_df['addOn'].to_list()
    output_paths = config_df['output_path'].to_list()
    file_names = config_df['file_name']
    types2 = config_df['type2'].to_list()

    for source_location, type, addOn, output_path, file_name, type2 in zip(source_locations, types, addOns, output_paths, file_names, types2):

        if type == 'csv':
            df = Dr.read_from_csv_file(source_location, delimiter=',')
            log.info('successfully loaded csv file')
            file_name = source_location.split("/")[-1]
            log.info(f"output file name {file_name}")

        elif type == 'delimited':
            df = Dr.read_from_csv_file(source_location, delimiter=addOn)
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
            df = Dr.read_from_json_file(source_location, orient=addOn)
            log.info('successfully loaded json file')
            file_name = source_location.split("/")[-1]
            log.info(f"output file name {file_name}")
            
        elif type == 'sql':
            if addOn == 'mysql':
                root = "mysql+mysqlconnector"
                df, file_name1 = Dr.read_from_sql_file(source_location, root)
                log.info('successfully loaded dataframe from sql;mysql')
                log.info(f"output file name {file_name}")

            elif addOn == 'postgres':
                root = "postgresql+psycopg2"
                df, file_name = Dr.read_from_sql_file(source_location, root)
                log.info('successfully loaded dataframe from sql;postgres')
                log.info(f"output file name {file_name}")

            else:
                print("kindly check your config file!")

        elif type == 'cloud':
            df = Dr.read_from_cloud(source_location)
            log.info('successfully loaded dataframe from cloud;aws')

        else:
            print("kindly check your config file!")

        # print(df.info())
        st.subheader("Before Transforming:")
        # df_info  = df.info()
        column_info = pd.DataFrame({
            'Column': df.columns,
            'Type': df.dtypes,
            'Non-Null Count': df.count(),
            'Dtype': [df[col].dtype for col in df.columns]
        })

        if st.button("DateFrame info"):
            st.dataframe(column_info, hide_index=True)

        df = Dr.data_cleaning(df)
        # prof_name = "prof_"+file_name.split(".")[0]+".csv"
        st.subheader("After Transformation:")
        column_info = pd.DataFrame({
            'Column': df.columns,
            'Type': df.dtypes,
            'Non-Null Count': df.count(),
            'Dtype': [df[col].dtype for col in df.columns]
        })

        if st.button("Dataframe info"):
            st.dataframe(column_info, hide_index=True)

        st.subheader("Load dataset")
        if st.button("load"):
            Dr.write_to_csv(df, output_path, file_name)
            st.success("data loaded successfully")

else:
    st.warning("kindly upload the config file")


# for source_location,type,addOn,output_path,file_name,type2 in zip(source_locations,types,addOns,output_paths,file_names,types2):

#     if type == 'csv':
#         df = Dr.read_from_csv_file(source_location,delimeter=',')
#         log.info('successfully loaded csv file')
#         file_name = source_location.split("/")[-1]
#         log.info(f"output file name {file_name}")

#     elif type == 'delimited':
#         df = Dr.read_from_csv_file(source_location,delimeter=addOn)
#         log.info('successfully loaded delimited file') 
#         file_name = source_location.split("/")[-1]
#         log.info(f"output file name {file_name}")

#     elif type == 'excel':
#         df = Dr.read_from_excel_file(source_location)
#         log.info('successfully loaded excel file') 
#         file_name = source_location.split("/")[-1]
#         log.info(f"output file name {file_name}")

#     elif type == 'parquet':
#         df = Dr.read_from_parquet_file(source_location)
#         log.info('successfully loaded parquet file')
#         file_name = source_location.split("/")[-1]
#         log.info(f"output file name {file_name}")

#     elif type == 'json':
#         df = Dr.read_from_json_file(source_location,orient=addOn)
#         log.info('successfully loaded json file')
#         file_name = source_location.split("/")[-1]
#         log.info(f"output file name {file_name}")
#     elif type == 'sql': 
#         if addOn == 'mysql':
#             root = "mysql+mysqlconnector"
#             df,file_name1 = Dr.read_from_sql_file(source_location,root)
#             log.info('successfully loaded dataframe from sql;mysql')
#             log.info(f"output file name {file_name}")

#         elif addOn == 'postgres':
#             root = "postgresql+psycopg2"
#             df,file_name = Dr.read_from_sql_file(source_location,root)
#             log.info('successfully loaded dataframe from sql;postgres')
#             log.info(f"output file name {file_name}")

#         else:
#             print("kindly check your config file!")
#     elif type == 'cloud':
#         df = Dr.read_from_cloud(source_location)
#         log.info('successfully loaded dataframe from cloud;aws')
    

#     else:
#         print("kindly check your config file!")
#     #print(df.info())
#     st.subheader("Before Transforming:")
#     #df_info  = df.info()
#     column_info = pd.DataFrame({
#     'Column': df.columns,
#     'Type': df.dtypes,
#     'Non-Null Count': df.count(),
#     'Dtype': [df[col].dtype for col in df.columns]
# })
#     if st.button("DateFrame info"):
#         st.dataframe(column_info,hide_index = True)
#     df = Dr.data_cleaning(df)
#     #prof_name = "prof_"+file_name.split(".")[0]+".csv"
#     st.subheader("After Transformation:")
#     column_info = pd.DataFrame({
#     'Column': df.columns,
#     'Type': df.dtypes,
#     'Non-Null Count': df.count(),
#     'Dtype': [df[col].dtype for col in df.columns]})
#     if st.button("Dataframe info"):
#         st.dataframe(column_info,hide_index=True)
#     st.subheader("storing dataset")
#     if st.button("load Data"):

#         Dr.write_to_csv(df,output_path,file_name)
#         st.success("data loaded successfully")

