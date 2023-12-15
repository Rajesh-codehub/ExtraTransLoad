import pandas as pd
from faker import Faker
faker = Faker()


num_rows = 10
column_name = ['name','company','Date_of_birth']
data_list = []
for _ in range(num_rows):
    list_data = [faker.name(),faker.company(),faker.date_of_birth()]
    data_list.append(list_data)
df = pd.DataFrame(data_list,columns = column_name)
print(df.head())
df.to_json('/home/razz/Rajesh/ExtraTransLoad/source_files/mockdata.json',orient='records',lines=True,date_format='iso')

print("Done!!! generating mock data")