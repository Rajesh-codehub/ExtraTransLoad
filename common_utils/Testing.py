import json
from sqlalchemy import create_engine, Column, Integer, String, Date, MetaData, Table
from sqlalchemy.sql import text
from datetime import datetime

file_path = "/home/razz/Rajesh/ExtraTransLoad/Credentials/postgres.json"

with open(file_path, 'r') as jsondata:
    db_config = json.load(jsondata)
    try:
        # Replace these placeholders with your Microsoft SQL Server credentials
        # db_config = {
        #     'user': 'your_username',
        #     'password': 'your_password',
        #     'host': 'your_host',
        #     'database': 'your_database',
        #     'driver': 'SQL SERVER',  # Adjust the driver based on your setup
        # }

        # Specify the driver in the connection string
        connection_string = (
            f"mssql+pyodbc://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}?"
            f"driver={{{db_config['driver']}}}"
        )

        # Create a connection to the Microsoft SQL Server database using SQLAlchemy
        engine = create_engine(connection_string)

        # Define a table schema
        metadata = MetaData()

        my_table = Table(
            'example_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(255)),
            Column('company', String(255)),
            Column('date_of_birth', Date)
        )

        # Create the table
        metadata.create_all(engine)

        # Insert 5 rows of data
        data = [
            {'name': 'John Doe', 'company': 'ABC Corp', 'date_of_birth': datetime(1980, 5, 15)},
            {'name': 'Jane Smith', 'company': 'XYZ Ltd', 'date_of_birth': datetime(1992, 8, 22)},
            {'name': 'Bob Johnson', 'company': '123 Inc', 'date_of_birth': datetime(1975, 11, 8)},
            {'name': 'Alice Brown', 'company': '456 Co', 'date_of_birth': datetime(1988, 2, 18)},
            {'name': 'Charlie Williams', 'company': '789 LLC', 'date_of_birth': datetime(1995, 7, 3)},
        ]

        # Insert data into the table
        with engine.connect() as connection:
            for row in data:
                stmt = my_table.insert().values(row)
                connection.execute(stmt)

        print("Table created and data inserted successfully.")

    except Exception as e:
        print(f"Error: {e}")
