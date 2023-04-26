import os
import tempfile
import pandas as pd
import requests
import datetime
import psycopg2
import os
from sqlalchemy import create_engine
from sqlalchemy import text
import dotenv, os
from dotenv import dotenv_values

def get_database_conn():
    dotenv.load_dotenv(r"C:\Users\NGSL0161\Desktop\Data Engineering class\projects\environmental variables\.env")
    db_user_name = os.getenv('DB_USER_NAME')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    port = os.getenv('PORT')
    host = os.getenv('HOST')
    return db_user_name, db_password, db_name, port, host

# Data Extraction layer
def extract_data():
    url = "https://drive.google.com/file/d/1SzmRIwlpL5PrFuaUe_1TAcMV0HYHMD_b/view"
    file_id = url.split('/')[5]
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "data.csv")
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        df = pd.read_csv(file_path)
    df.to_csv('Covid_19/data/data.csv', index=False)
    print("data extracted successfully")


# Data transformation layer
def transform_data():
    df = pd.read_csv('Covid_19/data/data.csv')
    df['ObservationDate'] =  pd.to_datetime(df['ObservationDate'], infer_datetime_format=True)
    df.to_csv('Covid_19/data/transformed.csv', index=False)
    return df


# Data loading layer
def load_data(db_user_name, db_password, db_name, port, host):
    engine = create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@localhost/postgres')

        

    # Create covid_data database using the engine object
    with engine.connect() as connection:  # We use the "WITH" context manager to automatically close connection
        try:  # A try block to catch any error creating the new database
        # Remove transaction block to run DROP DATABASE
            connection.execute(text("COMMIT"))
            connection.execute(text(f'DROP DATABASE IF EXISTS {db_name}'))
            connection.execute(text("COMMIT"))
            connection.execute(text(f'CREATE DATABASE {db_name}'))
        except psycopg2.OperationalError as error:
            print(error)
        print("database created successfully")

    # Create a connection engine to the covid_19_data database using the psycorg2 function.
    conn = psycopg2.connect(f'dbname={db_name} user={db_user_name} password={db_password} host={host} port={port}')
    cur = conn.cursor()

   
    # SQL query for creating the table for holding the extracted data
    cur.execute("""
                CREATE TABLE IF NOT EXISTS covid_19_data (
                SNo INT,
                ObservationDate DATE,
                Province VARCHAR(255),
                Country VARCHAR(255),
                LastUpdate VARCHAR(255),
                Confirmed INT,
                Deaths INT,
                Recovered INT,
                PRIMARY KEY (SNO))
    """)

    with open('Covid_19/data/transformed.csv', 'r') as f:
        next(f) # Skip the header row.
        cur.copy_from(f, 'covid_19_data', sep=',')

    conn.commit()
    print('Data sucessfully loaded to database!')


def main():
    db_user_name, db_password, db_name, port, host = get_database_conn()
    # Determin if there is a scrapped data. If this is True, the data is transformed and loaded.
    # If data does not exist, the data is extracted, transformed and loaded.
    if os.path.exists('Covid_19/data/data.csv'):
        transform_data()
        load_data(db_user_name, db_password, db_name, port, host)
    else:
        extract_data()
        transform_data()
        load_data(db_user_name, db_password, db_name, port, host)


# Execute the script by invoking the main method
main()
