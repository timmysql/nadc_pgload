# import urllib.request
# import shutil
import os
import pandas as pd
import numpy as np
import glob
import fileinput
# import config
from db_config import config_mssql
import db_connect as dbc
import sqlalchemy
import sql_procedures_etl as etl



db_path = os.getcwd() + '\\database_files\\'

class FolderConfig:
    filename = 'nadc_data.zip'
    url = 'http://www.nebraska.gov/nadc_data/' + filename
    folder = db_path
    # folder = 'D:\\folder\\'
    download_path = folder + filename
    #
    unzip_folder = filename.replace('.zip','')
    unzip_full_path = folder + unzip_folder + '\\'


def csv_to_df(file_path):
    df = pd.read_csv(file_path, sep='|', engine='python', quoting=3, encoding='ascii')
    	
    df = df.rename(columns=str.lower)
    df.columns = df.columns.str.replace(' ','_')
    # df = df.rename(columns=str.replace(' ','_'))
    # df = pd.read_csv(file_path)
    return df


def insert_file_to_postgres(file_path, table_name):       
    print("file_path: " + file_path)
    print("table_name: " + table_name)
    df = csv_to_df(file_path)      
    
    db_engine = dbc.get_alchemy_config_postgres()
    print(db_engine)
    with db_engine.connect() as connection:
        # if table_name != 'DATE_UPDATED.TXT':
        try:
            # outputdict = sqlcol(df)
            # print(db_engine)
            df.to_sql(table_name, con=db_engine, if_exists='replace')
            # df.to_sql(table_name, con=db_engine, schema='stg', if_exists='replace')
            # dtype = outputdict)
            # dtype={sqlalchemy.types.Text()})
        except Exception as e:
            raise
# Engine(postgresql+psycopg2://osint:***@localhost/osint)

# engine = create_engine('postgresql+psycopg2://scott:tiger@localhost/mydatabase')

def loop_over_files():
    files = glob.glob(FolderConfig.unzip_full_path + '*.txt')
    for i in files:
        file_path = i
        table_name = i.replace(FolderConfig.unzip_full_path, '')
        table_name = table_name.replace('.txt', '').replace('.TXT','')
        insert_file_to_postgres(file_path=file_path, table_name=table_name)     
        # df = csv_to_df(file_path)


if __name__ == '__main__':
    # file_path = "C:\\Users\\timko\code\\nadc_pgload\database_files\\nadc_data\\commlatefile.txt"
    # table_name = "commlatefile"
    # df = csv_to_df(file_path)
    # print(df)
    # insert_file_to_postgres(file_path=file_path, table_name=table_name)
   
    loop_over_files()
