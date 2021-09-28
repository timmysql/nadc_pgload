import urllib.request
import shutil
import os
import pandas as pd
import numpy as np
import glob
import fileinput
# import config
from config import config_alchemy
import db_connect as dbc



db_path = os.getcwd() + '/database_files/'

class FolderConfig:
    filename = 'nadc_data.zip'
    url = 'http://www.nebraska.gov/nadc_data/' + filename
    folder = db_path
    # folder = 'D:\\folder\\'
    download_path = folder + filename
    #
    unzip_folder = filename.replace('.zip','')
    unzip_full_path = folder + unzip_folder + '/'


def download_the_files():
    print(FolderConfig.folder)
    if not os.path.exists(FolderConfig.folder):
        os.makedirs(FolderConfig.folder)
    
    print("folder: " + FolderConfig.folder)
    print("download_path: " + FolderConfig.download_path)
    urllib.request.urlretrieve(FolderConfig.url, FolderConfig.download_path)
    # shutil.unpack_archive(filename[, extract_dir[, format]])
    shutil.unpack_archive(FolderConfig.download_path, FolderConfig.folder, 'zip')
    print('download complete')


def fix_csv_headers():
    b1_text_to_search = '|Field 22|Field 23|Field 23|Field 24|Field 25|'
    b1_replacement_text = '|Field 22|Field 23|Field 24|Field 25|'
    b_text_to_search = '|Other Info|Lobbyist ID|'
    b_replacement_text = '|Other Info|Blanks|Lobbyist ID|'
    bb_text_to_search = '|Other Info|Lobbyist ID|'
    bb_replacement_text = '|Other Info|Blanks|Lobbyist ID|'

    with fileinput.FileInput(FolderConfig.unzip_full_path + 'formb1.txt', inplace=True, backup='.bak') as file:
        for line in file:
            print(line.replace(b1_text_to_search, b1_replacement_text), end='')

    with fileinput.FileInput(FolderConfig.unzip_full_path + 'lformb.txt', inplace=True, backup='.bak') as file:
        for line in file:
            print(line.replace(b_text_to_search, b_replacement_text), end='')

    with fileinput.FileInput(FolderConfig.unzip_full_path + 'lformbb.txt', inplace=True, backup='.bak') as file:
        for line in file:
            print(line.replace(bb_text_to_search, bb_replacement_text), end='')


def csv_to_df(fp):
    data = pd.read_csv(fp, sep='|', engine='python', quoting=3)
    # df = pd.DataFrame(data, columns= ['Name','Country','Age'])

    data.columns = data.columns.str.replace(' ', '')
    data.replace([np.inf, -np.inf], np.nan, inplace=True)
    data = data.fillna("")
    return data


def insert_panda_files_to_sql():
    files = glob.glob(FolderConfig.unzip_full_path + '*.txt')
    for i in files:
        filepath = i
        table_name = i.replace(FolderConfig.unzip_full_path, '')
        table_name = table_name.replace('.txt', '')
        df = csv_to_df(filepath)
        print(filepath)

        print(table_name)
        db_engine = dbc.get_alchemy_config()
        # with dbc.db_engine.connect() as connection:
        with db_engine.connect() as connection:
            if table_name != 'DATE_UPDATED.TXT':
                try:
                    # df.to_sql(table_name, con=dbc.db_engine, if_exists='replace')
                    df.to_sql(table_name, con=db_engine, if_exists='replace')
                except Exception as e:
                    raise

def run_process(run_dl, run_fs, run_insert):
    if run_dl == 1:
        try:
            print('download_the_files')
            download_the_files()
        except Exception as e:
            raise
    if run_fs == 1:
        try:
            print('fixing strings')
            fix_csv_headers()
        except Exception as e:
            raise
    if run_insert == 1:        
        try:
            print('insert files')
            insert_panda_files_to_sql()
        except Exception as e:
            raise


if __name__ == '__main__':
    run_dl = 0
    run_fs = 0    
    run_insert = 1
    run_process(run_dl=run_dl, run_fs=run_fs, run_insert=run_insert)
