import os
import urllib.request
import shutil
import fileinput

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
    # Data last loaded: 
    date_updated_search = 'Data last loaded: '
    date_updated_replace = 'DataLastLoaded' + '\n'

    with fileinput.FileInput(FolderConfig.unzip_full_path + 'DATE_UPDATED.TXT', inplace=True, backup='.bak') as file:
        for line in file:
            print(line.replace(date_updated_search, date_updated_replace), end='')    

    with fileinput.FileInput(FolderConfig.unzip_full_path + 'formb1.txt', inplace=True, backup='.bak') as file:
        for line in file:
            print(line.replace(b1_text_to_search, b1_replacement_text), end='')

    with fileinput.FileInput(FolderConfig.unzip_full_path + 'lformb.txt', inplace=True, backup='.bak') as file:
        for line in file:
            print(line.replace(b_text_to_search, b_replacement_text), end='')

    with fileinput.FileInput(FolderConfig.unzip_full_path + 'lformbb.txt', inplace=True, backup='.bak') as file:
        for line in file:
            print(line.replace(bb_text_to_search, bb_replacement_text), end='')



if __name__ == '__main__':
    # test_single_file('formb73.txt')
    download_the_files()
    fix_csv_headers()
    
