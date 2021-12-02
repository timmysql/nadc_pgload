import download_files_and_insert as dfi
import config as cfg
import etl_sequence as etl

if __name__ == '__main__':  
    
    # you will need to create a database.ini file
    # here's some help... run this and enter the configs once generated
    # cfg.generate_db_ini()

    # set these to 1 or 0 depending on if you want them to execute
    run_dl = 0 # download nadc files to database_files directory
    run_fs = 0 # fix the headers   p
    run_insert = 1 # insert files into postgres > nadc database

    # #
    etl.sql_truncate_stage()
    dfi.run_process(run_dl=run_dl, run_fs=run_fs, run_insert=run_insert)