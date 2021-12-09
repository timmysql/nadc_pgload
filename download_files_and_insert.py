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




def csv_to_df(fp):
    data = pd.read_csv(fp, sep='|', engine='python', quoting=3, encoding='ascii',
    dtype={
                     'Entry Date': str,
                     'Cash Contribution': str,
                     'Amount': str,
                     'Form ID Number': str,
                     'ID': str,
                     'Amount Received': str,
                     'Amount Repaid': str,
                     'Amount Forgiven': str,
                     'Paid by 3rd Party': str,
                     'Support/Oppose': str,
                     'Microfilm Number': str,
                    'Adjustments For Cash Receipts': str,
                    'Adjustments For Cash Receipts Not Included Elsewhere': str,
                    'Amount': str,
                    'Amount Forgiven': str,
                    'Amount Paid': str,
                    'Amount Received': str,
                    'Amount Repaid': str,
                    'Amount Unpaid': str,
                    'Balance Of Public Funds': str,
                    'Cash Contribution': str,
                    'Cash Contributions This Period From All Other Sources': str,
                    'Cash Contributions This Period From Candidates Own Funds': str,
                    'Cash Contributions This Period From Other Individuals': str,
                    'Cash Disbursements For Administrative Operating Expenses': str,
                    'Cash Disbursements For Candidate And Ballot Question Committees': str,
                    'Cash DisbursementsForFederalAndOutOfStateElections': str,
                    'Cash Expenditures This Period': str,
                    'Cash Expenditures This Period2': str,
                    'Cash On Hand At Beginning Of Period': str,
                    'Cash On Hand At Close Of This Period': str,
                    'Cash Receipts This Period': str,
                    'Cash Receipts This Period From Contributors': str,
                    'Cash Receipts This Period From Loans': str,
                    'Cash Receipts This Period From Other': str,
                    'Cash Received In Payment Of Pledges Reported': str,
                    'Cash Received In Payment Of Pledges Reported In Prior Period': str,
                    'Compensation Received By Lobbyist': str,
                    'Deduct Contributions To Candidate': str,
                    'Deduct Loan Repayments': str,
                    'Deduct RefundsOfContributionsPreviouslyReported': str,
                    'Entertainment Expenses For All Other': str,
                    'Entertainment Expenses For Elected Executive Branch Officials': str,
                    'Entertainment Expenses For Elected Executive Branch Officials Period': str,
                    'Entertainment Expenses For Members Of The Legislature': str,
                    'Entertainment Expenses For Members Of The Legislature This Period': str,
                    'Expend In Numbers': str,
                    'Expenditure Amount': str,
                    'Expenditure Cash Amount': str,
                    'Expenditure In Kind Amount': str,
                    'Expenditures Of Public Funds': str,
                    'Gifts ExceptForAdmissionsForElectedExecutiveBranchOfficials': str,
                    'Gifts ExceptForAdmissionsForMembersOfLegislature': str,
                    'Gifts Of Admissions For Elected Executive Branch Officials': str,
                    'Gifts Of Admissions For Members Of Legislature': str,
                    'Gifts Of Admissions To All Others': str,
                    'Gifts Of Admissions ToElected Executive Branch Officials': str,
                    'Gifts Of Admissions To Members Of The Legislature': str,
                    'Gifts To All Others': str,
                    'Gifts To Elected Executive Branch Officials': str,
                    'Gifts To Members Of The Legislature': str,
                    'In Kind': str,
                    'In Kind And Independent Expenditures': str,
                    'In-Kind Contribution': str,
                    'In Kind Contribution': str,
                    'In Kind Contributions Received This Period': str,
                    'InKindContributionsThisPeriodFromAllOtherSources': str,
                    'InKindContributionsThisPeriodFromCandidate': str,
                    'InKindContributionsThisPeriodFromOtherIndividuals': str,
                    'InKindExpendituresThisPeriod': str,
                    'InterestAndOtherInvestmentIncomeReceivedThisPeriod': str,
                    'InterestIncome': str,
                    'Interest Paid': str,
                    'Loans Forgiven This Period': str,
                    'Loans Received This Period': str,
                    'Loans Repaid By Third Parties This Period': str,
                    'Lobbyist Compensation Paid By Lobbyist': str,
                    'Lobbyist Compensation Paid By Principal': str,
                    'Lobbyist Reimbursement Paid By Lobbyist': str,
                    'Lobbyist Reimbursement Paid By Principal': str,
                    'Lodging Expenses': str,
                    'Miscellaneous Expenses': str,
                    'Miscellaneous Expenses This Period': str,
                    'Net Cash Disbursements This Period': str,
                    'Net Cash Received This Period': str,
                    'Net Cash Receive This Period': str,
                    'Net Receipts For The Elections Year To Date': str,
                    'Net Receipts This Period': str,
                    'Office Expenses': str,
                    'Other Entertainment Expenses': str,
                    'Other Gifts Except For Admissions': str,
                    'Other Gifts Of Admissions': str,
                    'Paid': str,
                    'Paid By 3rd Party': str,
                    'Payments On Pledges Made In Prior Reporting Periods': str,
                    'Previous Contributions': str,
                    'Previous Disbursements Reported For This Election Year': str,
                    'Previous Expenditures Reported For This Election Year': str,
                    'Previous Receipts Reported For This Election Year': str,
                    'Public Funds On Hand': str,
                    'Public Funds Received': str,
                    'Reduced': str,
                    'Reimbursement Of Expenses': str,
                    'Subtotal': str,
                    'Subtotal 2': str,
                    'Total ': str,
                    'Total AllBills': str,
                    'Total Cash And Cash Equivalents At Close Of Period': str,
                    'Total Cash Disbursements': str,
                    'Total Cash Receipts': str,
                    'Total Cash Received This Period': str, 
                    'Total Contribution': str,
                    'Total Disbursements For The Election Year To Date': str,
                    'Total Disbursements For This Election Period': str,
                    'Total Entertainment Expenses': str,
                    'Total Expenditure': str,
                    'Total Expenditures For The Election Year To Date': str,
                    'Total Expenditures This Period': str,
                    'Total Gifts': str,
                    'Total Gifts Except For Admissions': str,
                    'Total Gifts Of Admissions': str,
                    'Total In Kind Contributions This Period': str,
                    'Total Of Compensation And Reimbursements': str,
                    'Total Receipts': str,
                    'Total Receipts For The Election Year To Date': str,
                    'Total Unitemized Bills': str,
                    'Total Unpaid Bills': str,
                    'Total Unpaid Pledges This Period': str,
                    'Transfer Amount': str,
                    'Travel Expenses': str,
                    'Unpaid Pledges': str,
                    'Unpaid Pledges Made This Period': str,
                    'Unpaid Pledges This Period': str,
                    'Unpaid Pledges This Period From All Other Sources': str,
                    'Unpaid Pledges This Period From Candidate': str,
                    'Unpaid Pledges This Period From Other Individuals': str,
                    'Value Of Investments Held By A Committee At Close Of Period': str,
                    'Field 1': str,
                    'Field 2A': str,
                    'Field 2B': str,
                    'Field 2C': str,
                    'Field 3': str,
                    'Field 4A': str,
                    'Field 4B': str,
                    'Field 5': str,
                    'Field 6': str,
                    'Field 7A': str,
                    'Field 7B': str,
                    'Field 7C': str,
                    'Field 7D': str,
                    'Field 8A': str,
                    'Field 8B': str,
                    'Field 8C': str,
                    'Field 8D': str,
                    'Field 9': str,
                    'Field 10': str,
                    'Field 11': str,
                    'Field 12': str,
                    'Field 13': str,
                    'Field 14': str,
                    'Field 15': str,
                    'Field 16': str,
                    'Field 17': str,
                    'Field 18': str,
                    'Field 19': str,
                    'Field 20': str,
                    'Field 21': str,
                    'Field 22': str,
                    'Field 23': str,
                    'Field 24': str,
                    'Field 25': str,
                    'Field 26': str,
                     'Field 27': str,
                    'Summary Line 1': str,
                    'Summary Line 2': str,	
                    'Summary Line 3': str,	
                    'Summary Line 4': str,	
                    'Summary Line 5A': str,	
                    'Summary Line 5B': str,	
                    'Summary Line 5C'	: str,
                    'Summary Line 6': str,
                    'Summary Line 7': str,	
                    'Summary Line 8'	: str,
                    'Summary Line 9': str,	
                    'Summary Line 10': str,	
                    'Summary Line 11A': str,
                    'Summary Line 11B': str,
                    'Summary Line 11C': str,
                    'Summary Line 11D': str,
                    'Summary Line 12A': str,
                    'Summary Line 12B': str,
                    'Summary Line 12C': str,
                    'Summary Line 12D': str,
                    'Summary Line 13': str,
                    'Previous Total': str,
                    'Total': str,
                    'Amount': str

    }
                     
    )
    # df = pd.DataFrame(data, columns= ['Name','Country','Age'])
    # data.info(verbose=True)
    data.columns = data.columns.str.replace(' ', '')
    data.replace([np.inf, -np.inf], np.nan, inplace=True)
    data = data.fillna("")
    return data

def sqlcol(dfparam):    
    
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        # print(str(i) + ' - ' + str(j))
        # print(j)
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.VARCHAR(length=4000)})

        if "int64" in str(j):
            dtypedict.update({i: sqlalchemy.types.VARCHAR(length=100)})
                                                                  
        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float64" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=100)})


        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.BigInteger})

        # if "float64" in str(j):
        #     # dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})
        #     dtypedict.update({i: sqlalchemy.types.VARCHAR(length=100)})            
            # dtypedict.update({i: sqlalchemy.types.NVARCHAR(1000)})
        # print(j)
        # if "float" in str(j):
        #     dtypedict.update({i: sqlalchemy.types.VARCHAR(length=100)})
            # dtypedict.update({i: sqlalchemy.types.NVARCHAR(1000)})            

    return dtypedict

def insert_panda_files_to_sql():
    files = glob.glob(FolderConfig.unzip_full_path + '*.txt')
    for i in files:
        filepath = i
        table_name = i.replace(FolderConfig.unzip_full_path, '')
        table_name = table_name.replace('.txt', '').replace('.TXT','')
        


        df = csv_to_df(filepath)
        # print(filepath)
        # print('--------------------------------------')
        # df.info(verbose=True)
        # df.info()
        # print(type(df))
        # print(dir(df))        

        print(table_name)
        db_engine = dbc.get_alchemy_config()

        with db_engine.connect() as connection:
            # if table_name != 'DATE_UPDATED.TXT':
            try:
                outputdict = sqlcol(df)

                # df.to_sql(table_name, con=dbc.db_engine, if_exists='replace')
                df.to_sql(table_name, con=db_engine, schema='stg', if_exists='replace',
                dtype = outputdict)
                # dtype={sqlalchemy.types.Text()})
            except Exception as e:
                raise

def test_single_file(file_name):
    files = glob.glob(FolderConfig.unzip_full_path + '*.txt')
    for i in files:
        # print(file_name)
        filepath = i
        # print(filepath)
        table_name = i.replace(FolderConfig.unzip_full_path, '')
        table_name = table_name.replace('.txt', '').replace('.TXT','')
        

        if file_name in filepath:
            print(filepath)
            df = csv_to_df(filepath)
            df.info(verbose=True)




def run_process(run_dl, run_fs, run_insert, run_etl):
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
    if run_etl == 1:        
        try:
            print('running etl')
            etl.run_etl()
        except Exception as e:
            raise        




if __name__ == '__main__':
    # test_single_file('formb73.txt')
    run_dl = 1
    run_fs = 1   
    run_insert = 1
    run_etl = 1
    run_process(run_dl=run_dl, run_fs=run_fs, run_insert=run_insert, run_etl=run_etl)
