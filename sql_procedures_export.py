import sql_generic
import db_connect as dbc
import pandas as pd

db_engine = dbc.get_alchemy_config()  

# cnxn = pyodbc.connect(
#     Trusted_Connection='No',
#     Driver='{ODBC Driver 17 for SQL Server}',
#     Server=server,
#     Database=database,
#     UID=username,
#     PWD=password
# )


# def return_df(tsql):    
#     cursor = cnxn.cursor()

#     # -----------------------------------------------
#     # tsql = f"""SELECT * FROM ne_radio;"""
#     # cursor.execute(sql) 
#     # data = cursor.fetchall() 
#     # df = psql.frame_query(tsql, cnxn)
#     df = pd.read_sql(tsql, cnxn)
#     # print(data)
#     # print(dict(data))
#     # while row: 
#     #     print(row[0])
#     #     row = cursor.fetchall()
#     cursor.close()
#     cnxn.close()
#     return df


def return_df(tsql):        
    with db_engine.connect() as connection:
        # if table_name != 'DATE_UPDATED.TXT':
        try:
            # outputdict = sqlcol(df)
            # tsql = f"""SELECT * FROM ne_radio;"""
            # df.to_sql(table_name, con=dbc.db_engine, if_exists='replace')
            df = pd.read_sql(tsql, con=db_engine)
            # print(df)
        except Exception as e:
            raise           
    return df  
  
  
def export_table_as_csv(database, schema, table_name):
    tsql = f"""SELECT * FROM [{database}].[{schema}].[{table_name}]"""
    
    df = return_df(tsql)
    df.to_csv(f"""C:\\Users\\timko\code\\nadc_pgload\\export_csv_{database}\{table_name}.csv""",chunksize=1000)
    print(df)

def export_dim_contributors():
    tsql = f"""SELECT [ContributorEntityKey]
      ,[ContributorID]
      ,[EntityPrefix]
      ,[EntityType]
      ,[ContributorName]
      ,[IsNotRegistered]
      ,[DoNotUse]
      ,[IsDissolved]
      ,[FullName]
      ,[LastName]
      ,[FirstName]
      ,[MiddleInitial]
      ,[ContributorAddress]
      ,[ContributorCity]
      ,[ContributorState]
      ,[ContributorZip]
      ,[IsContributingEntity]
      ,[IsRecipientEntity]
      ,[SupportCandidateEntityKey]
      ,[IsCurrentExecutiveBranch]
      ,[ContributorTypeGroup]
      ,[ContributorTypeGroupDesc]
  FROM [nadc].[dw].[vDimContributors]"""
    
    df = return_df(tsql)
    df.to_csv('contributors.csv',chunksize=10000)
    # print(df)


if __name__ == '__main__':
    # NADC_STAGE
    export_table_as_csv(database='nadc_flatwater', schema='dw', table_name='DimCandidates')
    export_table_as_csv(database='nadc_flatwater', schema='dw', table_name='DimCommittees')
    export_table_as_csv(database='nadc_flatwater', schema='dbo', table_name='vDimContributors')
    export_table_as_csv(database='nadc_flatwater', schema='dbo', table_name='vDimRecipients')
    export_table_as_csv(database='nadc_flatwater', schema='dw', table_name='DimDate')
    export_table_as_csv(database='nadc_flatwater', schema='dw', table_name='DimEntities')
    export_table_as_csv(database='nadc_flatwater', schema='dw', table_name='DimSupportOppose')
    export_table_as_csv(database='nadc_flatwater', schema='dw', table_name='DimTransactionType')    
    export_table_as_csv(database='nadc_flatwater', schema='ffp',table_name='vFactContributions')
    # export_table_as_csv(table_name='FactExpenditures')
    # export_dim_contributors()  
          