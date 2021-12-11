import sql_generic as sql
import db_connect as dbc

sql.pg_execute

def sql_truncate_stage():
    db_engine = dbc.get_alchemy_config()
    with db_engine.connect().execution_options(autocommit=True) as connection:
        # if table_name != 'DATE_UPDATED.TXT':
        try:
            print('trying spLoad_TruncateStage')
            connection.execute("EXECUTE stg.spLoad_TruncateStage")
            # df.to_sql(table_name, con=dbc.db_engine, if_exists='replace')
            # df.to_sql(table_name, con=db_engine, schema='stg', chunksize=5000, if_exists='append',dtype={"EntryDate": sqlalchemy.types.Text()})
        except Exception as e:
            raise


def run_etl():
    db_engine = dbc.get_alchemy_config()
    with db_engine.connect().execution_options(autocommit=True) as connection:
        # if table_name != 'DATE_UPDATED.TXT':
        try:
            print('trying spLoad_ALL_MASTER')
            connection.execute("EXECUTE dw.spLoad_ALL_MASTER")
            # df.to_sql(table_name, con=dbc.db_engine, if_exists='replace')
            # df.to_sql(table_name, con=db_engine, schema='stg', chunksize=5000, if_exists='append',dtype={"EntryDate": sqlalchemy.types.Text()})
        except Exception as e:
            raise        

def run_fact_master_etl():
    db_engine = dbc.get_alchemy_config()
    with db_engine.connect().execution_options(autocommit=True) as connection:
        # if table_name != 'DATE_UPDATED.TXT':
        try:
            print('trying spLoad_Fact_MASTER')
            connection.execute("EXECUTE dw.spLoad_Fact_MASTER")
            # df.to_sql(table_name, con=dbc.db_engine, if_exists='replace')
            # df.to_sql(table_name, con=db_engine, schema='stg', chunksize=5000, if_exists='append',dtype={"EntryDate": sqlalchemy.types.Text()})
        except Exception as e:
            raise        


def run_fact_contributions_etl():
    db_engine = dbc.get_alchemy_config()
    with db_engine.connect().execution_options(autocommit=True) as connection:
        # if table_name != 'DATE_UPDATED.TXT':
        try:
            print('trying spLoad_FactContributions')
            connection.execute("EXEC dw.spLoad_FactContributions")
            # df.to_sql(table_name, con=dbc.db_engine, if_exists='replace')
            # df.to_sql(table_name, con=db_engine, schema='stg', chunksize=5000, if_exists='append',dtype={"EntryDate": sqlalchemy.types.Text()})
        except Exception as e:
            raise    

# EXEC dw.spLoad_FactContributions


if __name__ == "__main__":
    run_fact_contributions_etl()
    # sql_truncate_stage()
