import sql_generic as sql
import db_connect as dbc

sql.pg_execute

def sql_truncate_stage():
    db_engine = dbc.get_alchemy_config()
    with db_engine.connect() as connection:
        # if table_name != 'DATE_UPDATED.TXT':
        try:
            connection.execute("EXECUTE stg.spLoad_TruncateStage")
            # df.to_sql(table_name, con=dbc.db_engine, if_exists='replace')
            # df.to_sql(table_name, con=db_engine, schema='stg', chunksize=5000, if_exists='append',dtype={"EntryDate": sqlalchemy.types.Text()})
        except Exception as e:
            raise


def run_etl():
    db_engine = dbc.get_alchemy_config()
    with db_engine.connect() as connection:
        # if table_name != 'DATE_UPDATED.TXT':
        try:
            connection.execute("EXECUTE dw.spLoad_ALL_MASTER")
            # df.to_sql(table_name, con=dbc.db_engine, if_exists='replace')
            # df.to_sql(table_name, con=db_engine, schema='stg', chunksize=5000, if_exists='append',dtype={"EntryDate": sqlalchemy.types.Text()})
        except Exception as e:
            raise        



if __name__ == "__main__":
    sql_truncate_stage()
