from sqlalchemy import create_engine
import db_config as cfg
# import pyodbc 


def get_alchemy_config_postgres():
    engine_string = cfg.config_postgres()
    db_engine = create_engine(engine_string)
    return db_engine



def get_alchemy_config_sql():
    engine_string = cfg.config_mssql()
    db_engine = create_engine(engine_string)
    return db_engine

if __name__ == "__main__":
    sql = get_alchemy_config_sql()
    pg = get_alchemy_config_postgres()
    print(sql)
    print(pg)
    

