from sqlalchemy import create_engine
import config as cfg
# import pyodbc 


def get_alchemy_config():
    engine_string = cfg.config_mssql()
    db_engine = create_engine(engine_string)
    return db_engine

if __name__ == "__main__":
    get_alchemy_config()

