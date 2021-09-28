from sqlalchemy import create_engine
from configparser import ConfigParser, RawConfigParser
import os

# ini_file = 'database.ini'

def write_file():
    config.write(open('database.ini', 'w'))


def generate_db_ini():
    # parser = ConfigParser()
    if not os.path.exists('database.ini'):
        # config['postgresql'] = {'host': 'localhost', 'database': 'nadc', 'user': 'yourusername', 'password': 'yourpassword'}
        # write_file()
        config = RawConfigParser()

# Please note that using RawConfigParser's set functions, you can assign
# non-string values to keys internally, but will receive an error when
# attempting to write to a file or when you get it in non-raw mode. Setting
# values using the mapping protocol or ConfigParser's set() does not allow
# such assignments to take place.
        section = 'postgresql'
        config.add_section(section)
        config.set(section, 'host', 'localhost')
        config.set(section, 'database', 'nadc')
        config.set(section, 'user', 'yourusername')
        config.set(section, 'password', 'yourpassword')


# Writing our configuration file to 'example.cfg'
    with open('database.ini', 'w') as configfile:
        config.write(configfile)

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db

def config_alchemy(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        # print(type(params))
        for param in params:
            if param[0] == 'user':
                # print('found user ' + param[1])
                user = param[1]
            if param[0] == 'password':
                # print('found host ' + param[1])
                password = param[1]
            if param[0] == 'host':
                # print('found host ' + param[1])
                host = param[1]
            if param[0] == 'database':
                # print('found database ' + param[1])                                                
                database = param[1]
        db = f"""postgresql+psycopg2://{user}:{password}@{host}/{database}"""
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db    

# def get_alchemy_config():
#     engine_string = config_alchemy()
#     db_engine = create_engine(engine_string)
#     return db_engine

if __name__ == "__main__":
    generate_db_ini()
    db = config_alchemy()
    # print(db)


   