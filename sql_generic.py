import psycopg2
from config import config
# import db_connect as dbc

### send some sql to these so you don't have to write so much code... ugh


 
def pg_return_dataset(pg_sql):
        
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        # connect to the PostgreSQL server
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)		
        # create a cursor
        cur = conn.cursor()       
            # print(pg_sql)
        cur.execute(pg_sql) 
        dataset = cur.fetchall()         
        conn.commit()
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            
            conn.close()
            # print('Database connection closed.')
    return dataset


def pg_return_scalar(pg_sql):
    # value you wish to return must be the first column in the select list 
    # sql should be written as a limit 1, or an aggregate  that returns a single row
    # may work to return the 1st row in the dataset... need to test that    
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)		
        # create a cursor
        cur = conn.cursor()       
            # print(pg_sql)
        cur.execute(pg_sql) 
        scalar = cur.fetchone()         
        conn.commit()
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            
            conn.close()
            # print('Database connection closed.')
    return scalar[0]

def pg_execute(pg_sql):    
    # user = auth.api.get_user(lookup_name)
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)		
        # create a cursor
        cur = conn.cursor()       
            # print(pg_sql)
        cur.execute(pg_sql)                
        conn.commit()
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            
            conn.close()
            # print('Database connection closed.')
            
            
   

if __name__ == '__main__':
    pg_sql = f"""SELECT * FROM commlatefile;"""
    dataset = pg_return_dataset(pg_sql)
    print(dataset)

