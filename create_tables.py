import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all tables listed in drop_table_queries list

    Arguments:
        cur: DB cursor
        conn: DB connect session
    Return: None  
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all tables listed in create_Table_queries list 

    Arguments:
        cur: DB cursor
        conn: DB connect session
    Return: None  
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function which performs
      1. Read in configuration values
      2. Make DB connection and a cursor
      3. Drop all tables by calling drop_tables() function
      4. Create all tables by calling create_tables() function
      5. Close DB connection

    Arguments: None
    Return: None  
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    #print('Start dropping existing tables')
    drop_tables(cur, conn)
    #print('Dropping tables finished')
    #print('Start creating tables')
    create_tables(cur, conn)
    #print('Creating tables finished')

    conn.close()


if __name__ == "__main__":
    main()