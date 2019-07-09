import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load original datasets into staging tables 

    Arguments:
        cur: DB cursor
        conn: DB connect session
    Return: None  
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transform staging data and insert into DW tables 

    Arguments:
        cur: DB cursor
        conn: DB connect session
    Return: None  
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function which performs
      1. Read in configuration values
      2. Make DB connection and a cursor
      3. Load S3 dataset into staging tables by calling load_staging_tables() function
      4. Do ETL of staging data into DW tables by calling insret_tables() function
      5. Close DB connection

    Arguments: None
    Return: None  
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #print('Start loading S3 data into staging tables')
    load_staging_tables(cur, conn)
    #print('Staging tables created and start inserting staging data into star schema tables')
    insert_tables(cur, conn)
    #print('Inserting into star schema tables finished')

    conn.close()


if __name__ == "__main__":
    main()