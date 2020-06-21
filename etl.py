import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
    load data to staging tables 
    by executing scrpts in sql_queries
        agr:
        cur -- cursor to the database
        conn -- connection to the database
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""
    Insert data to tables 
    by executing scrpts in sql_queries
        agr:
        cur -- cursor to the database
        conn -- connection to the database
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print("Connecting to the database")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Load data to staging tables start")
    load_staging_tables(cur, conn)
    print("Load data to staging tables end")
    
    print("Inert data to tables start")
    insert_tables(cur, conn)
    print("Inert data to tables end")
    
    conn.close()
    print("Close Connection")

if __name__ == "__main__":
    main()