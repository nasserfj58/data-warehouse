import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
   drop all tables 
    by executing scrpts in sql_queries
        agr:
        cur -- cursor to the database
        conn -- connection to the database
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""
   drop all tables 
    by executing scrpts in sql_queries
        agr:
        cur -- cursor to the database
        conn -- connection to the database
    """
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print("connecting to the database")
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}"
        .format(*config['CLUSTER'].values())
        )
    
    cur = conn.cursor()
    
    print("Drop tables start")
    drop_tables(cur, conn)
    print("Drop tables end")
    
    print("Create tables start")
    create_tables(cur, conn)
    print("Create tables end")
    
    conn.close()
    print("Close Connection")

if __name__ == "__main__":
    main()
