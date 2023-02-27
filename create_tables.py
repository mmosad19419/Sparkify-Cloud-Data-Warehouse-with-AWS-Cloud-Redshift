#!/usr/bin/env python

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema, direct_queries

def drop_tables(cur, conn):
    """
    drop tables using the drop tables queries in "drop_table_queries" list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    create tables in the database using the queries in "create_tables_queries" list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """

    # get config parameters
    config = configparser.ConfigParser()
    config.read_file(open("cluster_config.cfg", encoding="utf-8"))

    HOST = config.get("CLUSTER", "HOST").replace('"', "")
    DB_NAME = config.get("CLUSTER", "DB_NAME").replace('"', "")
    DB_USER = config.get("CLUSTER", "DB_USER").replace('"', "")
    DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD").replace('"', "")
    DB_PORT = config.get("CLUSTER", "DB_PORT").replace('"', "")


    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"\
                            .format(HOST, DB_NAME, DB_USER, DB_PASSWORD, int(DB_PORT)))
    
    cur = conn.cursor()

    # CREATE SCHEMA
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

    print("Tables Created Successfully")



if __name__ == "__main__":
    main()