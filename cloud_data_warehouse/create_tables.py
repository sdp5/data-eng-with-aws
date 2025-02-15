import configparser
import psycopg2
import redshift_connector
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop any existing tables."""

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create new tables."""

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Create tables entry point."""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn = redshift_connector.connect(
        host=config['DWH']['DWH_ENDPOINT'],
        database=config['DWH']['DWH_DB'],
        port=int(config['DWH']['DWH_PORT']),
        user=config['DWH']['DWH_DB_USER'],
        password=config['DWH']['DWH_DB_PASSWORD']
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
