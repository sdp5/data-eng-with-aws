import configparser
import redshift_connector
from sql_queries import count_rows_queries, five_most_playable_queries


def exec_count_rows_queries(cur, conn):
    for i, query in enumerate(count_rows_queries):
        print(f'\nQuery {i + 1}/{len(count_rows_queries)}')

        print('\n\nExecuting')
        print(query)
        cur.execute(query)
        queryset = cur.fetchall()

        for row in queryset:
            print("Number of rows: ", row)

        print('\n\n')


def exec_five_most_playable_queries(cur, conn):
    for i, item in enumerate(five_most_playable_queries):
        print(f'\nQuery {i + 1}/{len(five_most_playable_queries)}')

        print(f'\n\n{item["question"]}')
        cur.execute(item["query"])
        queryset = cur.fetchall()

        for row in queryset:
            print(row)

        print('\n\n')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to the RedShift Cluster Database')
    conn = redshift_connector.connect(
        host=config['DWH']['DWH_ENDPOINT'],
        database=config['DWH']['DWH_DB'],
        port=int(config['DWH']['DWH_PORT']),
        user=config['DWH']['DWH_DB_USER'],
        password=config['DWH']['DWH_DB_PASSWORD']
    )

    cur = conn.cursor()

    print('Count rows in each of the tables.')
    exec_count_rows_queries(cur, conn)

    print('Five most playable.')
    exec_five_most_playable_queries(cur, conn)

    print('Closing database connection.')
    conn.close()


if __name__ == "__main__":
    main()
