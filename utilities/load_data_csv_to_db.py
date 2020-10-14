from configparser import ConfigParser
# import pyodbc
import psycopg2
import pandas as pd


def convert_excel_to_csv(input_file_name, sheet_name, output_file_name):
    df = pd.read_excel(input_file_name, sheet_name=sheet_name)
    print(df.head(5))
    df.to_csv(output_file_name, index=False, header=True)


def connect(server, db_name):
    conn = None
    try:
        # conn = pyodbc.connect('Trusted_Connection=yes', driver='{SQL Server}', server=server, database=db_name)
        conn = psycopg2.connect(host=server, username='postgres', password='admin', dbname=db_name, port=5432)
    except psycopg2.DatabaseError as e:
        print('Failed to connect with error {}'.format(e))
    return conn


def extract_schema(file_name, delimiter, table_name, chunk_size=100):
    sample_df = pd.read_csv(file_name, delimiter, nrows=chunk_size)
    cols = sample_df.columns
    cols = [col.replace(' ', '_') for col in cols]
    data_type = 'varchar(500)'
    create_query = 'if not exists (select * from sysobjects where name=\'' + table_name + '\' and xtype=\'U\') create table dbo.' + table_name + ' (' + ','.join(
        [col + ' ' + data_type for col in cols]) + ')'
    insert_query = 'insert into dbo.' + table_name + '(' + ','.join(cols) + ') values (' + ','.join(
        ['?'] * len(cols)) + ')'

    return create_query, insert_query


def write_to_db(conn, file_name, delimiter, table_name, create_query, insert_query, chunk_size=None):
    """
    load everything once when chunk_size is None
    """
    # set up table
    if conn:
        cursor = conn.cursor()
        print('creating table {} if not exist....'.format(table_name))
        cursor.execute(create_query)
        truncate_query = 'Truncate table dbo.' + table_name
        cursor.execute(truncate_query)
        conn.commit()
    else:
        print('connection is not established')

    # insert into table
    chunk_i = 0
    cursor.fast_executemany = True
    for chunk in pd.read_csv(file_name, delimiter, chunksize=chunk_size, dtype='object'):
        chunk_i += 1
        print('loading chuck {}'.format(chunk_i))
        chunk.fillna('', inplace=True)
        # print(chunk.info())
        values = chunk.values.tolist()
        tuple_of_tuples = tuple(tuple(x) for x in values)
        cursor.executemany(insert_query, tuple_of_tuples)
        conn.commit()


def load_data_from_csv_to_db(server, db_name, file_name, table_name, delimiter, chunk_size1=100, chunk_size2=None):
    """
    :param: server - string - destination server name
    :param: db_name - string - destination database name
    :param: table_name - string - destination table name
    :param: delimiter - string - source file delimiter
    :param: chunk_size1 - int - chunk size used to extract schema
    :param: chunk_size2 - int - chunk size used to read data from source file
    """
    create_query, insert_query = extract_schema(file_name, delimiter, table_name, chunk_size=chunk_size1)
    conn = connect(server, db_name)
    write_to_db(conn, file_name, delimiter, table_name, create_query, insert_query, chunk_size=chunk_size2)
    if conn:
        conn.close()
