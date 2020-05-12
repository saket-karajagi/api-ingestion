import argparse
import csv
import psycopg2
import pandas as pd
from sodapy import Socrata
from config import molekule_db, api_config
from datetime import datetime

def field_builder(field_names, build_type):
    fields = ""
    last_field = field_names[-1]
    for i in range(0, len(field_names) - 1):
        field = field_names[i]
        if build_type == 'destination':
            fields += f"{field}, "
        elif build_type == 'staging':
            fields += f'"{field}" varchar, '
    if build_type == 'destination':
        fields += f"{last_field}"
    elif build_type == 'staging':
        fields += f'"{last_field}" varchar'
        
    return fields

def extract_api(url, key, dataset_id):
    date = datetime.today().strftime('%Y-%m-%d')
    client = Socrata(f"{url}", f"{key}")
    print("extracting data from NYC Open Data . .\n")
    results = client.get_all(f"{dataset_id}", content_type="csv")

    row_count = 0 
    with open(f'restaurants_{date}.csv', 'w') as f:
        writer = csv.writer(f)
        for item in results:
            if row_count != 0 and item[0] != 'camis':
                writer.writerow(item)
            elif row_count == 0 and item[0] == 'camis':
                writer.writerow(item)
            row_count += 1
    return f'restaurants_{date}.csv'

def create_staging_object(cursor, field_names, table_name):
    cursor.execute(f"""DROP TABLE IF EXISTS {table_name}_staging;""")

    fields = field_builder(field_names, build_type='staging')

    print("creating staging table\n")
    create_staging_table_sql = f"""CREATE TABLE {table_name}_staging({fields});"""
    print(create_staging_table_sql + '\n')
    cursor.execute(create_staging_table_sql)

def load_staging_object(cursor, file_name, table_name):
    load_staging_table_sql = f"""COPY {table_name}_staging from STDIN CSV HEADER;"""
    with open(file_name, 'r') as f:
        print("loading staging table\n")
        cursor.copy_expert(load_staging_table_sql, f)
        print(load_staging_table_sql + '\n')

def load_destination_object(cursor, field_names, table_name):
    trunc_destination_table_sql = f"""TRUNCATE TABLE {table_name};"""
    cursor.execute(trunc_destination_table_sql)
    print(trunc_destination_table_sql + '\n')

    fields = field_builder(field_names, build_type='destination')

    load_destination_table_sql = f"""INSERT INTO {table_name}(data_blob)\
    SELECT\
    row_to_json((SELECT d FROM (SELECT {fields}) d)) AS data\
    FROM {table_name}_staging;"""
    print("loading destination table")
    cursor.execute(load_destination_table_sql)
    print(load_destination_table_sql + '\n')

##def transform_fact_and_dimensions(cursor):
    

def main():
    parser = argparse.ArgumentParser(description='Obtain dataset name')
    parser.add_argument('-d', '--dataset_name', required=True, help='')
    args = vars(parser.parse_args())

    dataset_to_ingest = args['dataset_name']
    dataset_config = api_config[dataset_to_ingest]
    url, key, dataset_id = dataset_config['url'], dataset_config['app_key'], dataset_config['dataset_id']

    ##connector to Amazon RDS where data is ingested
    connection = psycopg2.connect(user = molekule_db['user'],
      password = molekule_db['password'],
      host = molekule_db['host'],
      port = molekule_db['port'],
      database = molekule_db['database'])

    csv_file = extract_api(url, key, dataset_id)
    
    cursor = connection.cursor()

    with open(csv_file, "r") as f:
        reader = csv.reader(f)
        field_names = next(reader)

    create_staging_object(cursor, field_names, dataset_to_ingest)
    load_staging_object(cursor, csv_file, dataset_to_ingest)
    load_destination_object(cursor, field_names, dataset_to_ingest)

    connection.commit()

if __name__ == '__main__':
    main()
