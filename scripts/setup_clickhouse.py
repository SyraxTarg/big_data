""" ce script ajoute les données des CSV dans la base clickhouse."""

import clickhouse_connect
import json
import pandas as pd
import csv
import os
from datetime import datetime
import logging
from dotenv import load_dotenv

load_dotenv()

client = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST"),
    port=os.getenv("CLICKHOUSE_PORT"),
    username=os.getenv("CLICKHOUSE_USERNAME"),
    password=os.getenv("CLICKHOUSE_USERNAME")
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("ADDING DATA TO CLICKHOUSE")

try:

    logging.info("ADDING STAYS")
    directory = "file_storage/sejours"
    for date_dir in os.listdir(f'{directory}'):
            sejours = []
            for file in os.listdir(f'{directory}/{date_dir}'):

                table_name_sejours = f"sejours_{date_dir.replace('-', '_')}"
                with open(f'{directory}/{date_dir}/{file}') as file_obj:

                    reader_obj_sejours = csv.reader(file_obj)
                    for row in reader_obj_sejours:
                        if row[3] != '' and row[3] != 'admission_ts':
                            row[3] = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S")
                        if row[4] != '' and row[4] != 'discharge_ts':
                            row[4] = datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S")
                        elif row[4] == '':
                            row[4] = None
                        sejours.append(row)
                    sejours.pop(0)

                # table bronze
                client.command(f'DROP TABLE IF EXISTS chu.{table_name_sejours}_bronze')
                client.command(f'''
                    CREATE TABLE IF NOT EXISTS chu.{table_name_sejours}_bronze (
                        stay_id String,
                        patient_id String,
                        service_code String,
                        admission_ts DateTime,
                        discharge_ts Nullable(DateTime),
                        admission_mode String,
                        discharge_mode String
                    )
                    ENGINE = MergeTree()
                    ORDER BY stay_id
                ''')
                client.insert(f'chu.{table_name_sejours}_bronze', sejours)


                # table silver
                client.command(f'DROP TABLE IF EXISTS chu.{table_name_sejours}_silver')
                client.command(f'''
                    CREATE TABLE IF NOT EXISTS chu.{table_name_sejours}_silver (
                        stay_id String,
                        patient_id String,
                        service_code String,
                        admission_ts DateTime,
                        discharge_ts Nullable(DateTime),
                        admission_mode String,
                        discharge_mode String
                    )
                    ENGINE = MergeTree()
                    ORDER BY stay_id
                ''')
                client.insert(f'chu.{table_name_sejours}_silver', sejours)



    logging.info("ADDING PATIENTS")
    directory = "file_storage/patients_clean"
    for date_dir in os.listdir(f'{directory}'):
            patients = []
            for file in os.listdir(f'{directory}/{date_dir}'):

                table_name_patients = f"patients_{date_dir.replace('-', '_')}"
                with open(f'{directory}/{date_dir}/{file}') as file_obj:

                    reader_obj_patients = csv.reader(file_obj)
                    for row in reader_obj_patients:
                        row.remove(row[-1])
                        if row[1] != "birth_date":
                            row[1] = datetime.strptime(row[1], "%Y-%m-%d")
                        patients.append(row)
                    patients.pop(0)

                # table bronze
                client.command(f'DROP TABLE IF EXISTS chu.{table_name_patients}_bronze')
                client.command(f'''
                    CREATE TABLE IF NOT EXISTS chu.{table_name_patients}_bronze (
                        patient_id String,
                        birth_date Date32,
                        sex String,
                    )
                    ENGINE = MergeTree()
                    ORDER BY patient_id
                ''')
                client.insert(f'chu.{table_name_patients}_bronze', patients)


                # table silver
                client.command(f'DROP TABLE IF EXISTS chu.{table_name_patients}_silver')
                client.command(f'''
                    CREATE TABLE IF NOT EXISTS chu.{table_name_patients}_silver (
                        patient_id String,
                        birth_date Date32,
                        sex String,
                    )
                    ENGINE = MergeTree()
                    ORDER BY patient_id
                ''')
                client.insert(f'chu.{table_name_patients}_silver', patients)


    logging.info("ADDING REFERENTIALS")
    directory = "file_storage/referentiels"
    for date_dir in os.listdir(f'{directory}'):
            cim10 = []
            services = []
            for file in os.listdir(f'{directory}/{date_dir}'):

                table_name_cim10 = f"cim10_{date_dir.replace('-', '_')}"
                table_name_services = f"services_{date_dir.replace('-', '_')}"
                with open(f'{directory}/{date_dir}/{file}') as file_obj:
                    reader_obj_referentiels = csv.reader(file_obj)
                    for row in reader_obj_referentiels:
                        if file == "cim10.csv":
                            cim10.append(row)
                        if file == "services.csv":
                            services.append(row)
                if file == "cim10.csv":
                    cim10.pop(0)


                    # table bronze
                    client.command(f'DROP TABLE IF EXISTS chu.{table_name_cim10}_bronze')
                    client.command(f'''
                        CREATE TABLE IF NOT EXISTS chu.{table_name_cim10}_bronze (
                            code_cim10 String,
                            libelle String,
                        )
                        ENGINE = MergeTree()
                        ORDER BY code_cim10
                    ''')
                    client.insert(f'chu.{table_name_cim10}_bronze', cim10)


                    # table silver
                    client.command(f'DROP TABLE IF EXISTS chu.{table_name_cim10}_silver')
                    client.command(f'''
                        CREATE TABLE IF NOT EXISTS chu.{table_name_cim10}_silver (
                            code_cim10 String,
                            libelle String,
                        )
                        ENGINE = MergeTree()
                        ORDER BY code_cim10
                    ''')
                    client.insert(f'chu.{table_name_cim10}_silver', cim10)

                if file == "services.csv":
                    services.pop(0)


                    # table bronze
                    client.command(f'DROP TABLE IF EXISTS chu.{table_name_services}_bronze')
                    client.command(f'''
                        CREATE TABLE IF NOT EXISTS chu.{table_name_services}_bronze (
                            service_code String,
                            service_label String,
                        )
                        ENGINE = MergeTree()
                        ORDER BY service_code
                    ''')
                    client.insert(f'chu.{table_name_services}_bronze', services)


                    # table silver
                    client.command(f'DROP TABLE IF EXISTS chu.{table_name_services}_silver')
                    client.command(f'''
                        CREATE TABLE IF NOT EXISTS chu.{table_name_services}_silver (
                            service_code String,
                            service_label String,
                        )
                        ENGINE = MergeTree()
                        ORDER BY service_code
                    ''')
                    client.insert(f'chu.{table_name_services}_silver', services)


    logging.info("ADDING DIAGNOSTICS")
    directory = "file_storage/diagnostics"
    for date_dir in os.listdir(f'{directory}'):
            diagnostics = []
            for file in os.listdir(f'{directory}/{date_dir}'):

                table_name_diagnostics = f"diagnostics_{date_dir.replace('-', '_')}"
                with open(f'{directory}/{date_dir}/{file}') as file_obj:

                    diag = json.load(file_obj)
                    for r in diag:
                        stay_id = r.get("stay_id")
                        diagnostics_json = r.get("diagnostics")
                        for diag in diagnostics_json:
                            diagnostics.append([stay_id, diag.get("code_cim10"), diag.get("type")])


                # table bronze
                client.command(f'DROP TABLE IF EXISTS chu.{table_name_diagnostics}_bronze')
                client.command(f'''
                    CREATE TABLE IF NOT EXISTS chu.{table_name_diagnostics}_bronze (
                        stay_id String,
                        code_cim10 String,
                        type String
                    )
                    ENGINE = MergeTree()
                    ORDER BY stay_id
                ''')
                client.insert(f'chu.{table_name_diagnostics}_bronze', diagnostics)


                # table silver
                client.command(f'DROP TABLE IF EXISTS chu.{table_name_diagnostics}_silver')
                client.command(f'''
                    CREATE TABLE IF NOT EXISTS chu.{table_name_diagnostics}_silver (
                        stay_id String,
                        code_cim10 String,
                        type String
                    )
                    ENGINE = MergeTree()
                    ORDER BY stay_id
                ''')
                client.insert(f'chu.{table_name_diagnostics}_silver', diagnostics)


    logging.info("ADDING MONITORING")
    directory = "file_storage/monitoring"
    for date_dir in os.listdir(f'{directory}'):
            monitorings = []
            for file in os.listdir(f'{directory}/{date_dir}'):

                table_name_monitoring = f"monitoring_{date_dir.replace('-', '_')}"
                parquet_file = pd.read_parquet(f'{directory}/{date_dir}/monitoring.parquet')

                for i in range(len(parquet_file)):
                    monitorings.append([parquet_file.get("stay_id")[i], parquet_file.get("ts")[i], parquet_file.get("heart_rate")[i], parquet_file.get("spo2")[i], parquet_file.get("temp_c")[i]])


                # table bronze
                client.command(f'DROP TABLE IF EXISTS chu.{table_name_monitoring}_bronze')
                client.command(f'''
                    CREATE TABLE IF NOT EXISTS chu.{table_name_monitoring}_bronze (
                        stay_id String,
                        ts date,
                        heart_rate Int,
                        spo2 Int,
                        temp_c Float
                    )
                    ENGINE = MergeTree()
                    ORDER BY stay_id
                ''')
                client.insert(f'chu.{table_name_monitoring}_bronze', monitorings)


                # table silver
                client.command(f'DROP TABLE IF EXISTS chu.{table_name_monitoring}_silver')
                client.command(f'''
                    CREATE TABLE IF NOT EXISTS chu.{table_name_monitoring}_silver (
                        stay_id String,
                        ts date,
                        heart_rate Int,
                        spo2 Int,
                        temp_c Float
                    )
                    ENGINE = MergeTree()
                    ORDER BY stay_id
                ''')
                client.insert(f'chu.{table_name_monitoring}_silver', monitorings)

except Exception as e:
    logging.error(f'AN ERROR HAPPENED: {e}')

logging.info("DATA SUCCESSFULLY ADDED")