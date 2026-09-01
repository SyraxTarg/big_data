""" ce script ajoute les données des CSV dans la base clickhouse."""

import json
import pandas as pd
import csv
import os
from datetime import datetime
import logging
from big_data.clickhouse_config.clickhouse import client as clickhouse_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("ADDING DATA TO CLICKHOUSE")

def main():
    try:
        logging.info("ADDING STAYS")
        directory = "lake/sejours"
        # table bronze
        clickhouse_client.command(f'DROP TABLE IF EXISTS chu.sejours_bronze')
        clickhouse_client.command(f'''
            CREATE TABLE IF NOT EXISTS chu.sejours_bronze (
                stay_id String,
                patient_id String,
                service_code String,
                admission_ts DateTime,
                discharge_ts Nullable(DateTime),
                inserted_at DateTime,
                data_path String
            )
            ENGINE = MergeTree()
            ORDER BY stay_id
        ''')
        for date_dir in os.listdir(f'{directory}'):
                sejours = []
                for file in os.listdir(f'{directory}/{date_dir}'):
                    with open(f'{directory}/{date_dir}/{file}') as file_obj:
                        reader_obj_sejours = csv.reader(file_obj)
                        for row in reader_obj_sejours:
                            if row[3] != '' and row[3] != 'admission_ts':
                                row[3] = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S")
                            if row[4] != '' and row[4] != 'discharge_ts':
                                row[4] = datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S")
                            elif row[4] == '':
                                row[4] = None
                            del row[-2:]
                            row.append(datetime.now())
                            row.append(f'{directory}/{date_dir}/{file}')
                            sejours.append(row)
                        sejours.pop(0)

                    clickhouse_client.insert(f'chu.sejours_bronze', sejours)



        logging.info("ADDING PATIENTS")
        directory = "lake/patients"
        # table bronze
        clickhouse_client.command(f'DROP TABLE IF EXISTS chu.patients_bronze')
        clickhouse_client.command(f'''
            CREATE TABLE IF NOT EXISTS chu.patients_bronze (
                patient_id String,
                birth_date Date32,
                sex String,
                inserted_at DateTime,
                data_path String
            )
            ENGINE = MergeTree()
            ORDER BY patient_id
        ''')

        for date_dir in os.listdir(f'{directory}'):
                patients = []
                for file in os.listdir(f'{directory}/{date_dir}'):
                    with open(f'{directory}/{date_dir}/{file}') as file_obj:
                        reader_obj_patients = csv.reader(file_obj)
                        for row in reader_obj_patients:
                            row.remove(row[-1])
                            if row[1] != "birth_date":
                                row[1] = datetime.strptime(row[1], "%Y-%m-%d")
                            row.append(datetime.now())
                            row.append(f'{directory}/{date_dir}/{file}')
                            patients.append(row)
                        patients.pop(0)
                    clickhouse_client.insert(f'chu.patients_bronze', patients)


        logging.info("ADDING REFERENTIALS")
        directory = "lake/referentiels"

        # table bronze
        clickhouse_client.command(f'DROP TABLE IF EXISTS chu.cim10_bronze')
        clickhouse_client.command(f'''
            CREATE TABLE IF NOT EXISTS chu.cim10_bronze (
                code_cim10 String,
                libelle String,
                inserted_at DateTime,
                data_path String
            )
            ENGINE = MergeTree()
            ORDER BY code_cim10
        ''')


        # table bronze
        clickhouse_client.command(f'DROP TABLE IF EXISTS chu.services_bronze')
        clickhouse_client.command(f'''
            CREATE TABLE IF NOT EXISTS chu.services_bronze (
                service_code String,
                service_label String,
                inserted_at DateTime,
                data_path String
            )
            ENGINE = MergeTree()
            ORDER BY service_code
        ''')


        for date_dir in os.listdir(f'{directory}'):
                cim10 = []
                services = []
                for file in os.listdir(f'{directory}/{date_dir}'):
                    with open(f'{directory}/{date_dir}/{file}') as file_obj:
                        reader_obj_referentiels = csv.reader(file_obj)
                        for row in reader_obj_referentiels:
                            if file == "cim10.csv":
                                row.append(datetime.now())
                                row.append(f'{directory}/{date_dir}/{file}')
                                cim10.append(row)
                            if file == "services.csv":
                                row.append(datetime.now())
                                row.append(f'{directory}/{date_dir}/{file}')
                                services.append(row)
                    if file == "cim10.csv":
                        cim10.pop(0)
                        clickhouse_client.insert(f'chu.cim10_bronze', cim10)

                    if file == "services.csv":
                        services.pop(0)
                        clickhouse_client.insert(f'chu.services_bronze', services)


        logging.info("ADDING DIAGNOSTICS")
        directory = "lake/diagnostics"

        # table bronze
        clickhouse_client.command(f'DROP TABLE IF EXISTS chu.diagnostics_bronze')
        clickhouse_client.command(f'''
            CREATE TABLE IF NOT EXISTS chu.diagnostics_bronze (
                stay_id String,
                code_cim10 String,
                inserted_at DateTime,
                data_path String
            )
            ENGINE = MergeTree()
            ORDER BY stay_id
        ''')

        for date_dir in os.listdir(f'{directory}'):
                diagnostics = []
                for file in os.listdir(f'{directory}/{date_dir}'):
                    with open(f'{directory}/{date_dir}/{file}') as file_obj:
                        diag = json.load(file_obj)
                        for r in diag:
                            stay_id = r.get("stay_id")
                            diagnostics_json = r.get("diagnostics")
                            for diag in diagnostics_json:
                                diagnostics.append([stay_id, diag.get("code_cim10"), datetime.now(), f'{directory}/{date_dir}/{file}'])

                    clickhouse_client.insert(f'chu.diagnostics_bronze', diagnostics)



        logging.info("ADDING MONITORING")
        directory = "lake/monitoring"

        # table bronze
        clickhouse_client.command(f'DROP TABLE IF EXISTS chu.monitoring_bronze')
        clickhouse_client.command(f'''
            CREATE TABLE IF NOT EXISTS chu.monitoring_bronze (
                stay_id String,
                ts date,
                heart_rate Int,
                spo2 Int,
                temp_c Float,
                inserted_at DateTime,
                data_path String
            )
            ENGINE = MergeTree()
            ORDER BY stay_id
        ''')

        for date_dir in os.listdir(f'{directory}'):
                monitorings = []
                for file in os.listdir(f'{directory}/{date_dir}'):
                    parquet_file = pd.read_parquet(f'{directory}/{date_dir}/monitoring.parquet')
                    for i in range(len(parquet_file)):
                        monitorings.append(
                            [
                                parquet_file.get("stay_id")[i], 
                                parquet_file.get("ts")[i], 
                                parquet_file.get("heart_rate")[i], 
                                parquet_file.get("spo2")[i], 
                                parquet_file.get("temp_c")[i],
                                datetime.now(),
                                f'{directory}/{date_dir}/{file}'
                            ]
                        )

                    clickhouse_client.insert(f'chu.monitoring_bronze', monitorings)


    except Exception as e:
        logging.error(f'AN ERROR HAPPENED: {e}')

    logging.info("DATA SUCCESSFULLY ADDED")


if __name__ == "__main__":
    main()