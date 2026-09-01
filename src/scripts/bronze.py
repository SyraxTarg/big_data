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
                            del row[-2:]
                            sejours.append(row)
                        sejours.pop(0)


                    # table bronze
                    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name_sejours}_bronze')
                    clickhouse_client.command(f'''
                        CREATE TABLE IF NOT EXISTS chu.{table_name_sejours}_bronze (
                            stay_id String,
                            patient_id String,
                            service_code String,
                            admission_ts DateTime,
                            discharge_ts Nullable(DateTime)
                        )
                        ENGINE = MergeTree()
                        ORDER BY stay_id
                    ''')
                    clickhouse_client.insert(f'chu.{table_name_sejours}_bronze', sejours)


                    # table silver
                    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name_sejours}_silver')
                    clickhouse_client.command(f'''
                        CREATE TABLE IF NOT EXISTS chu.{table_name_sejours}_silver (
                            stay_id String,
                            patient_id String,
                            service_code String,
                            admission_ts DateTime,
                            discharge_ts Nullable(DateTime)
                        )
                        ENGINE = MergeTree()
                        ORDER BY stay_id
                    ''')
                    clickhouse_client.insert(f'chu.{table_name_sejours}_silver', sejours)



        logging.info("ADDING PATIENTS")
        directory = "lake/patients"
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
                    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name_patients}_bronze')
                    clickhouse_client.command(f'''
                        CREATE TABLE IF NOT EXISTS chu.{table_name_patients}_bronze (
                            patient_id String,
                            birth_date Date32,
                            sex String,
                        )
                        ENGINE = MergeTree()
                        ORDER BY patient_id
                    ''')
                    clickhouse_client.insert(f'chu.{table_name_patients}_bronze', patients)


                    # table silver
                    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name_patients}_silver')
                    clickhouse_client.command(f'''
                        CREATE TABLE IF NOT EXISTS chu.{table_name_patients}_silver (
                            patient_id String,
                            birth_date Date32,
                            sex String,
                        )
                        ENGINE = MergeTree()
                        ORDER BY patient_id
                    ''')
                    clickhouse_client.insert(f'chu.{table_name_patients}_silver', patients)


        logging.info("ADDING REFERENTIALS")
        directory = "lake/referentiels"
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
                        clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name_cim10}_bronze')
                        clickhouse_client.command(f'''
                            CREATE TABLE IF NOT EXISTS chu.{table_name_cim10}_bronze (
                                code_cim10 String,
                                libelle String,
                            )
                            ENGINE = MergeTree()
                            ORDER BY code_cim10
                        ''')
                        clickhouse_client.insert(f'chu.{table_name_cim10}_bronze', cim10)


                        # table silver
                        clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name_cim10}_silver')
                        clickhouse_client.command(f'''
                            CREATE TABLE IF NOT EXISTS chu.{table_name_cim10}_silver (
                                code_cim10 String,
                                libelle String,
                            )
                            ENGINE = MergeTree()
                            ORDER BY code_cim10
                        ''')
                        clickhouse_client.insert(f'chu.{table_name_cim10}_silver', cim10)

                    if file == "services.csv":
                        services.pop(0)


                        # table bronze
                        clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name_services}_bronze')
                        clickhouse_client.command(f'''
                            CREATE TABLE IF NOT EXISTS chu.{table_name_services}_bronze (
                                service_code String,
                                service_label String,
                            )
                            ENGINE = MergeTree()
                            ORDER BY service_code
                        ''')
                        clickhouse_client.insert(f'chu.{table_name_services}_bronze', services)


                        # table silver
                        clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name_services}_silver')
                        clickhouse_client.command(f'''
                            CREATE TABLE IF NOT EXISTS chu.{table_name_services}_silver (
                                service_code String,
                                service_label String,
                            )
                            ENGINE = MergeTree()
                            ORDER BY service_code
                        ''')
                        clickhouse_client.insert(f'chu.{table_name_services}_silver', services)


        logging.info("ADDING DIAGNOSTICS")
        directory = "lake/diagnostics"
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
                                diagnostics.append([stay_id, diag.get("code_cim10")])


                    # table bronze
                    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name_diagnostics}_bronze')
                    clickhouse_client.command(f'''
                        CREATE TABLE IF NOT EXISTS chu.{table_name_diagnostics}_bronze (
                            stay_id String,
                            code_cim10 String
                        )
                        ENGINE = MergeTree()
                        ORDER BY stay_id
                    ''')
                    clickhouse_client.insert(f'chu.{table_name_diagnostics}_bronze', diagnostics)


                    # table silver
                    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name_diagnostics}_silver')
                    clickhouse_client.command(f'''
                        CREATE TABLE IF NOT EXISTS chu.{table_name_diagnostics}_silver (
                            stay_id String,
                            code_cim10 String
                        )
                        ENGINE = MergeTree()
                        ORDER BY stay_id
                    ''')
                    clickhouse_client.insert(f'chu.{table_name_diagnostics}_silver', diagnostics)


        logging.info("ADDING MONITORING")
        directory = "lake/monitoring"
        for date_dir in os.listdir(f'{directory}'):
                monitorings = []
                for file in os.listdir(f'{directory}/{date_dir}'):

                    table_name_monitoring = f"monitoring_{date_dir.replace('-', '_')}"
                    parquet_file = pd.read_parquet(f'{directory}/{date_dir}/monitoring.parquet')

                    for i in range(len(parquet_file)):
                        monitorings.append([parquet_file.get("stay_id")[i], parquet_file.get("ts")[i], parquet_file.get("heart_rate")[i], parquet_file.get("spo2")[i], parquet_file.get("temp_c")[i]])


                    # table bronze
                    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name_monitoring}_bronze')
                    clickhouse_client.command(f'''
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
                    clickhouse_client.insert(f'chu.{table_name_monitoring}_bronze', monitorings)


                    # table silver
                    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name_monitoring}_silver')
                    clickhouse_client.command(f'''
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
                    clickhouse_client.insert(f'chu.{table_name_monitoring}_silver', monitorings)

    except Exception as e:
        logging.error(f'AN ERROR HAPPENED: {e}')

    logging.info("DATA SUCCESSFULLY ADDED")


if __name__ == "__main__":
    main()