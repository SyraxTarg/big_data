""" ce script ajoute les données des CSV dans la base clickhouse."""

import json
import pandas as pd
from enum import Enum
import csv
import os
from datetime import datetime
import logging
from big_data.clickhouse_config.clickhouse import client as clickhouse_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("ADDING DATA TO CLICKHOUSE")

class referentials(Enum):
    CIM10 = "cim10"
    SERVICES = "services"


class Bronze():

    def __init__(self, database):
         self.db = database


    def create_table(self, table_name, args: list):
        try:
            clickhouse_client.command(f'DROP TABLE IF EXISTS {self.db}.{table_name}')
            table_args = ""
            for arg in args:
                table_args += f'{arg["arg"]} {arg["type"]}, '
            clickhouse_client.command(f'''
                        CREATE TABLE IF NOT EXISTS {self.db}.{table_name} ({table_args})
                        ENGINE = MergeTree()
                        ORDER BY {args[0]["arg"]}
                    ''')
        except Exception as e:
            raise e


    def creating_stays_table(self, directory: str):
        try:
            logging.info("ADDING STAYS")
            self.create_table(
                "sejours_bronze",
                [
                    {"arg": "stay_id", "type": "String"},
                    {"arg": "patient_id", "type": "String"},
                    {"arg": "service_code", "type": "String"},
                    {"arg": "admission_ts", "type": "DateTime"},
                    {"arg": "discharge_ts", "type": "Nullable(DateTime)"},
                    {"arg": "inserted_at", "type": "DateTime"},
                    {"arg": "data_path", "type": "String"},
                ]
            )
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
        except Exception as e:
            logging.error("Something went wrong during Stays table creation.")
            raise e


    def creating_patients_table(self, directory):
        try:
            logging.info("ADDING PATIENTS")
            self.create_table(
                "patients_bronze",
                [
                    {"arg": "patient_id", "type": "String"},
                    {"arg": "birth_date", "type": "Date32"},
                    {"arg": "sex", "type": "String"},
                    {"arg": "inserted_at", "type": "DateTime"},
                    {"arg": "data_path", "type": "String"},
                ]
            )

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
        except Exception as e:
            logging.error("Something went wrong during Patients table creation.")
            raise e


    def creating_referentials_tables(self, directory: str, referential: referentials):
        try:
            logging.info("ADDING REFERENTIALS")
            match referential:
                case referentials.SERVICES:
                    self.create_table(
                        "services_bronze",
                        [
                            {"arg": "service_code", "type": "String"},
                            {"arg": "service_label", "type": "String"},
                            {"arg": "inserted_at", "type": "DateTime"},
                            {"arg": "data_path", "type": "String"},
                        ]
                    )
                    for date_dir in os.listdir(f'{directory}'):
                        services = []
                        for file in os.listdir(f'{directory}/{date_dir}'):
                            if file == "services.csv":
                                with open(f'{directory}/{date_dir}/{file}') as file_obj:
                                    reader_obj_referentiels = csv.reader(file_obj)
                                    for row in reader_obj_referentiels:
                                        row.append(datetime.now())
                                        row.append(f'{directory}/{date_dir}/{file}')
                                        services.append(row)
                                services.pop(0)
                                clickhouse_client.insert(f'chu.services_bronze', services)


                case referentials.CIM10:
                    self.create_table(
                        "cim10_bronze",
                        [
                            {"arg": "code_cim10", "type": "String"},
                            {"arg": "libelle", "type": "String"},
                            {"arg": "inserted_at", "type": "DateTime"},
                            {"arg": "data_path", "type": "String"},
                        ]
                    )
                    for date_dir in os.listdir(f'{directory}'):
                        cim10 = []
                        for file in os.listdir(f'{directory}/{date_dir}'):
                            if file == "cim10.csv":
                                with open(f'{directory}/{date_dir}/{file}') as file_obj:
                                    reader_obj_referentiels = csv.reader(file_obj)
                                    for row in reader_obj_referentiels:
                                        row.append(datetime.now())
                                        row.append(f'{directory}/{date_dir}/{file}')
                                        cim10.append(row)
                                cim10.pop(0)
                                clickhouse_client.insert(f'chu.cim10_bronze', cim10)


                case _:
                    raise Exception("Unknown referential")
        except Exception as e:
            logging.error(f'Something went wrong during {referential} table creation.')
            raise e



    def creating_diagnostics_tables(self, directory):
        try:
            logging.info("ADDING DIAGNOSTICS")
            self.create_table(
                "diagnostics_bronze",
                [
                    {"arg": "stay_id", "type": "String"},
                    {"arg": "code_cim10", "type": "String"},
                    {"arg": "inserted_at", "type": "DateTime"},
                    {"arg": "data_path", "type": "String"},
                ]
            )

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
        except Exception as e:
            logging.error("Something went wrong during Diagnostics table creation.")
            raise e



    def creating_monitoring_tables(self, directory):
        try:
            logging.info("ADDING MONITORING")
            self.create_table(
                "monitoring_bronze",
                [
                    {"arg": "stay_id", "type": "String"},
                    {"arg": "ts", "type": "Datetime"},
                    {"arg": "heart_rate", "type": "Int"},
                    {"arg": "spo2", "type": "Int"},
                    {"arg": "temp_c", "type": "Float"},
                    {"arg": "inserted_at", "type": "DateTime"},
                    {"arg": "data_path", "type": "String"},
                ]
            )

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
            logging.error("Something went wrong during Monotoring table creation.")
            raise e


def main():
    try:
        logging.info("STEP : BRONZE")
        bronze = Bronze(database="chu")
        bronze.creating_stays_table("lake/sejours")
        bronze.creating_patients_table("lake/patients")
        bronze.creating_referentials_tables("lake/referentiels", referentials.CIM10)
        bronze.creating_referentials_tables("lake/referentiels", referentials.SERVICES)
        bronze.creating_diagnostics_tables("lake/diagnostics")
        bronze.creating_monitoring_tables("lake/monitoring")
        logging.info("STEP BRONZE COMPLETED")
    except Exception as e:
        raise e

if __name__ == "__main__":
    main()