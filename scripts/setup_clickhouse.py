import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', port=18123, username='root', password='root')

import csv


import os


directory = "file_storage/sejours"
for date_dir in os.listdir(f'{directory}'):
        sejours = []
        for file in os.listdir(f'{directory}/{date_dir}'):

            table_name_sejours = f"sejours_{date_dir.replace('-', '_')}"
            with open(f'{directory}/{date_dir}/{file}') as file_obj:

                reader_obj_sejours = csv.reader(file_obj)
                for row in reader_obj_sejours:
                    sejours.append(row)
                # print(sejours)
                sejours.pop(0)

            client.command(f'''
                CREATE TABLE IF NOT EXISTS chu.{table_name_sejours} (
                    stay_id String,
                    patient_id String,
                    service_code String,
                    admission_ts String,
                    discharge_ts String,
                    admission_mode String,
                    discharge_mode String
                )
                ENGINE = MergeTree()
                ORDER BY stay_id
            ''')
            client.insert(f'chu.{table_name_sejours}', sejours)



directory = "file_storage/patients_clean"
for date_dir in os.listdir(f'{directory}'):
        patients = []
        for file in os.listdir(f'{directory}/{date_dir}'):

            table_name_patients = f"patients_{date_dir.replace('-', '_')}"
            with open(f'{directory}/{date_dir}/{file}') as file_obj:

                reader_obj_patients = csv.reader(file_obj)
                for row in reader_obj_patients:
                    row.remove(row[-1])
                    patients.append(row)
                patients.pop(0)

            client.command(f'''
                CREATE TABLE IF NOT EXISTS chu.{table_name_patients} (
                    patient_id String,
                    birth_date String,
                    sex String,
                )
                ENGINE = MergeTree()
                ORDER BY patient_id
            ''')
            client.insert(f'chu.{table_name_patients}', patients)



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
                client.command(f'''
                    CREATE TABLE IF NOT EXISTS chu.{table_name_cim10} (
                        code_cim10 String,
                        libelle String,
                    )
                    ENGINE = MergeTree()
                    ORDER BY code_cim10
                ''')
                client.insert(f'chu.{table_name_cim10}', cim10)
            if file == "services.csv":
                services.pop(0)
                client.command(f'''
                    CREATE TABLE IF NOT EXISTS chu.{table_name_services} (
                        service_code String,
                        service_label String,
                    )
                    ENGINE = MergeTree()
                    ORDER BY service_code
                ''')
                client.insert(f'chu.{table_name_services}', services)
