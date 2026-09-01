import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

client = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST"),
    port=os.getenv("CLICKHOUSE_PORT"),
    username=os.getenv("CLICKHOUSE_USERNAME"),
    password=os.getenv("CLICKHOUSE_USERNAME")
)


import os


directory = "file_storage/patients_clean"
for date_dir in os.listdir(f'{directory}'):
        table_name_patients = f"patients_{date_dir.replace('-', '_')}"
        client.query(f'''
            DELETE FROM chu.{table_name_patients}_silver WHERE sex NOT IN ('F', 'M')
        ''')

directory = "file_storage/monitoring"
for date_dir in os.listdir(f'{directory}'):
        table_name_monitoring = f"monitoring_{date_dir.replace('-', '_')}"
        client.query(f'''
            DELETE FROM chu.{table_name_monitoring}_silver
            WHERE 
              heart_rate < 20 OR heart_rate > 250
              OR spo2 < 50 OR spo2 > 100
              OR temp_c < 30 OR temp_c > 45
        ''')