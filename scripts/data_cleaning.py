import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', port=18123, username='root', password='root')


import os


directory = "file_storage/patients_clean"
for date_dir in os.listdir(f'{directory}'):
        table_name_patients = f"patients_{date_dir.replace('-', '_')}"
        client.query(f'''
            DELETE FROM chu.{table_name_patients} WHERE sex NOT IN ('F', 'M')
        ''')

directory = "file_storage/monitoring"
for date_dir in os.listdir(f'{directory}'):
        table_name_monitoring = f"monitoring_{date_dir.replace('-', '_')}"
        client.query(f'''
            DELETE FROM chu.{table_name_monitoring}
            WHERE 
              heart_rate < 20 OR heart_rate > 250
              OR spo2 < 50 OR spo2 > 100
              OR temp_c < 30 OR temp_c > 45
        ''')