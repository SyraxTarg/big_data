from big_data.clickhouse_config.clickhouse import client as clickhouse_client
import os
import logging

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("CLEANING DATA FOR SILVER")

    logging.info("CLEANING PATIENTS")
    directory = "lake/patients"
    for date_dir in os.listdir(f'{directory}'):
            table_name_patients = f"patients_{date_dir.replace('-', '_')}_silver"
            clickhouse_client.query(f'''
                DELETE FROM chu.{table_name_patients} WHERE sex NOT IN ('F', 'M')
            ''')
            clickhouse_client.query(f'''
                DELETE FROM chu.{table_name_patients} WHERE birth_date > now
            ''')


    logging.info("CLEANING STAYS")
    directory = "lake/sejours"
    for date_dir in os.listdir(f'{directory}'):
            table_name_sejours = f"sejours_{date_dir.replace('-', '_')}_silver"
            clickhouse_client.query(f'''
                DELETE FROM chu.{table_name_sejours} WHERE discharge_ts < admission_ts
            ''')


    logging.info("CLEANING MONITORING")
    directory = "lake/monitoring"
    for date_dir in os.listdir(f'{directory}'):
            table_name_monitoring = f"monitoring_{date_dir.replace('-', '_')}_silver"
            clickhouse_client.query(f'''
                DELETE FROM chu.{table_name_monitoring}
                WHERE 
                heart_rate < 20 OR heart_rate > 250
                OR spo2 < 50 OR spo2 > 100
                OR temp_c < 30 OR temp_c > 45
            ''')

if __name__ == "__main__":
    main()