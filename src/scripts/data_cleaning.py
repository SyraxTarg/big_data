from big_data.clickhouse_config.clickhouse import client as clickhouse_client
import os

def main():
    directory = "lake/patients"
    for date_dir in os.listdir(f'{directory}'):
            table_name_patients = f"patients_{date_dir.replace('-', '_')}"
            clickhouse_client.query(f'''
                DELETE FROM chu.{table_name_patients}_silver WHERE sex NOT IN ('F', 'M')
            ''')

    directory = "lake/monitoring"
    for date_dir in os.listdir(f'{directory}'):
            table_name_monitoring = f"monitoring_{date_dir.replace('-', '_')}"
            clickhouse_client.query(f'''
                DELETE FROM chu.{table_name_monitoring}_silver
                WHERE
                heart_rate < 20 OR heart_rate > 250
                OR spo2 < 50 OR spo2 > 100
                OR temp_c < 30 OR temp_c > 45
            ''')


if __name__ == "__main__":
    main()