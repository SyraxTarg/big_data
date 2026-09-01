from big_data.clickhouse_config.clickhouse import client as clickhouse_client
import logging

def copy_tables():
    logging.info("COPYING TABLES FROM BRONZE FOR SILVER")

    logging.info("COPYING STAYS")
    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.sejours_silver')
    clickhouse_client.query(f'''
        CREATE TABLE chu.sejours_silver AS chu.sejours_bronze
    ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.sejours_silver SELECT * FROM chu.sejours_bronze;
    ''')

    logging.info("COPYING PATIENTS")
    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.patients_silver')
    clickhouse_client.query(f'''
        CREATE TABLE chu.patients_silver AS chu.patients_bronze
    ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.patients_silver SELECT * FROM chu.patients_bronze;
    ''')

    logging.info("COPYING REFERENTIALS")
    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.cim10_silver')
    clickhouse_client.query(f'''
        CREATE TABLE chu.cim10_silver AS chu.cim10_bronze
    ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.cim10_silver SELECT * FROM chu.cim10_bronze;
    ''')

    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.services_silver')
    clickhouse_client.query(f'''
        CREATE TABLE chu.services_silver AS chu.services_bronze
    ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.services_silver SELECT * FROM chu.services_bronze;
    ''')

    logging.info("COPYING DIAGNOSTICS")
    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.diagnostics_silver')
    clickhouse_client.query(f'''
        CREATE TABLE chu.diagnostics_silver AS chu.diagnostics_bronze
    ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.diagnostics_silver SELECT * FROM chu.diagnostics_bronze;
    ''')

    logging.info("COPYING MONITORING")
    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.monitoring_silver')
    clickhouse_client.query(f'''
        CREATE TABLE chu.monitoring_silver AS chu.monitoring_bronze
    ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.monitoring_silver SELECT * FROM chu.monitoring_bronze;
    ''')

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    copy_tables()

    logging.info("CLEANING DATA FOR SILVER")

    logging.info("CLEANING PATIENTS")
    clickhouse_client.query(f'''
        DELETE FROM chu.patients_silver WHERE sex NOT IN ('F', 'M')
    ''')
    clickhouse_client.query(f'''
        DELETE FROM chu.patients_silver WHERE birth_date > now
    ''')


    logging.info("CLEANING STAYS")
    clickhouse_client.query(f'''
        DELETE FROM chu.sejours_silver WHERE discharge_ts < admission_ts
    ''')


    logging.info("CLEANING MONITORING")
    clickhouse_client.query(f'''
        DELETE FROM chu.monitoring_silver
        WHERE 
        heart_rate < 20 OR heart_rate > 250
        OR spo2 < 50 OR spo2 > 100
        OR temp_c < 30 OR temp_c > 45
    ''')

if __name__ == "__main__":
    main()