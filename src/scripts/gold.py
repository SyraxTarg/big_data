from big_data.clickhouse_config.clickhouse import client as clickhouse_client
import logging

def copy_tables():
    logging.info("COPYING TABLES FROM BRONZE FOR SILVER")

    logging.info("COPYING STAYS")
    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.sejours_gold')
    clickhouse_client.query(f'''
        CREATE TABLE chu.sejours_gold AS chu.sejours_bronze
    ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.sejours_gold SELECT * FROM chu.sejours_bronze;
    ''')

    logging.info("COPYING PATIENTS")
    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.patients_gold')
    clickhouse_client.query(f'''
        CREATE TABLE chu.patients_gold AS chu.patients_bronze
    ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.patients_gold SELECT * FROM chu.patients_bronze;
    ''')

    logging.info("COPYING REFERENTIALS")
    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.cim10_gold')
    clickhouse_client.query(f'''
        CREATE TABLE chu.cim10_gold AS chu.cim10_bronze
    ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.cim10_gold SELECT * FROM chu.cim10_bronze;
    ''')

    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.services_gold')
    clickhouse_client.query(f'''
        CREATE TABLE chu.services_gold AS chu.services_bronze
    ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.services_gold SELECT * FROM chu.services_bronze;
    ''')

    logging.info("COPYING DIAGNOSTICS")
    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.diagnostics_gold')
    clickhouse_client.query(f'''
        CREATE TABLE chu.diagnostics_gold AS chu.diagnostics_silver
    ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.diagnostics_gold SELECT * FROM chu.diagnostics_silver;
    ''')
    

    logging.info("COPYING MONITORING")
    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.monitoring_gold')
    clickhouse_client.query(f'''
        CREATE TABLE chu.monitoring_gold AS chu.monitoring_silver
    ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.monitoring_gold SELECT * FROM chu.monitoring_silver;
    ''')

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    copy_tables()


if __name__ == "__main__":
    main()