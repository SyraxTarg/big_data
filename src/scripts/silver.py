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
    clickhouse_client.command(f'''
                CREATE TABLE IF NOT EXISTS chu.diagnostics_silver (
                    patient_id String,
                    code_cim10 String,
                    inserted_at DateTime,
                    data_path String
                )
                ENGINE = MergeTree()
                ORDER BY patient_id
            ''')
    clickhouse_client.query(f'''
        INSERT INTO chu.diagnostics_silver SELECT "chu"."patients_bronze"."patient_id" AS "patient_id", "chu"."diagnostics_bronze"."code_cim10", "chu"."diagnostics_bronze"."inserted_at", "chu"."diagnostics_bronze"."data_path" FROM "chu"."diagnostics_bronze"
        INNER JOIN "chu"."sejours_bronze" ON "chu"."diagnostics_bronze"."stay_id" = "chu"."sejours_bronze"."stay_id"
        INNER JOIN "chu"."patients_bronze" ON "chu"."sejours_bronze"."patient_id" = "chu"."patients_bronze"."patient_id"
    ''')


    logging.info("COPYING MONITORING")
    clickhouse_client.command(f'DROP TABLE IF EXISTS chu.monitoring_silver')
    clickhouse_client.command(f'''
                CREATE TABLE IF NOT EXISTS chu.monitoring_silver (
                    patient_id String,
                    ts date,
                    heart_rate Int,
                    spo2 Int,
                    temp_c Float,
                    inserted_at DateTime,
                    data_path String
                )
                ENGINE = MergeTree()
                ORDER BY patient_id
            ''')
    clickhouse_client.command('''INSERT INTO chu.monitoring_silver SELECT "chu"."patients_bronze"."patient_id" AS "patient_id",
                                  "chu"."monitoring_bronze"."ts",
                                  "chu"."monitoring_bronze"."heart_rate",
                                  "chu"."monitoring_bronze"."spo2",
                                  "chu"."monitoring_bronze"."temp_c",
                                  "chu"."monitoring_bronze"."inserted_at",
                                  "chu"."monitoring_bronze"."data_path" FROM "chu"."monitoring_bronze"
                                  INNER JOIN "chu"."sejours_bronze" ON "chu"."monitoring_bronze"."stay_id" = "chu"."sejours_bronze"."stay_id"
                                  INNER JOIN "chu"."patients_bronze" ON "chu"."sejours_bronze"."patient_id" = "chu"."patients_bronze"."patient_id"''')

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