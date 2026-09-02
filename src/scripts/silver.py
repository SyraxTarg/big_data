from big_data.clickhouse_config.clickhouse import get_db_name, client as clickhouse_client
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Silver():
    def __init__(self):
        self.db = get_db_name()

    def copy_table_generic(self, table_name):
        try:
            clickhouse_client.command(f'DROP TABLE IF EXISTS {self.db}.{table_name}_silver')
            clickhouse_client.query(f'''
                CREATE TABLE {self.db}.{table_name}_silver AS {self.db}.{table_name}_bronze
            ''')
            clickhouse_client.query(f'''
                INSERT INTO {self.db}.{table_name}_silver SELECT * FROM {self.db}.{table_name}_bronze;
            ''')
        except Exception as e:
            raise e

    def copy_diagnostics_table(self):
        try:
            logging.info("Copying diagnostics")
            clickhouse_client.command(f'DROP TABLE IF EXISTS {self.db}.diagnostics_silver')
            clickhouse_client.command(f'''
                        CREATE TABLE IF NOT EXISTS {self.db}.diagnostics_silver (
                            patient_id String,
                            code_cim10 String,
                            inserted_at DateTime,
                            data_path String
                        )
                        ENGINE = MergeTree()
                        ORDER BY patient_id
                    ''')
            clickhouse_client.query(f'''
                INSERT INTO {self.db}.diagnostics_silver
                    SELECT  {self.db}.patients_bronze.patient_id AS patient_id,
                            {self.db}.diagnostics_bronze.code_cim10,
                            {self.db}.diagnostics_bronze.inserted_at,
                            {self.db}.diagnostics_bronze.data_path
                    FROM {self.db}.diagnostics_bronze
                    INNER JOIN {self.db}.sejours_bronze ON {self.db}.diagnostics_bronze.stay_id = {self.db}.sejours_bronze.stay_id
                    INNER JOIN {self.db}.patients_bronze ON {self.db}.sejours_bronze.patient_id = {self.db}.patients_bronze.patient_id
            ''')
        except Exception as e:
            raise e

    def copy_monitoring_table(self):
        try:
            logging.info("Copying monitoring")
            clickhouse_client.command(f'DROP TABLE IF EXISTS chu.monitoring_silver')
            clickhouse_client.command(f'''
                        CREATE TABLE IF NOT EXISTS chu.monitoring_silver (
                            patient_id String,
                            ts DateTime,
                            heart_rate Nullable(Int),
                            spo2 Nullable(Int),
                            temp_c Nullable(Float),
                            inserted_at DateTime,
                            data_path String
                        )
                        ENGINE = MergeTree()
                        ORDER BY patient_id
                    ''')
            clickhouse_client.command(f'''
                        INSERT INTO {self.db}.monitoring_silver
                            SELECT  {self.db}.patients_bronze.patient_id AS patient_id,
                                    {self.db}.monitoring_bronze.ts,
                                    {self.db}.monitoring_bronze.heart_rate,
                                    {self.db}.monitoring_bronze.spo2,
                                    {self.db}.monitoring_bronze.temp_c,
                                    {self.db}.monitoring_bronze.inserted_at,
                                    {self.db}.monitoring_bronze.data_path
                            FROM {self.db}.monitoring_bronze
                            INNER JOIN {self.db}.sejours_bronze ON {self.db}.monitoring_bronze.stay_id = {self.db}.sejours_bronze.stay_id
                            INNER JOIN {self.db}.patients_bronze ON {self.db}.sejours_bronze.patient_id = {self.db}.patients_bronze.patient_id
                    ''')
        except Exception as e:
            raise e

    def copy_tables(self):
        try:
            logging.info("COPYING TABLES FROM BRONZE")

            logging.info("Copying stays")
            self.copy_table_generic('sejours')

            logging.info("Copying patients")
            self.copy_table_generic('patients')

            logging.info("Copying referentials")
            self.copy_table_generic('cim10')
            self.copy_table_generic('services')

            self.copy_diagnostics_table()
            self.copy_monitoring_table()

            logging.info("COPY DONE\n")
        except Exception as e:
            raise e


    def clean_data(self):
        try:
            logging.info("CLEANING DATA")

            logging.info("Cleaning patients")
            clickhouse_client.query(f'''
                DELETE FROM {self.db}.patients_silver WHERE sex NOT IN ('F', 'M')
            ''')
            clickhouse_client.query(f'''
                DELETE FROM {self.db}.patients_silver WHERE birth_date > now
            ''')

            logging.info("Cleaning stays")
            clickhouse_client.query(f'''
                DELETE FROM {self.db}.sejours_silver WHERE discharge_ts < admission_ts
            ''')

            logging.info("Cleaning monitoring")
            clickhouse_client.query(f'''
                ALTER TABLE {self.db}.monitoring_silver
                UPDATE heart_rate = NULL
                WHERE heart_rate < 20 OR heart_rate > 250
            ''')
            clickhouse_client.query(f'''
                ALTER TABLE {self.db}.monitoring_silver
                UPDATE spo2 = NULL
                WHERE spo2 < 50 OR spo2 > 100
            ''')
            clickhouse_client.query(f'''
                ALTER TABLE {self.db}.monitoring_silver
                UPDATE temp_c = NULL
                WHERE temp_c < 30 OR temp_c > 45
            ''')

            logging.info("CLEANING DONE\n")
        except Exception as e:
            raise e


    def deduplicate_data(self):
        try:
            logging.info("DEDUPLICATING DATA")

            logging.info("Deduplicating patients")
            clickhouse_client.query(f'''
                OPTIMIZE TABLE {self.db}.patients_silver FINAL DEDUPLICATE BY patient_id
            ''')

            logging.info("Deduplicating stays")
            clickhouse_client.query(f'''
                OPTIMIZE TABLE {self.db}.sejours_silver FINAL DEDUPLICATE BY stay_id
            ''')

            logging.info("Deduplicating monitoring")
            clickhouse_client.query(f'''
                OPTIMIZE TABLE {self.db}.monitoring_silver FINAL DEDUPLICATE BY patient_id, ts
            ''')

            logging.info("Deduplicating diagnostics")
            clickhouse_client.query(f'''
                OPTIMIZE TABLE {self.db}.diagnostics_silver FINAL DEDUPLICATE
            ''')

            logging.info("Deduplicating references")
            clickhouse_client.query(f'''
                OPTIMIZE TABLE {self.db}.cim10_silver FINAL DEDUPLICATE
            ''')
            clickhouse_client.query(f'''
                OPTIMIZE TABLE {self.db}.services_silver FINAL DEDUPLICATE
            ''')

            logging.info("DEDUPLICATING DONE\n")
        except Exception as e:
            raise e


    def delete_small_cohorts(self, cohort_size_limit):
        try:
            logging.info("DELETING COHORTS < 5")

            logging.info("Deleting cohorts < 5 for stays")
            clickhouse_client.query(f'''
                DELETE FROM {self.db}.sejours_silver
                WHERE service_code IN (
                    SELECT service_code
                    FROM {self.db}.sejours_silver
                    GROUP BY service_code
                    HAVING COUNT(DISTINCT patient_id) < {cohort_size_limit}
                )
            ''')

            logging.info("Deleting cohorts < 5 for monitoring")
            clickhouse_client.query(f'''
                DELETE FROM {self.db}.monitoring_silver
                WHERE heart_rate IN (
                    SELECT heart_rate
                    FROM {self.db}.monitoring_silver
                    GROUP BY heart_rate
                    HAVING COUNT(DISTINCT patient_id) < {cohort_size_limit}
                )
            ''')
            clickhouse_client.query(f'''
                DELETE FROM {self.db}.monitoring_silver
                WHERE spo2 IN (
                    SELECT spo2
                    FROM {self.db}.monitoring_silver
                    GROUP BY spo2
                    HAVING COUNT(DISTINCT patient_id) < {cohort_size_limit}
                )
            ''')
            clickhouse_client.query(f'''
                DELETE FROM {self.db}.monitoring_silver
                WHERE temp_c IN (
                    SELECT temp_c
                    FROM {self.db}.monitoring_silver
                    GROUP BY temp_c
                    HAVING COUNT(DISTINCT patient_id) < {cohort_size_limit}
                )
            ''')

            logging.info("Deleting cohorts < 5 for diagnostics")
            clickhouse_client.query(f'''
                DELETE FROM {self.db}.diagnostics_silver
                WHERE code_cim10 IN (
                    SELECT code_cim10
                    FROM {self.db}.diagnostics_silver
                    GROUP BY code_cim10
                    HAVING COUNT(DISTINCT patient_id) < 5
                )
            ''')

            logging.info("DELETING COHORTS DONE\n")
        except Exception as e:
            raise e

def main():
    try:
        logging.info("STEP : SILVER\n")

        silver = Silver()
        COHORT_SIZE_LIMIT = 5

        silver.copy_tables()
        silver.clean_data()
        silver.deduplicate_data()
        silver.delete_small_cohorts(COHORT_SIZE_LIMIT)

        logging.info("STEP SILVER COMPLETED")
    except Exception as e:
            raise e

if __name__ == "__main__":
    main()