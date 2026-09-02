from big_data.clickhouse_config.clickhouse import client as clickhouse_client
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
class Gold():

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

    def copy_table(self, table_name):
        clickhouse_client.command(f'DROP TABLE IF EXISTS chu.{table_name}_gold')
        clickhouse_client.query(f'''
            CREATE TABLE chu.{table_name}_gold AS chu.{table_name}_silver
        ''')
        clickhouse_client.query(f'''
            INSERT INTO chu.{table_name}_gold SELECT * FROM chu.{table_name}_silver;
        ''')


    def gold_stays(self):
        logging.info("COPYING STAYS")
        try:
            self.copy_table("sejours")
        except Exception as e:
            logging.error("Something went wrong during stays copy")
            raise e

    def gold_patients(self):
        logging.info("COPYING PATIENTS")
        try:
            self.copy_table("patients")
        except Exception as e:
            logging.error("Something went wrong during patients copy")
            raise e

    def gold_cim10(self):
        logging.info("COPYING CIM10")
        try:
            self.copy_table("cim10")
        except Exception as e:
            logging.error("Something went wrong during cim10 copy")
            raise e

    def gold_services(self):
        logging.info("COPYING SERVICES")
        try:
            self.create_table(
                    "services_gold",
                    [
                        {"arg": "service_code", "type": "String"},
                        {"arg": "service_label", "type": "String"},
                        {"arg": "dms", "type": "Float64"},
                        {"arg": "inserted_at", "type": "DateTime"},
                        {"arg": "data_path", "type": "String"},
                    ]
                )
            clickhouse_client.query(f'''
                INSERT INTO chu.services_gold
                SELECT service_code, service_label, AVG(dateDiff(hours, admission_ts, discharge_ts)) AS DMS, inserted_at, data_path
                FROM "chu"."services_silver"
                JOIN "chu"."sejours_silver" ON "chu"."sejours_silver"."service_code" = "chu"."services_silver"."service_code"
                GROUP BY service_code, service_label, inserted_at, data_path ;
            ''')
        except Exception as e:
            logging.error("Something went wrong during services copy")
            raise e

    def gold_service_per_day(self):
        logging.info("PATIENTS COUNT PER DAY")
        try:
            self.create_table(
                    "services_per_day_gold",
                    [
                        {"arg": "service_code", "type": "String"},
                        {"arg": "service_label", "type": "String"},
                        {"arg": "patients_count", "type": "Int"},
                        {"arg": "date", "type": "Date"},
                    ]
                )
            clickhouse_client.query(f'''
                INSERT INTO chu.services_per_day_gold
                SELECT service_code, service_label, COUNT(date(admission_ts)) AS patients_count, date(admission_ts) AS date
                FROM "chu"."services_silver" JOIN "chu"."sejours_silver"
                ON "chu"."sejours_silver"."service_code" = "chu"."services_silver"."service_code"
                GROUP BY service_code, service_label, date
                ORDER BY date ASC;
            ''')
        except Exception as e:
            logging.error("Something went wrong during services copy")
            raise e

    def gold_monitoring(self):
        logging.info("COPYING MONITORING")
        try:
            self.copy_table("monitoring")
        except Exception as e:
            logging.error("Something went wrong during monitoring copy")
            raise e

    def gold_diagnostics(self):
        logging.info("TABLE DIAGNOSTICS")
        try:
            self.create_table(
                    "diagnostics_gold",
                    [
                        {"arg": "patient_id", "type": "String"},
                        {"arg": "code_cim10", "type": "String"},
                        {"arg": "age", "type": "int"},
                        {"arg": "inserted_at", "type": "DateTime"},
                        {"arg": "data_path", "type": "String"},
                    ]
                )
            clickhouse_client.query(f'''
                INSERT INTO chu.diagnostics_gold SELECT patient_id,
                code_cim10,
                age('year', birth_date, today()) AS age,
                inserted_at, data_path
                FROM "chu"."diagnostics_silver"
                JOIN "chu"."patients_silver"
                ON "chu"."diagnostics_silver"."patient_id" = "chu"."patients_silver"."patient_id";
            ''')
        except Exception as e:
            logging.error("Something went wrong during diagnostics copy")
            raise e

    def cohorts_per_diagnostic(self):
        try:
            logging.info("Processing cohorts per diagnostics")
            self.create_table(
                    "cohorts_per_diagnostic_gold",
                    [
                        {"arg": "code_cim10", "type": "String"},
                        {"arg": "libelle", "type": "String"},
                        {"arg": "user_count", "type": "int"}
                    ]
                )
            clickhouse_client.query(f'''
                INSERT INTO chu.cohorts_per_diagnostic_gold
                    WITH cohort_sizes AS (
                        SELECT 
                            code_cim10 ,
                            COUNT(DISTINCT patient_id) AS user_count
                        FROM chu.diagnostics_gold
                        GROUP BY code_cim10 
                    )
                    SELECT 
                        diag.code_cim10 ,
                        cim10.libelle,
                        diag.user_count
                    FROM cohort_sizes diag
                    INNER JOIN chu.cim10_gold cim10 ON cim10.code_cim10 = diag.code_cim10
            ''')
        except Exception as e:
            logging.error("Something went wrong during cohorts per diagnostics process")
            raise e

    def clinical_research(self):
        try:
            logging.info("PROCESSING CLINICAL RESEARCH")
            self.cohorts_per_diagnostic()
        except Exception as e:
            logging.error("Something went wrong during clinical research process")
            raise e

def main():
    try:
        logging.info("STEP: GOLD")
        gold = Gold("chu")
        gold.gold_stays()
        gold.gold_patients()
        gold.gold_cim10()
        gold.gold_services()
        gold.gold_diagnostics()
        gold.gold_monitoring()
        gold.gold_service_per_day()
        gold.clinical_research()
        logging.info("STEP GOLD COMPLETED")
    except Exception as e:
        logging.error("Something went wrong during golding")
        raise e



if __name__ == "__main__":
    main()