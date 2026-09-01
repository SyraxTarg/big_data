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
            self.copy_table("services")
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
                        {"arg": "stay_id", "type": "String"},
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
        logging.info("STEP GOLD COMPLETED")
    except Exception as e:
        logging.error("Something went wrong during golding")
        raise e



if __name__ == "__main__":
    main()