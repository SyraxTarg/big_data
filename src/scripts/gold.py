from big_data.clickhouse_config.clickhouse import get_db_name, client as clickhouse_client
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
class Gold():

    def __init__(self):
        self.db = get_db_name()

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

    # Partie 1
    def gold_service_dms(self):
        logging.info("Processing services DMS")
        try:
            self.create_table(
                    "services_dms_gold",
                    [
                        {"arg": "service_label", "type": "String"},
                        {"arg": "dms", "type": "Float64"},
                    ]
                )
            clickhouse_client.query(f'''
                INSERT INTO {self.db}.services_dms_gold
                    SELECT service_label, AVG(dateDiff(hours, admission_ts, discharge_ts)) AS dms
                    FROM {self.db}.services_silver services
                    JOIN {self.db}.sejours_silver sejours ON sejours.service_code = services.service_code
                    GROUP BY service_label
            ''')
        except Exception as e:
            logging.error("Something went wrong during service DMS process")
            raise e

    def gold_service_per_day(self):
        logging.info("Processing patients count per day")
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
                INSERT INTO {self.db}.services_per_day_gold
                    SELECT service_code, service_label, COUNT(date(admission_ts)) AS patients_count, date(admission_ts) AS date
                    FROM {self.db}.services_silver services
                    JOIN {self.db}.sejours_silver sejours ON sejours.service_code = services.service_code
                    GROUP BY service_code, service_label, date
                    ORDER BY date ASC
            ''')
        except Exception as e:
            logging.error("Something went wrong during patients count per day process")
            raise e

    def gold_readmission_per_services(self):
        logging.info("Processing readmissions per service")
        try:
            self.create_table(
                    "readmissions_gold",
                    [
                        {"arg": "service_label", "type": "String"},
                        {"arg": "total_stay", "type": "Int"},
                        {"arg": "total_readmissions", "type": "Int"},
                        {"arg": "readmission_percentage", "type": "Float64"},
                    ]
                )
            clickhouse_client.query(f'''
                INSERT INTO {self.db}.readmissions_gold
                WITH exists_and_admissions AS (
                    SELECT
                        patient_id,
                        service_label,
                        discharge_ts AS last_exit_date,
                        LEAD(admission_ts) OVER (PARTITION BY patient_id ORDER BY admission_ts ASC) AS next_admission_date
                    FROM {self.db}.sejours_silver
                    JOIN {self.db}.services_silver ON {self.db}.sejours_silver.service_code = {self.db}.services_silver.service_code
                ),
                readmissions AS (
                    SELECT
                        service_label,
                        patient_id,
                        CASE
                            WHEN next_admission_date IS NOT NULL
                            AND dateDiff('day', last_exit_date, next_admission_date) > 0
                            AND dateDiff('day', last_exit_date, next_admission_date) <= 30
                            THEN 1
                            ELSE 0
                        END AS is_readmitted
                    FROM exists_and_admissions
                )
                SELECT
                    service_label,
                    COUNT(DISTINCT patient_id) AS total_stays,
                    SUM(is_readmitted) AS total_readmissions_30j,
                    ROUND(SUM(is_readmitted) * 100.0 / COUNT(DISTINCT patient_id), 2) AS readmission_percentage
                FROM readmissions
                GROUP BY service_label
                ORDER BY readmission_percentage DESC;
            ''')
        except Exception as e:
            logging.error("Something went wrong during readmissions per services process")
            raise e


    def gold_readmission_total(self):
        logging.info("Processing readmissions total")
        try:
            self.create_table(
                    "readmissions_total_gold",
                    [
                        {"arg": "total_stay", "type": "Int"},
                        {"arg": "total_readmissions", "type": "Int"},
                        {"arg": "readmission_percentage", "type": "Float64"},
                    ]
                )
            clickhouse_client.query(f'''
                INSERT INTO {self.db}.readmissions_total_gold
                    WITH exists_and_admissions AS (
                        SELECT
                            patient_id,
                            discharge_ts AS last_exit_date,
                            LEAD(admission_ts) OVER (PARTITION BY patient_id ORDER BY admission_ts ASC) AS next_admission_date
                        FROM {self.db}.sejours_silver
                    ),
                    readmissions AS (
                        SELECT
                            CASE
                                WHEN next_admission_date IS NOT NULL
                                AND dateDiff('day', last_exit_date, next_admission_date) > 0
                                AND dateDiff('day', last_exit_date, next_admission_date) <= 30
                                THEN 1
                                ELSE 0
                            END AS is_readmitted
                        FROM exists_and_admissions
                    )
                    SELECT
                        COUNT(*) AS total_stays,
                        SUM(is_readmitted) AS total_readmissions_30j,
                        ROUND(SUM(is_readmitted) * 100.0 / COUNT(*), 2) AS total_readmission_percentage
                    FROM readmissions;
            ''')
        except Exception as e:
            logging.error("Something went wrong during readmissions total process")
            raise e

    def monitoring_alerts_per_day(self):
        try:
            BRADYCARDIA_THRESHOLD = 50
            TACHYCARDIA_THRESHOLD = 100
            O2_DESATURATION_THRESHOLD = 92
            FEVER_THRESHOLD = 38.5

            logging.info("Processing monitoring alerts per day")
            self.create_table(
                    "monitoring_alerts_per_day_gold",
                    [
                        {"arg": "date", "type": "Date"},
                        {"arg": "total_monitored", "type": "int"},
                        {"arg": "total_alerts", "type": "int"}
                    ]
                )
            clickhouse_client.query(f'''
            INSERT INTO {self.db}.monitoring_alerts_per_day_gold
                SELECT
                    date(ts) AS date,
                    COUNT() AS total_monitored,
                    COUNT(
                        CASE WHEN  heart_rate < {BRADYCARDIA_THRESHOLD}
                        OR heart_rate > {TACHYCARDIA_THRESHOLD}
                        OR spo2 < {O2_DESATURATION_THRESHOLD}
                        OR temp_c > {FEVER_THRESHOLD} THEN 1 END
                    ) AS total_alerts
                FROM {self.db}.monitoring_silver
                GROUP BY date(ts)
                ORDER BY date
            ''')
        except Exception as e:
            logging.error("Something went wrong during monitoring alerts per day process")
            raise e

    def gold_create_age_per_sex(self):
        logging.info("CREATING AGE GROUP TABLE")
        try:
            self.create_table(
                    "age_per_sex_gold",
                    [
                        {"arg": "age_group", "type": "String"},
                        {"arg": "sex", "type": "String"},
                        {"arg": "patients_count", "type": "Nullable(Int)"},
                        {"arg": "avg_age", "type": "Float"},
                        {"arg": "code_cim10", "type": "String"},
                        {"arg": "libelle_cim10", "type": "String"},
                    ]
                )
        except Exception as e:
            logging.error("Something went wrong during services copy")
            raise e

    def gold_insert_age_per_sex(self, age_start: int, age_stop: Optional[int] = None):
        logging.info(f'Inserting age groups {age_start}-{age_stop}')
        try:
            if age_stop:
                clickhouse_client.query(f'''
                    INSERT INTO {self.db}.age_per_sex_gold (age_group, sex, patients_count, avg_age, code_cim10, libelle_cim10)
                    SELECT '{age_start}-{age_stop}', sex, COUNT(patient_id) AS patients_count, AVG(age) AS avg_age, code_cim10, libelle FROM (
                        select p.patient_id AS patient_id, age('year', birth_date, today()) as age, p.sex AS sex, d.code_cim10 AS code_cim10, c.libelle
                        FROM "{self.db}"."patients_silver" p
                        JOIN {self.db}.diagnostics_silver d
                        ON p.patient_id = d.patient_id
                        JOIN {self.db}.cim10_silver c
                        ON d.code_cim10 = c.code_cim10
                        GROUP BY p.patient_id, p.sex, age, d.code_cim10, c.libelle HAVING age BETWEEN {age_start} AND {age_stop}
                                                ) GROUP BY sex, code_cim10, libelle;
                ''')
            else:
                clickhouse_client.query(f'''
                    INSERT INTO {self.db}.age_per_sex_gold (age_group, sex, patients_count, avg_age, code_cim10, libelle_cim10)
                    SELECT '>{age_start}', sex, COUNT(patient_id) AS patients_count, AVG(age) AS avg_age, code_cim10, libelle FROM (
                        select p.patient_id AS patient_id, age('year', birth_date, today()) as age, p.sex AS sex, d.code_cim10 AS code_cim10, c.libelle
                        FROM "{self.db}"."patients_silver" p
                        JOIN {self.db}.diagnostics_silver d
                        ON p.patient_id = d.patient_id
                        JOIN {self.db}.cim10_silver c
                        ON d.code_cim10 = c.code_cim10
                        GROUP BY p.patient_id, p.sex, age, d.code_cim10, c.libelle HAVING age >= {age_start}
                                                ) GROUP BY sex, code_cim10, libelle;
                ''')
            clickhouse_client.command(f'alter table {self.db}.age_per_sex_gold update patients_count = null where patients_count < 5;')
        except Exception as e:
            logging.error("Something went wrong during age group insertion")
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
                INSERT INTO {self.db}.cohorts_per_diagnostic_gold
                    WITH cohort_sizes AS (
                        SELECT
                            code_cim10 ,
                            COUNT(DISTINCT patient_id) AS user_count
                        FROM {self.db}.diagnostics_silver
                        GROUP BY code_cim10
                    )
                    SELECT
                        diag.code_cim10,
                        cim10.libelle,
                        diag.user_count
                    FROM cohort_sizes diag
                    INNER JOIN {self.db}.cim10_silver cim10 ON cim10.code_cim10 = diag.code_cim10
                    ORDER BY diag.user_count DESC
            ''')
        except Exception as e:
            logging.error("Something went wrong during cohorts per diagnostics process")
            raise e

    # Partie 2
    def gold_service_categories_dms(self):
        logging.info("Processing categories DMS")
        try:
            self.create_table(
                    "categories_dms_gold",
                    [
                        {"arg": "categorie", "type": "String"},
                        {"arg": "DMS", "type": "Float64"},
                    ]
                )
            clickhouse_client.query(f'''
                INSERT INTO {self.db}.categories_dms_gold
                    SELECT categorie, AVG(dateDiff(hours, admission_ts, discharge_ts)) AS DMS
                    FROM {self.db}.services_silver services
                    JOIN {self.db}.sejours_silver sejours ON sejours.service_code = services.service_code
                    GROUP BY categorie
            ''')
        except Exception as e:
            logging.error("Something went wrong during categories DMS process")
            raise e

    def gold_service_categories_per_day(self):
        logging.info("Processing stay count per day by categories")
        try:
            self.create_table(
                    "categories_per_day_gold",
                    [
                        {"arg": "categorie", "type": "String"},
                        {"arg": "stay_count", "type": "Int"},
                        {"arg": "date", "type": "Date"},
                    ]
                )
            clickhouse_client.query(f'''
                INSERT INTO {self.db}.categories_per_day_gold
                    SELECT categorie, COUNT(date(admission_ts)) AS stay_count, date(admission_ts) AS date
                    FROM {self.db}.services_silver services
                    JOIN {self.db}.sejours_silver sejours ON sejours.service_code = services.service_code
                    GROUP BY categorie, date
                    ORDER BY date ASC
            ''')
        except Exception as e:
            logging.error("Something went wrong during stay count per categories per day process")
            raise e


    def gold_service_categories(self):
        logging.info("Processing stay count per categories")
        try:
            self.create_table(
                    "categories_stay_gold",
                    [
                        {"arg": "categorie", "type": "String"},
                        {"arg": "stay_count", "type": "Int"},
                    ]
                )
            clickhouse_client.query(f'''
                INSERT INTO {self.db}.categories_stay_gold
                    SELECT categorie, COUNT(date(admission_ts)) AS stay_count
                    FROM {self.db}.services_silver services
                    JOIN {self.db}.sejours_silver sejours ON sejours.service_code = services.service_code
                    GROUP BY categorie
                    ORDER BY stay_count ASC
            ''')
        except Exception as e:
            logging.error("Something went wrong during patient count per categories process")
            raise e
        
    def gold_actes_per_service(self):
        logging.info("Processing actes per service")
        try:
            self.create_table(
                    "actes_per_service_gold",
                    [
                        {"arg": "actes_count", "type": "int"},
                        {"arg": "service_label", "type": "String"}
                    ]
            )

            clickhouse_client.query(f'''
                    INSERT INTO {self.db}.actes_per_service_gold
                        SELECT count(actes.acte_ts) as actes_count, services.service_label as service_label
                        FROM {self.db}.sejours_silver sejours
                        JOIN {self.db}.patients_silver patients ON patients.patient_id = sejours.patient_id
                        JOIN {self.db}.actes_silver actes ON actes.patient_id = patients.patient_id
                        JOIN {self.db}.services_silver services ON services.service_code = sejours.service_code
                        GROUP BY service_label
            ''')
        except Exception as e:
            logging.error("Something went wrong during actes per service process")
            raise e

    def gold_avg_actes_per_stay(self):
        logging.info("Processing average actes per stay")
        try:
            self.create_table(
                    "avg_actes_per_stay_gold",
                    [
                        {"arg": "avg_actes_count", "type": "float"},
                    ]
            )

            clickhouse_client.query(f'''
                    INSERT INTO {self.db}.avg_actes_per_stay_gold
                        WITH cte AS (
                            SELECT count(acte_ts) as actes_count, stay_id
                            FROM {self.db}.sejours_silver sejours
                            JOIN {self.db}.actes_silver actes ON actes.patient_id = sejours.patient_id
                            WHERE actes.acte_ts BETWEEN sejours.admission_ts AND sejours.discharge_ts
                            GROUP BY stay_id
                        )
                        SELECT AVG(actes_count) AS avg_actes_count
                        FROM cte
            ''')
        except Exception as e:
            logging.error("Something went wrong during actes per stay process")
            raise e



    def gold_actes_per_ccam(self):
        logging.info("Processing actes per ccam")
        try:
            self.create_table(
                    "actes_per_ccam_gold",
                    [
                        {"arg": "libelle", "type": "String"},
                        {"arg": "actes_count", "type": "int"},
                    ]
            )

            clickhouse_client.query(f'''
                    INSERT INTO {self.db}.actes_per_ccam_gold
                        SELECT ccam.libelle AS libelle, count(actes.acte_ts) AS actes_count
                        FROM {self.db}.actes_silver actes
                        JOIN {self.db}.ccam_silver ccam ON ccam.code_ccam = actes.code_ccam
                        GROUP BY ccam.libelle, ccam.code_ccam
            ''')
        except Exception as e:
            logging.error("Something went wrong during actes per ccam process")
            raise e


    def gold_actes_per_bed(self):
        logging.info("Processing number of acts per bud capacity")
        try:
            self.create_table(
                    "actes_per_bed_gold",
                    [
                        {"arg": "service_label", "type": "String"},
                        {"arg": "capacite_lits", "type": "Int"},
                        {"arg": "nb_actes", "type": "Int"},
                    ]
            )

            clickhouse_client.query(f'''
                    INSERT INTO {self.db}.actes_per_bed_gold
                         SELECT services.service_label, services.capacite_lits, COUNT(*) AS nb_actes
                        FROM {self.db}.sejours_silver sejours
                        JOIN {self.db}.actes_silver actes ON actes.patient_id = sejours.patient_id
                        JOIN {self.db}.services_silver services ON services.service_code = sejours.service_code
                        WHERE actes.acte_ts BETWEEN sejours.admission_ts AND sejours.discharge_ts
                        GROUP BY services.service_label, services.capacite_lits
            ''')
        except Exception as e:
            logging.error("Something went wrong during actes per bed capacity")
            raise e

    def gold_amount_charged_per_service(self):
        logging.info("Processing amount charged per service")
        try:
            self.create_table(
                    "amount_charged_per_service_gold",
                    [
                        {"arg": "amount_charged_euros", "type": "int"},
                        {"arg": "service_label", "type": "String"}
                    ]
            )

            clickhouse_client.query(f'''
                INSERT INTO {self.db}.amount_charged_per_service_gold
                        SELECT
                            sum(ccam.tarif_euros) as amount_charged_euros,
                            services.service_label as service_label
                        FROM {self.db}.sejours_silver sejours
                        JOIN {self.db}.patients_silver patients ON patients.patient_id = sejours.patient_id
                        JOIN {self.db}.actes_silver actes ON actes.patient_id = patients.patient_id
                        JOIN {self.db}.services_silver services ON services.service_code = sejours.service_code
                        JOIN {self.db}.ccam_silver ccam ON ccam.code_ccam = actes.code_ccam
                        GROUP BY service_code, service_label
            ''')
        except Exception as e:
            logging.error("Something went wrong during aount charged per service process")
            raise e

    def hospital_management(self):
        try:
            logging.info("PROCESSING HOSPITAL MANAGEMENT")
            # Partie 1
            self.gold_service_dms()
            self.gold_service_per_day()
            self.gold_readmission_per_services()
            self.gold_readmission_total()
            self.monitoring_alerts_per_day()

            # Partie 2
            self.gold_service_categories_dms()
            self.gold_service_categories_per_day()
            self.gold_service_categories()
            self.gold_actes_per_service()
            self.gold_avg_actes_per_stay()
            self.gold_actes_per_ccam()
            self.gold_actes_per_bed()
            self.gold_amount_charged_per_service()
            logging.info("HOSPITAL MANAGEMENT PROCESS DONE\n")
        except Exception as e:
            logging.error("Something went wrong during hospital management process")
            raise e

    def clinical_research(self):
        try:
            logging.info("PROCESSING CLINICAL RESEARCH")
            self.cohorts_per_diagnostic()
            self.gold_create_age_per_sex()
            self.gold_insert_age_per_sex(0, 10)
            self.gold_insert_age_per_sex(11, 17)
            self.gold_insert_age_per_sex(18, 25)
            self.gold_insert_age_per_sex(26, 39)
            self.gold_insert_age_per_sex(40, 65)
            self.gold_insert_age_per_sex(66)
            logging.info("CLINICAL RESEARCH PROCESS DONE\n")
        except Exception as e:
            logging.error("Something went wrong during clinical research process")
            raise e

def main():
    try:
        logging.info("STEP: GOLD\n")
        gold = Gold()
        gold.hospital_management()
        gold.clinical_research()
        logging.info("STEP GOLD COMPLETED")
    except Exception as e:
        logging.error("Something went wrong during golding")
        raise e



if __name__ == "__main__":
    main()