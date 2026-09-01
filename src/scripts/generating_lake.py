import csv
import shutil
import os
import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

directory_source = "file_storage"
directory_lake = "lake"
last_lake_generation_path = "config/last_lake_generation.csv"
ALLOWED_RESOURCES = ["diagnostics", "monitoring", "patients", "referentiels", "sejours"]

def if_not_exist_create_it(path) -> None:
    logging.info(f'Checking if {path} exists')
    if not os.path.exists(path):
        logging.info(f'Creating')
        os.mkdir(path)

def get_last_date_in_lake() -> datetime.datetime:
    with open(last_lake_generation_path, "r") as file_obj:
        reader = csv.reader(file_obj)
        for row in reader:
            last_date = row[0]
        return datetime.datetime.strptime(last_date, "%Y-%m-%d").date()

def update_last_date_in_lake(date: datetime.datetime) -> None:
    date = date.strftime("%Y-%m-%d")
    with open(last_lake_generation_path,"w", newline='') as result:
        writer= csv.writer( result )
        writer.writerow([date])


def main():
    last_date = get_last_date_in_lake()
    logging.info(f'Generating or updating data lake')
    if_not_exist_create_it(directory_lake)
    for dir in os.listdir(directory_source):
        logging.info(f'Directory {dir}')
        if dir in ALLOWED_RESOURCES:
            if_not_exist_create_it(f'{directory_lake}/{dir}')
            for dir_date in os.listdir(f'{directory_source}/{dir}'):
                logging.info(f'Entering {directory_source}/{dir}')
                if datetime.datetime.strptime(dir_date.replace(",", ""), "%Y-%m-%d").date() > get_last_date_in_lake():
                    if_not_exist_create_it(f'{directory_lake}/{dir}/{dir_date}')

                    for filename in os.listdir(f'{directory_source}/{dir}/{dir_date}'):
                        if dir == "patients":
                            logging.info(f'Cleaning {directory_source}/{dir}')
                            with open(f'{directory_source}/{dir}/{dir_date}/{filename}', "r") as file_obj:
                                reader = csv.reader(file_obj)
                                with open(f'{directory_lake}/{dir}/{dir_date}/{filename}',"w", newline='') as result:
                                    writer= csv.writer( result )
                                    for row in reader:
                                        writer.writerow((row[0], row[4], row[5], row[6]))
                        else:
                            shutil.copyfile(f'{directory_source}/{dir}/{dir_date}/{filename}', f'{directory_lake}/{dir}/{dir_date}/{filename}')
                    last_date = datetime.datetime.strptime(dir_date.replace(",", ""), "%Y-%m-%d").date()
        else:
            logging.warning("Unknown directory : passing.")

    if get_last_date_in_lake() < last_date:
        update_last_date_in_lake(last_date)

if __name__ == "__main__":
    main()


