import csv

import os

directory = "file_storage/patients"
os.mkdir("file_storage/patients_clean")
for dir in os.listdir(directory):
    os.mkdir(f'file_storage/patients_clean/{dir}')
    for file in os.listdir(f'{directory}/{dir}'):
        filename = os.fsdecode(file)
        print(f'{directory}/{dir}/{filename}')

        with open(f'{directory}/{dir}/{filename}', "r") as file_obj:

            reader = csv.reader(file_obj)

            with open(f'file_storage/patients_clean/{dir}/{filename}',"w", newline='') as result:
                writer= csv.writer( result )

                for row in reader:
                    writer.writerow((row[0], row[4], row[5], row[6]))
                    print(row)
# os.rmdir("file_storage/patients")