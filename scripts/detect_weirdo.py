import csv


with open('file_storage/sejours/2026-08-26/sejours.csv') as file_obj:

    reader_obj = csv.reader(file_obj)

    for row in reader_obj:
        if row[-1] == '':
            print(row)