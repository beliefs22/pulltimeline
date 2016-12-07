import os
import csv
import importrecords
import ceirstokens
from datetime import datetime

def importToRedcap(import_file_dir):
    import_file_name = import_file_dir + os.sep + 'CEIRS_ACTIVE_IMPORT_FILE' + \
                       str(datetime.now().date()).replace("-","_") +\
                       '.csv'
    importfile = open(import_file_name,'r')
    import_reader = csv.reader(importfile)
    #skip_headers
    data = []
    active_token = ceirstokens.active_token()
    for row in import_reader:
        data.append(",".join(row))
    payload = "\n".join(data)
    importrecords.importRecords(active_token, payload)

def main():
    pass

if __name__ == "__main__":
    main()
