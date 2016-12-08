from process import getDirectories, getIds, pullDataFromCleanedFiles
from datetime import datetime
from shutil import copyfile
import os
from createsource import createSourceFromData
from importtoredcap import importToRedcap
from deletephi import deletePHI
from cleanfiles import cleanFiles


def getData():
    directories = getDirectories()
    clean_dir = directories['Clean Dir']
    subject_dir = directories['Subject Dir']
    import_dir = directories['Import Dir']
    log_dir = directories['Log Dir']
    completed_dir = directories['Complete Dir']
    source_dir = directories['Source Dir']
    print(directories)

    # Create copy of import file
    sep = os.sep
    copy_file_name = 'CEIRS_ACTIVE_IMPORT_FILE' + \
                     str(datetime.now().date()).replace("-", "_") + \
                     '.csv'
    copyfile(os.getcwd() + sep + 'CEIRS_ACTIVE_IMPORT_FILE.csv',
             import_dir + sep + copy_file_name)

    logfile = log_dir + sep + 'Log_' + \
              str(datetime.date(datetime.now())).replace("-", "_") + ".txt"

    current_files = os.listdir(subject_dir)
    print(current_files)
    cleanFiles(clean_dir, subject_dir)
    ids = getIds(clean_dir)
    print(ids)
    pullDataFromCleanedFiles(ids, clean_dir, logfile)
    createSourceFromData(ids, clean_dir, source_dir, import_dir, logfile)
    deletePHI(ids, subject_dir, clean_dir, completed_dir)
    importToRedcap(import_dir)
    print("All done, and it didn't fail yay")
