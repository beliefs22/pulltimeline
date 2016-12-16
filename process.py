import os
import re

from maketables import makeTable


def getDirectories():
    """Returns directories where various files are located or saved to"""
    sep = os.sep
    subject_files_dir = r'I:\Research\CEIRS\Year 3\Active Surveillance\Source Documents for Chart Reviews\ED Subject Files'
    cleaned_files_dir = r'H:\timelinedata\Subject_Files\Cleaned_Files'
    import_files_dir = r'H:\timelinedata\Import_Backups'
    completed_files_dir = r'H:\timelinedata\Subject_Files\Completed Files'
    log_dir = r'H:\timelinedata\Logs'
    source_dir = r'I:\Research\CEIRS\Year 3\Active Surveillance\Source Documents for Chart Reviews'
    directories = {'Subject Dir': subject_files_dir,
                   'Clean Dir': cleaned_files_dir,
                   'Import Dir': import_files_dir,
                   'Complete Dir': completed_files_dir,
                   'Log Dir': log_dir,
                   'Source Dir': source_dir
                   }
    return directories


def getIds(cleaned_files_dir):
    """Returns list of subject ids to test based on files in cleaned files directory"""
    current_files = os.listdir(cleaned_files_dir)
    id_pat = re.compile(r'(_)(\d{4})')
    ids = set()
    for file_name in current_files:
        id_search = id_pat.search(file_name)
        if id_search is not None:
            file_id = id_search.group(2)
            ids.add(file_id)
        else:
            print "Could not find a study ID for %s" % file_name
    return ids


def pullDataFromCleanedFiles(ids, cleaned_files_dir, logfile):
    """Process to pull data from Cleaned Files
    Args:
        ids (list) : string of ids to pull data for
        cleaned_files_dir (str) : location of cleaned files
        logfile (str) : log filename
        """
    from insertdata import Vitals, Cbc, Cmp, Medications, Influenza, Assessment
    from insertdata import BloodGas, Cultures, RespVirus, Disposition, Xpert, Imaging
    sep = os.sep
    add_dir = lambda directory, filename: directory + sep + filename
    current_files = os.listdir(cleaned_files_dir)
    information_types = [Vitals, Cbc, Cmp, Medications, Influenza, Assessment,
                         BloodGas, Cultures, RespVirus, Disposition, Xpert, Imaging]
    for subject_id in ids:
        print("Finding files for id {}".format(subject_id))
        subject_files = [filename
                         for filename in current_files
                         if filename.find(subject_id) != -1
                         if filename.find(".txt") != -1
                         ]
        for single_file in subject_files:
            filename = add_dir(cleaned_files_dir, single_file)
            conn = makeTable(filename)
            print("Starting data extractions for {}".format(filename))
            for information_type in information_types:
                with open(filename, 'rb') as subject_file:
                    data_extractor = information_type(conn, subject_file, logfile)
                    data_extractor.extractData()
            print("Finished data extraction for {}".format(filename))