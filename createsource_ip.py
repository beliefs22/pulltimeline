import sqlite3
import collections
import os
import getcommonnames
import csv
from datetime import datetime
import re


def createSourceFromData(ids, cleaned_files_dir, source_files_dir, import_files_dir, logfile):
    """Creates Source document and CSV to Import Records to Redcap

    Args:
        ids (list) : str of ids to create source documents for
        cleaned_files_dir (str) : location of clean files
        source_files_dir (str) : location of source files
        import_files_dir (str) : location of import files
        logfile (str) : filename for logfile
    """
    sep = os.sep
    # visit_num_pat = re.compile(r'(IP)(\d)')
    current_files = os.listdir(cleaned_files_dir)
    # export_file_name = import_files_dir + os.sep + 'CEIRS_ACTIVE_IMPORT_FILE_IP' + \
    #                    str(datetime.now().date()).replace("-", "_") + ".csv"

    # Prep file for importing all records to redcap
    # importfile = open(export_file_name, 'ab+')
    # csv_reader = csv.reader(importfile)
    # csv_writer = csv.writer(importfile)
    #
    # headers = csv_reader.next()
    # header_locations = {header: index
    #                     for index, header in enumerate(headers)
    #                     }

    # Create source document for Indivisual visit and collect data to append to import file for each ID
    for subject_id in ids:
        # Container for data to write to importfile once all files are processed
        # data_to_write_csv = [""] * len(headers)
        # data_to_write_csv[
        #     header_locations['edptchart_visitnumber']] = "0"
        subject_files = [filename
                         for filename in current_files
                         if filename.find(subject_id) != -1
                         if filename.find(".db") != -1
                         ]
        for subject_file in subject_files:
            conn = sqlite3.connect(cleaned_files_dir + sep + subject_file)
            cur = conn.cursor()
            data_to_write = collections.OrderedDict()
            visit_num_pat = re.compile(r'(IP)(\d)')
            visit_match = visit_num_pat.search(subject_file)
            if visit_match is not None:
                visit_num = visit_match.group(2)
            else:
                print("No visit type found for subject {}".format(subject_file))
            # Create file for Souce Document Output
            # outfile = open(source_files_dir + sep + subject_file.replace(".db", ".txt"), "w")
            # Study ID
            study_id = "01-11-A-{}".format(subject_id)
            data_to_write['Study ID'] = study_id

            # Disposition

            cur.execute('''
            SELECT time, type
            FROM dispo'''
                        )
            dispo_data = cur.fetchall()
            if dispo_data:
                for info in dispo_data:
                    time = info[0]
                    dispo_type = info[1]
                    if dispo_type == 'admit':
                        data_to_write['Admit Date'] = time.split()[0]
                    if dispo_type == 'discharge':
                        data_to_write['Discharge Date'] = time.split()[0]

            # Medications
            cur.execute('''
            SELECT time, type
            FROM medications'''
                        )
            abx_count = 0
            antiviral_count = 0
            abx_names = getcommonnames.getAbxNames()
            antiviral_names = getcommonnames.getAntiviralNames()
            med_data = cur.fetchall()
            if med_data:
                for medication in med_data:
                    admin_times = ",".join([admin.split()[0] for admin in medication[0].split(",") if admin.split()[0] != 'RN'])
                    med_name = medication[1]
                    if med_name in abx_names:
                        abx_count += 1
                        data_to_write['Antibiotic # {} - {}'.format(abx_count, med_name)] = admin_times
                    if med_name in antiviral_names:
                        antiviral_count += 1
                        data_to_write['Antiviral # {} - {}'.format(antiviral_count, med_name)] = admin_times


            # Influenza Testing
            # Result Types
            result_types = {'resp virus complex': {'negative': ['Influenza A NAT No RNA Detected',
                                                                'RSV NAT No RNA Detected',
                                                                'Rhinovirus NAT No RNA Detected',
                                                                'Parainfluenzae 1 NAT No RNA Detected',
                                                                'Parainfluenzae 2 NAT No RNA Detected',
                                                                'Parainfluenzae 3 NAT No RNA Detected'],
                                                   'positive': ['Influenza A NAT RNA Detected',
                                                                'RSV NAT RNA Detected',
                                                                'Rhinovirus NAT RNA Detected',
                                                                'Parainfluenzae 1 NAT RNA Detected',
                                                                'Parainfluenzae 2 NAT RNA Detected',
                                                                'Parainfluenzae 3 NAT RNA Detected']},
                            'viral culture, respiratory': {'negative': ['No virus isolated'],
                                                           'positive': []},
                            'influenza a/b + rsv': {'negative': ['Influenza A NAT No RNA Detected',
                                                                 'RSV NAT No RNA Detected'],
                                                    'positive': ['Influenza A NAT RNA Detected',
                                                                 'RSV NAT RNA Detected']}
                            }
            cur.execute('''
            SELECT *
            FROM labs
            WHERE type = ?
            OR type = ?
            OR type = ?''', ('resp virus complex','viral culture, respiratory','influenza a/b + rsv'))

            influenza_data = cur.fetchall()
            if influenza_data:
                for influenza_test in influenza_data:
                    time, test_type, value = influenza_test
                    value = value.replace("\t"," ")
                    result_matches = result_types.get(test_type)
                    negative_results = result_matches.get('negative')
                    for negative_match in negative_results:
                        if value.find(negative_match) != -1:
                            data_to_write['Influeza Test Type {}'.format(test_type + " " + time)] = \
                                data_to_write.get('Influeza Test Type {}'.format(test_type + " " + time), []) + \
                                            [negative_match]
                    positive_results = result_matches.get('positive')
                    for positive_match in positive_results:
                        if value.find(positive_match) != -1:
                            data_to_write['Influeza Test Type {}'.format(test_type + " " + time)] = \
                                data_to_write.get('Influeza Test Type {}'.format(test_type + " " + time), []) + \
                                            [positive_match]
                    if data_to_write.get('Influeza Test Type {}'.format(test_type + " " + time)):
                        data_to_write['Influeza Test Type {}'.format(test_type + " " + time)] = \
                            ",".join(data_to_write['Influeza Test Type {}'.format(test_type + " " + time)])

            for key,value in data_to_write.iteritems():
                print "{} : {}".format(key, value)





def main():
    pass


if __name__ == "__main__":
    main()