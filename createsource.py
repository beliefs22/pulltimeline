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
    visit_num_pat = re.compile(r'(ED)(\d)')
    current_files = os.listdir(cleaned_files_dir)
    export_file_name = import_files_dir + os.sep + 'CEIRS_ACTIVE_IMPORT_FILE' + \
                       str(datetime.now().date()).replace("-", "_") + ".csv"

    # Prep file for importing all records to redcap
    importfile = open(export_file_name, 'ab+')
    csv_reader = csv.reader(importfile)
    csv_writer = csv.writer(importfile)

    headers = csv_reader.next()
    header_locations = {header: index
                        for index, header in enumerate(headers)
                        }

    # Create source document for Indivisual visit and collect data to append to import file for each ID
    for subject_id in ids:
        # Container for data to write to importfile once all files are processed
        data_to_write_csv = [""] * len(headers)
        data_to_write_csv[
            header_locations['edptchart_visitnumber']] = "0"
        subject_files = [filename
                         for filename in current_files
                         if filename.find(subject_id) != -1
                         if filename.find(".db") != -1
                         ]
        for subject_file in subject_files:
            conn = sqlite3.connect(cleaned_files_dir + sep + subject_file)
            cur = conn.cursor()
            data_to_write = collections.OrderedDict()
            visit_num_pat = re.compile(r'(ED)(\d)')
            visit_match = visit_num_pat.search(subject_file)
            if visit_match is not None:
                visit_num = visit_match.group(2)
            else:
                print("No visit type found for subject {}".format(subject_file))
            # Create file for Souce Document Output
            outfile = open(source_files_dir + sep + subject_file.replace("_clean.db", ".txt"), "w")
            # Study ID
            study_id = "01-11-A-{}".format(subject_id)
            data_to_write['Study ID'] = study_id
            data_to_write_csv[
                header_locations['id']] = study_id

            # ED Visit Duration and disposition
            cur.execute('''
                    SELECT *
                    FROM disposition
                    ''')
            arrival_info = list(cur.fetchone())
            data_to_write['Arrival Date'] = arrival_info[0]
            data_to_write_csv[
                header_locations[
                    'ps_edchrev' + visit_num + "_" + 'arrived']] = arrival_info[0]
            data_to_write['Arrival Time'] = arrival_info[1]
            if len(arrival_info[1]) == 8:
                arrival_info[1] = arrival_info[1][:5]
            data_to_write_csv[
                header_locations[
                    'ps_edchrev' + visit_num + "_" + 'arrivet']] = arrival_info[1]
            data_to_write['Departure Date'] = arrival_info[2]
            data_to_write_csv[
                header_locations[
                    'ps_edchrev' + visit_num + "_" + 'departd']] = arrival_info[2]
            if len(arrival_info[3]) == 8:
                arrival_info[3] = arrival_info[3][:5]
            data_to_write['Departure Time'] = arrival_info[3]
            data_to_write_csv[
                header_locations[
                    'ps_edchrev' + visit_num + "_" + 'departt']] = arrival_info[3]
            data_to_write['Final Disposition'] = arrival_info[4]
            dispositions = {"admitted": 100, "discharged": 101, "eloped": 102, "other": 103}
            data_to_write_csv[
                header_locations[
                    'ps_edchrev' + visit_num + "_" + 'dispo']] = dispositions[arrival_info[4]]
            if arrival_info[4] == "discharged":
                cur.execute('''
                        SELECT disposition
                        FROM disposition
                        ''')
                data = cur.fetchall()
                if data != []:
                    obs = False
                    for disposition in data:
                        if disposition[0] == 'observation':
                            obs = True
                    if obs:
                        data_to_write_csv[
                            header_locations[
                                'ps_edchrev' + visit_num + "_" + 'dispoobs']] = "1"
                    else:
                        data_to_write_csv[
                            header_locations[
                                'ps_edchrev' + visit_num + "_" + 'dispoobs']] = "0"

            data_to_write_csv[
                header_locations['redcap_data_access_group']] = 'jhhs'

            possible_visit_num = int(data_to_write_csv[
                                         header_locations['edptchart_visitnumber']])
            if possible_visit_num < int(visit_num):
                possible_visit_num = str(visit_num)
            data_to_write_csv[
                header_locations['edptchart_visitnumber']] = possible_visit_num
            data_to_write_csv[
                header_locations['edchartrev_visit' + visit_num]] = arrival_info[0]
            data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'death']] = "0"

            # Physical Exam
            cur.execute('''
                    SELECT *
                    FROM vitals
                    ''')

            vital_info = cur.fetchone()
            data_to_write['Temperature'] = "999.9"
            data_to_write['Pulse'] = "999"
            data_to_write['Respiratory Rate'] = "999"
            data_to_write['Systolic Blood Pressure'] = "999"
            data_to_write['Oxygen Saturation'] = "999"

            data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'temp']] = "999.9"
            data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'pulse']] = "999"
            data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'rr']] = "999"
            data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'sbp']] = "999"
            data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'o2s']] = "999"

            if vital_info is not None:
                if vital_info[2] is not None:
                    data_to_write['Temperature'] = vital_info[2]
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'temp']] = vital_info[2]
                if vital_info[3] is not None:
                    data_to_write['Pulse'] = vital_info[3]
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'pulse']] = vital_info[3]
                if vital_info[4] is not None:
                    data_to_write['Respiratory Rate'] = vital_info[4]
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'rr']] = vital_info[4]
                if vital_info[1] is not None:
                    sbp = vital_info[1].split("/")[0]
                    data_to_write['Systolic Blood Pressure'] = sbp
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'sbp']] = sbp
                if vital_info[5] is not None:
                    data_to_write['Oxygen Saturation'] = vital_info[5]
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'o2s']] = vital_info[5]

            # Oxygen Supplementation
            cur.execute('''
                    SELECT device, rate_lmin
                    FROM vitals
                    ''')
            oxygen_info = cur.fetchall()
            oxygen_type = None
            oxygen_rate = "N/A"
            route = {'Nasal cannula': "1", '(Bipap)': "3", '(Cpap)': "3",
                     '(BIPAP)': "3", '(CPAP)': "3", 'Facemask/non-rebreather': "2",
                     'Intubated': "4", "None": "0"}
            final_route = "None"
            final_rate = 0
            for oxygen in oxygen_info:
                # there is a o2 device present
                if oxygen[0] is not None:
                    possible_route = route.get(oxygen[0], None)
                    if possible_route and \
                                    int(possible_route) > int(route.get(final_route)):
                        final_route = oxygen[0]
                if oxygen[1] is not None:
                    possible_rate = oxygen[1]
                    try:
                        if int(possible_rate) >= final_rate:
                            final_rate = int(oxygen[1])
                    except ValueError:
                        if float(possible_rate) >= float(final_rate):
                            final_rate = float(oxygen[1])
            if final_route == "None":
                oxygen_type = "None Given"
                oxygen_rate = "N/A"
            else:
                oxygen_type = final_route
                oxygen_rate = str(final_rate)

            data_to_write['Supplemental Oxygen Route'] = oxygen_type
            data_to_write['Oxygen Rate'] = oxygen_rate
            if oxygen_type != "None Given":
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'o2sup']] = "1"
                data_to_write_csv[
                    header_locations[
                        'ps_edchrev' + visit_num + "_" + 'o2sup_l']] = oxygen_rate
                data_to_write_csv[
                    header_locations[
                        'ps_edchrev' + visit_num + "_" + 'o2sup_r']] = \
                    route.get(oxygen_type)
                if oxygen_type == "Intubated":
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'intub']] = "1"
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'bipap']] = "0"

                if oxygen_type in ["(BIPAP)", "(Bipap)"]:
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'bipap']] = "1"
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'intub']] = "0"
                if oxygen_type not in ["(BIPAP)", "(Bipap)", "Intubated"]:
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'intub']] = "0"
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'bipap']] = "0"
            else:
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'o2sup']] = "0"
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'intub']] = "0"
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'bipap']] = "0"
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'suppoxy']] = "0"

            # GCS Score
            cur.execute('''
                    SELECT glasgow_coma_scale_score
                    FROM assessments
                    ''')
            data = cur.fetchone()
            gcs = "No Score In Timeline"
            if data is not None:
                if int(data[0]) <= gcs:
                    gcs = data[0]
                    data_to_write['GCS Score'] = str(gcs)
                    if int(gcs) < 13:
                        data_to_write_csv[
                            header_locations['ps_edchrev' + visit_num + "_" + 'ams']] = "1"
                    else:
                        data_to_write_csv[
                            header_locations['ps_edchrev' + visit_num + "_" + 'ams']] = "0"
            else:
                data_to_write['GCS Score'] = gcs

            # Blood Gas
            cur.execute('''
                    SELECT ph_arterial, ph_non_arterial
                    FROM bloodgas
                    ''')
            data = cur.fetchall()
            ph = "999"
            if data != []:
                for item in data:
                    if item[0] is not None:
                        ph = item[0]
                    if item[1] is not None:
                        ph = item[1]
            data_to_write['pH'] = ph
            data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'ph']] = ph

            # CMP
            cur.execute('''
                    SELECT sodium, glucose, urea_nitrogen
                    FROM cmp
                    ''')
            sodium = "999"
            glucose = "999"
            bun = "999"
            data = cur.fetchone()
            if data is not None:
                if data[0] is not None:
                    sodium = data[0]
                if data[1] is not None:
                    glucose = data[1]
                if data[2] is not None:
                    bun = data[2]
            data_to_write['Sodium'] = sodium
            data_to_write['Glucose'] = glucose
            data_to_write['BUN'] = bun
            data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'sodium']] = sodium
            data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'glucose']] = glucose
            data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'bun']] = bun

            # CBC
            cur.execute('''
                    SELECT hematocrit
                    FROM cbc
                    ''')
            hematocrit = "999"
            data = cur.fetchone()
            if data is not None:
                if data[0] is not None:
                    hematocrit = data[0]
            data_to_write['Hematocrit'] = hematocrit
            data_to_write_csv[
                header_locations[
                    'ps_edchrev' + visit_num + "_" + 'hemocr']] = hematocrit

            # Influenza testing:Influenza NAT
            cur.execute('''
                    SELECT time, result_time, influenza_a_nat, influenza_b_nat, rsv_nat
                    FROM influenza
                    ''')
            influenza_count = 0
            nat_testing = False
            resp_testing = False
            data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'flutesting']] = "0"
            data = cur.fetchall()
            if data != []:
                nat_testing = True
                for index, row in enumerate(data):
                    if influenza_count > 5:
                        break
                    influenza_count = index + 1
                    collect_time = row[0].split("-")[0]
                    result_time = row[1].split("-")[0]
                    influenza_a = row[2].strip()
                    influenza_b = row[3].strip()
                    rsv = row[4]
                    result = "Test Name: Influenza A/B RSV rapid NAT- Collect Date:\
                %s Result Date: %s Influenza A Result: %s Influenza B Result: %s RSV Result: \
                %s" % (collect_time, result_time, influenza_a, influenza_b, rsv)
                    # Write Influenza Test to import file
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                         str(influenza_count) + '_name']] = \
                        'Influenza A/B RSV rapid NAT'
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                         str(influenza_count) + '_testtype']] = "4"
                    # Positive Test write to import
                    if influenza_a == "RNA Detected" or \
                                    influenza_b != "RNA Detected":
                        data_to_write_csv[
                            header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                             str(influenza_count) + '_res']] = "2"
                    if influenza_a == "No RNA Detected" and \
                                    influenza_b == "No RNA Detected":
                        data_to_write_csv[
                            header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                             str(influenza_count) + '_res']] = "1"

                    cold, colt = collect_time.strip().split()
                    resd, rest = result_time.strip().split()
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                         str(influenza_count) + '_cold']] = cold
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                         str(influenza_count) + '_colt']] = colt
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                         str(influenza_count) + '_resd']] = resd
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                         str(influenza_count) + '_rest']] = rest
                    # Influenza A and B positive
                    if influenza_a == "RNA Detected" and influenza_b == "RNA Detected":
                        data_to_write_csv[
                            header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                             str(influenza_count) + '_typing']] = "1"
                        data_to_write_csv[
                            header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                             str(influenza_count) + '_typsp']] = "A and B"
                    # Influenza A only
                    if influenza_a == "RNA Detected":
                        data_to_write_csv[
                            header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                             str(influenza_count) + '_typing']] = "1"
                        data_to_write_csv[
                            header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                             str(influenza_count) + '_typsp']] = "A"
                    # Influenza B only
                    if influenza_b == "RNA Detected":
                        data_to_write_csv[
                            header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                             str(influenza_count) + '_typing']] = "1"
                        data_to_write_csv[
                            header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                             str(influenza_count) + '_typsp']] = "B"
                    # write test to source
                    data_to_write['Influenza Test #' + str(index + 1)] = result
                data_to_write['Influenza Total Test #'] = str(influenza_count)
            if influenza_count > 0:
                data_to_write_csv[
                    header_locations[
                        'ps_edchrev' + visit_num + "_" + 'flutests']] = \
                    str(influenza_count)
                data_to_write_csv[
                    header_locations[
                        'ps_edchrev' + visit_num + "_" + 'othervir']] = "1"
                if rsv == "RNA Detected":
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'othervir_rsv']] = "1"
                else:
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'othervir_rsv']] = "0"
            # Influenza testing: Resp Virus
            cur.execute('''
                    SELECT time, type
                    FROM respvirus
                    ''')
            data = cur.fetchall()
            if data != []:
                resp_testing = True
                # other viruses were tested
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'othervir']] = "1"
                for row in data:
                    if influenza_count > 5:
                        break
                    influenza_count += 1
                    collect_time = row[0].split("-")[0].strip()
                    result = "Test Name: Resp Virus Complex Panel- Collect Time: %s \
                Result Time: not resulted in ED" % (row[0],)
                    colt = collect_time[:5]
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                         str(influenza_count) + '_name']] = \
                        "Resp Virus Complex"
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                         str(influenza_count) + '_testtype']] = "3"
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                         str(influenza_count) + '_cold']] = \
                        arrival_info[0]
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                         str(influenza_count) + '_colt']] = colt
                    data_to_write['Influenza Test #' + str(influenza_count)] = result
                data_to_write['Influenza Total Test #'] = str(influenza_count)

            # Xpert Influenza Testing
            cur.execute('''
                    SELECT time, type
                    FROM xpert
                    ''')
            data = cur.fetchone()
            if data is not None:
                nat_testing = True
                # other viruses were tested
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'othervir']] = "1"
                influenza_count += 1
                collect_time = data[0].split("-")[0].strip()
                result = "Test Name: Xpert Flu - Collect Time: %s Result Time: not \
                resulted in Timeline" % (collect_time,)
                colt = collect_time[:5]
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                     str(influenza_count) + '_name']] = \
                    "Xpert Flu Test"
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                     str(influenza_count) + '_testtype']] = "4"
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                     str(influenza_count) + '_cold']] = \
                    arrival_info[0]
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'flut' + \
                                     str(influenza_count) + '_colt']] = colt
                data_to_write['Influenza Test #' + str(influenza_count)] = result
                data_to_write['Influenza Total Test #'] = str(influenza_count)

            # Write number of influeza test done to import file
            if influenza_count != 0:
                data_to_write_csv[
                    header_locations[
                        'ps_edchrev' + visit_num + "_" + 'flutesting']] = "1"
                if influenza_count > 0:
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'flutests']] = \
                        str(influenza_count)
                if nat_testing and not resp_testing:
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'othervir_para'
                            ]] = "0"
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'othervir_rhino'
                            ]] = "0"
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'othervir_meta'
                            ]] = "0"
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'othervir_adeno'
                            ]] = "0"
            else:
                data_to_write_csv[
                    header_locations[
                        'ps_edchrev' + visit_num + "_" + 'othervir']] = "0"

            # Imaging done
            cur.execute('''
                    SELECT time, type
                    FROM imaging
                    ''')
            imaging_done = False
            data = cur.fetchall()
            if data != []:
                imaging_done = True
                for index, row in enumerate(data):
                    result_time = row[0]
                    imaging_type = row[1]
                    result = "Result Time: %s Imaging Type: %s" % \
                             (result_time, imaging_type)
                    data_to_write['Imaging #' + str(index + 1)] = result
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'chest']] = "1"
            else:
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'chest']] = "0"

            # Medications
            cur.execute('''
                    SELECT *
                    FROM medications
                    ''')
            abx_names = getcommonnames.getAbxNames()
            antiviral_names = getcommonnames.getAntiviralNames()
            abx_count = 0
            antiviral_count = 0

            data = cur.fetchall()
            if data != []:
                count = 1
                for row in data:
                    dose_time = row[0].split("-")[0]
                    medication_string = row[1].replace("(","").replace(")","").split(" ")
                    for medication in medication_string:
                        if medication in abx_names:
                            if abx_count > 4:
                                break
                            data_to_write_csv[
                                header_locations['ps_edchrev' + visit_num + "_" + 'ab_ed']] = "1"
                            result = "Antibiotic: %s Time Given: %s" % \
                                     (medication, dose_time)
                            data_to_write['Medication #' + str(count)] = result
                            abx_count = count
                            data_to_write_csv[
                                header_locations[
                                    'ps_edchrev' + visit_num + "_" + 'ab_ed' + \
                                    str(abx_count) + '_name'
                                    ]] = medication
                            if 'injection' in medication_string or \
                                            'premix' in medication_string or \
                                            'bag/given' in medication_string:
                                data_to_write_csv[
                                    header_locations[
                                        'ps_edchrev' + visit_num + "_" + 'ab_ed' + \
                                        str(abx_count) + 'route']] = "3"
                            if 'tablet' in medication_string:
                                data_to_write_csv[
                                    header_locations[
                                        'ps_edchrev' + visit_num + "_" + 'ab_ed' + \
                                        str(abx_count) + 'route']] = "1"
                            data_to_write_csv[
                                header_locations[
                                    'ps_edchrev' + visit_num + "_" + 'ab_ed' + \
                                    str(abx_count) + 'time']] = dose_time
                            data_to_write_csv[
                                header_locations[
                                    'ps_edchrev' + visit_num + "_" + 'ab_ed' + \
                                    str(abx_count) + 'date']] = \
                                arrival_info[0]
                            count += 1
                            break
                        if medication in antiviral_names:
                            if antiviral_count > 2:
                                break
                            data_to_write_csv[
                                header_locations['ps_edchrev' + visit_num + "_" + 'fluav']] = "1"
                            result = "Antiviral: %s Time Given: %s" % \
                                     (medication, dose_time)
                            data_to_write['Medication #' + str(count)] = result

                            antiviral_count = count - abx_count
                            count += 1
                            data_to_write_csv[
                                header_locations['ps_edchrev' + visit_num + "_" + 'fluav' + \
                                                 str(antiviral_count) + '_name']] = medication
                            data_to_write_csv[
                                header_locations['ps_edchrev' + visit_num + "_" + 'fluav' + \
                                                 str(antiviral_count) + 'time']] = dose_time
                            data_to_write_csv[
                                header_locations['ps_edchrev' + visit_num + "_" + 'fluav' + \
                                                 str(antiviral_count) + 'date']] = \
                                arrival_info[0]
                            break
            data_to_write['Total Antibiotics Given'] = str(abx_count)
            if data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'ab_ed']] == "1":
                data_to_write_csv[
                    header_locations[
                        'ps_edchrev' + visit_num + "_" + 'ab_ed_num']] = str(abx_count)
                data_to_write['Total Antivirals Given'] = str(antiviral_count)
            else:
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'ab_ed']] = "0"
            if data_to_write_csv[
                header_locations['ps_edchrev' + visit_num + "_" + 'fluav']] == "1":
                data_to_write_csv[
                    header_locations[
                        'ps_edchrev' + visit_num + "_" + 'fluavnum'
                        ]] = str(antiviral_count)
            else:
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'fluav']] = "0"
            # Cultures
            cur.execute('''
                    SELECT *
                    FROM cultures
                    ''')
            data = cur.fetchall()
            culture_count = 0
            if data != []:
                for index, row in enumerate(data):
                    pieces = row[1].split(" ")
                    culture_type = " ".join(pieces[:len(pieces) - 2])
                    result_string = "Time Ordered: %s Culture Type: %s" % \
                                    (row[0].split("-")[0], culture_type)
                    culture_count = index + 1
                    data_to_write['Culture #' + str(culture_count)] = result_string
                data_to_write['Total Cultures Done'] = str(culture_count)

            # Discharge Orders
            if data_to_write['Final Disposition'] == 'admitted':
                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'dabx']] = "98"

                data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'fluavdisc']] = "97"
            else:
                cur.execute('''
                        SELECT *
                        FROM discharge_orders
                        ''')
                abx_names = getcommonnames.getAbxNames()
                antiviral_names = getcommonnames.getAntiviralNames()
                abx_count = 0
                antiviral_count = 0

                data = cur.fetchall()
                if data:
                    count = 1
                    for row in data:
                        medication_string = row[1].replace("(","").replace(")","").split(" ")
                        for medication in medication_string:
                            if medication in abx_names:
                                if abx_count > 3:
                                    break
                                data_to_write_csv[
                                    header_locations['ps_edchrev' + visit_num + "_" + 'dabx']] = "1"
                                result = "Antibiotic: %s" % \
                                         (medication,)
                                data_to_write['Discharge Antibiotic #' + str(count)] = result
                                abx_count = count
                                data_to_write_csv[
                                    header_locations[
                                        'ps_edchrev' + visit_num + "_" + 'dabx' + \
                                        str(abx_count) + 'name'
                                        ]] = medication
                                count += 1
                                break
                            if medication in antiviral_names:
                                if antiviral_count > 2:
                                    break
                                data_to_write_csv[
                                    header_locations['ps_edchrev' + visit_num + "_" + 'fluavdisc']] = "1"
                                result = "Antiviral: %s" % \
                                         (medication,)
                                data_to_write['Discharge Antiviral #' + str(count)] = result

                                antiviral_count = count - abx_count
                                count += 1
                                data_to_write_csv[
                                    header_locations['ps_edchrev' + visit_num + "_" + 'fluavdisc' + \
                                                     str(antiviral_count)]] = medication
                                break
                data_to_write['Total Discharge Antibiotics Given'] = str(abx_count)
                data_to_write['Total Discharge Antivirals Given'] = str(antiviral_count)
                if data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'dabx']] == "1":
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'abxquant']] = str(abx_count)

                else:
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'dabx']] = "0"
                if data_to_write_csv[
                    header_locations['ps_edchrev' + visit_num + "_" + 'fluavdisc']] == "1":
                    data_to_write_csv[
                        header_locations[
                            'ps_edchrev' + visit_num + "_" + 'fluavdiscct'
                            ]] = str(antiviral_count)
                else:
                    data_to_write_csv[
                        header_locations['ps_edchrev' + visit_num + "_" + 'fluavdisc']] = "0"

            for item in data_to_write:
                outfile.write(item + ": " + data_to_write[item] + "\n")
            # Instructions for what to manually seach for
            outfile.write("\n\n\n\n\n          Manual Chart Review \n")
            outfile.write("---------------------------------------------------------\n")
            always_search = '''
                Pharyngeal Erythema
                Cervical Lymphadenopathy
                Antiviral Prescriptions
                Antibiotic Prescriptions
                Final Diagnoses'''
            outfile.write('Variables needing lookup:\n %s\n' % \
                          always_search)

            if gcs == "No Score In Timeline":
                outfile.write('    AMS: Was subject altered in ED?\n')
            if abx_count > 0:
                outfile.write('    Antibiotics Indications for abx given in ED\n')
            if culture_count > 0:
                outfile.write('    Cultures Results for cultures done in ED\n')
            if imaging_done:
                outfile.write("    Imaging Results Results for Chest X-ray/CT done in ED\n")
            if oxygen_type != 'None Given':
                outfile.write('    Oxygen Supplmentation at discharge\n')
            # move back to proper place for appending data after reading data
            outfile.close()
            print "finish writing data for %s ED Visit: %s" % (subject_file, visit_num)
        # Write row for Subject to Importing File
        importfile.seek(0, 2)
        csv_writer.writerow(data_to_write_csv)
        print("Finished writing data to import file for subject {}".format(subject_id))
    importfile.close()
    print("Finished writeing source documents and import files")


def main():
    import process
    directories = process.getDirectories()
    clean = directories['Clean Dir']
    import_dir = directories['Import Dir']
    log_dir = directories['Log Dir']
    source_dir = directories['Source Dir']

    ids = process.getIds(directories['Clean Dir'])
    createSourceFromData(ids, clean, source_dir, import_dir, log_dir)


if __name__ == "__main__":
    main()
