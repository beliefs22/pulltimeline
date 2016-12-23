from collections import defaultdict
import os
import process
import re
import insertdataip
import makeiptables
import logging
import createsource_ip


def makePattern(pattern):
    template = r'({pattern}[ \n]+)(.*?)({pattern}[ \n]+)'.format(pattern=pattern)
    return re.compile(template,flags=re.IGNORECASE | re.DOTALL )

def getVitals(ip_subject_files_dir, subject_id, conn):
    current_files = os.listdir(ip_subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("Vitals") != -1
                     if subject_file.find(".txt") != -1
                     if subject_file.find(subject_id) != -1
                     ]
    bp_pat = re.compile(r'(\d+/\d+/\d+ \d{4}\s+)(\d{2,3}/\d{2,3})')
    heart_rate_pat = re.compile(r'(\d+/\d+/\d+ \d{4}\s+)(\d{2,3}\s+)')
    resp_pat = re.compile('(\d+/\d+/\d+ \d{4}\s+)(\d{1,2}\s+)')
    spo2_pat = re.compile(r'(\d+/\d+/\d+ \d{4}\s+)(\d{2,3}\s+%)')
    temp_pat = re.compile(r'(\d+/\d+/\d+ \d{4}\s+)(\d{2}\W{0,1}\d{0,1}\s+C)')
    search_words = [('BP',bp_pat), ('Heart Rate', heart_rate_pat), ('Resp', resp_pat), ('Temp', temp_pat),
                    ('SpO2', spo2_pat)]
    stop_words = ['BP', 'Heart Rate', 'Temp', 'Temp src', 'Resp', 'SpO2']
    for subject_file in subject_files:
        with open(ip_subject_files_dir + os.sep + subject_file,'r') as thefile:
            results = defaultdict(list)
            print "starting vitals search for {}".format('01-11-A-' + subject_id)
            data = thefile.read().decode('ascii','ignore').split("\n")
            for vital_type, result_pattern in search_words:
                text = iter(data)
                for line in text:
                    line = line.strip()
                    if line != vital_type:
                        continue
                    if line == vital_type:
                        line = text.next()
                        line = line.strip()
                        try:
                            while True:
                                result_match = result_pattern.search(line)
                                if result_match is not None:
                                    time = result_match.group(1)
                                    value = result_match.group(2)
                                    results[vital_type].append((time, value))
                                line = text.next()
                                line = line.strip()
                                if line in stop_words:
                                    break
                            break
                        except StopIteration:
                            print "End of file all done"
            vitals_insert = insertdataip.InsertVitalData(conn, results)
            vitals_insert.extractData()
            # for key, value in results.iteritems():
            #     print key
            #     print value
            #     print "______________________"


def getLabs(ip_subject_files_dir, subject_id, conn):
    relevant_labs = ['cbc', 'cmp','xr chest','influenza a/b + rsv','ct chest', 'basic metabolic panel', 'culture',
                     'blood gases', 'resp virus complex']
    relevant_cultures = ['bacterial/yeast culture, blood', 'bacterial culture/smear, respiratory',
                         'viral culture, respiratory', 'bacterial culture, urine' ]
    current_files = os.listdir(ip_subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("Labs") != -1
                     if subject_file.find(".txt") != -1
                     if subject_file.find(subject_id) != -1
                     ]
    result_pattern = re.compile(r'(Summary for.*?)(Lab and Collection.*?)(Result )', flags=re.DOTALL)

    for subject_file in subject_files:
        with open(ip_subject_files_dir + os.sep + subject_file,'r') as thefile:
            results = defaultdict(list)
            print "starting lab search for {}".format('01-11-A-' + subject_id)
            data = thefile.read().decode('ascii','ignore')
            result_matches = result_pattern.findall(data)
            for match in result_matches:
                # lab string contains lab name and date lab was done
                lab_string = match[1]
                for lab in relevant_labs:
                    lab_string = lab_string.lower()
                    if lab in lab_string:
                        if lab == 'culture':
                            for culture_type in relevant_cultures:
                                if culture_type in lab_string:
                                    lab = culture_type
                        lab_type = lab
                        component = match[0]
                        time = lab_string
                        results[lab_type].append((component, time))
            lab_insert = insertdataip.InsertLabData(conn, results)
            lab_insert.extractData()
            # for key, value in results.iteritems():
            #     print key
            #     for component, time in value:
            #         print "component", component
            #         print "time", time
            #         print "------------------------"
            #     print "______________________"




def getMeds(ip_subject_files_dir, subject_id, conn):
    import getcommonnames
    antibiotics = getcommonnames.getAbxNames()
    antivirals = getcommonnames.getAntiviralNames()

    current_files = os.listdir(ip_subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("Medications") != -1
                     if subject_file.find(".txt") != -1
                     if subject_file.find(subject_id) != -1
                     ]
    for subject_file in subject_files:
        with open(ip_subject_files_dir + os.sep + subject_file, 'r') as thefile:
            print "starting med search for {}".format('01-11-A-' + subject_id)
            medications = defaultdict(list)
            results = defaultdict(list)
            data = thefile.read().decode('ascii','ignore').split("\n")
            data = [line
                    for line in data
                    if line.strip() != ""
                    ]
            data = iter(data)
            # skip first line
            data.next()
            line = data.next()
            while not line.startswith("\t"):
                med_name = line
                line = data.next()
                try:
                    while line.startswith("\t"):
                        if line.find('Given') != -1:
                            medications[med_name].append(line)
                        line = data.next()
                except StopIteration:
                    print "Finished End of File"

            for med_string, admins in medications.iteritems():
                med_string = med_string.split()
                for med in med_string:
                    med = med.lower()
                    if med in antivirals or med in antibiotics or med == 'influenza':
                        results[med] += admins
                        break
            med_insert = insertdataip.InsertMedicationData(conn, results)
            med_insert.extractData()
            # for key, value in results.iteritems():
            #     print key
            #     print value
            #     print "______________________"


def getDisposition(ip_subject_files_dir, subject_id, conn):

    current_files = os.listdir(ip_subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("dispo") != -1
                     if subject_file.find(".txt") != -1
                     if subject_file.find(subject_id) != -1
                     ]
    for subject_file in subject_files:
        with open(ip_subject_files_dir + os.sep + subject_file, 'r') as thefile:
            results = defaultdict(list)
            print "starting dispo search for {}".format('01-11-A-' + subject_id)
            transfer_in = False
            discharge = False
            for line in thefile:
                line = line.decode('ascii', 'ignore')
                if line.find('Transfer In') != -1 and transfer_in == False:
                    results['admit'].append(line)
                    transfer_in = True
                if line.find('Discharge') != -1 and discharge == False:
                    results['discharge'].append(line)
                    discharge = True
            dispo_insert = insertdataip.InsertDispositionData(conn, results)
            dispo_insert.extractData()
            # for key, value in results.iteritems():
            #     print key
            #     print value
            #     print "______________________"


def getData():
    directories = process.getDirectories()
    ip_subject_file_dir = directories['IP Subject Dir']
    cleaned_file_dir = directories['Clean Dir']
    subject_ids = process.getIds(ip_subject_file_dir)
    source_files_dir = directories['Source Dir']
    import_files_dir = directories['Import Dir']
    for base_id in subject_ids:
        subject_id = '01_11_A_IP1_' + base_id
        print "starting data extraction for {}".format(subject_id)
        # conn = makeiptables.maketables(subject_id, cleaned_file_dir)
        # getVitals(ip_subject_file_dir, base_id, conn)
        # getLabs(ip_subject_file_dir, base_id, conn)
        # getMeds(ip_subject_file_dir, base_id, conn)
        # getDisposition(ip_subject_file_dir, base_id, conn)
    createsource_ip.createSourceFromData(subject_ids,cleaned_file_dir,source_files_dir, import_files_dir, logfile=None)





def main():
    directories = process.getDirectories()
    ip_subject_files_dir = directories['IP Subject Dir']
    getVitals(ip_subject_files_dir)
    # getMeds(ip_subject_files_dir)
    # getLabs(ip_subject_files_dir)


if __name__ == "__main__":
    main()