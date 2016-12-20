from collections import defaultdict
import os
import process
import re


def makePattern(pattern):
    template = r'({pattern}[ \n]+)(.*?)({pattern}[ \n]+)'.format(pattern=pattern)
    return re.compile(template,flags=re.IGNORECASE | re.DOTALL )

def getVitals(ip_subject_files_dir):
    current_files = os.listdir(ip_subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("Vitals") != -1
                     if subject_file.find(".txt") != -1
                     ]
    print("Vitals Files are", subject_files)
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
            print "starting vitals search for", thefile
            data = thefile.read().decode('ascii','ignore').split("\n")
            for word, result_pattern in search_words:
                print "looking for ", word
                text = iter(data)
                for line in text:
                    line = line.strip()
                    if line != word:
                        continue
                    if line == word:
                        print "starting search for", word
                        line = text.next()
                        line = line.strip()
                        try:
                            while True:
                                result_match = result_pattern.search(line)
                                if result_match is not None:
                                    time = result_match.group(1)
                                    value = result_match.group(2)
                                    print time, value
                                line = text.next()
                                line = line.strip()
                                if line in stop_words:
                                    break
                            break
                        except StopIteration:
                            print "End of file all done"

            # for key, value in datapoints.iteritems():
            #     values = value.split("\n")
            #     values = [value
            #               for value in values
            #               if value.strip() != ""
            #               if value.find("2016") == -1
            #               if value.find("Page") == -1
            #               if value.find("blank") == -1
            #               ]
            #     print("Key: {}".format(key))
            #     print("Value: {}".format(values))
            #     print "__________________________________________"
            #     print



def getLabs(ip_subject_files_dir):
    relevant_labs = ['CBC', 'CMP','XR Chest','Influenza A/B + RSV','CT Chest', 'Basic metabolic panel', 'Culture']
    current_files = os.listdir(ip_subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("Labs") != -1
                     if subject_file.find(".txt") != -1
                     ]
    print("Lab Files are", subject_files)
    result_pattern = re.compile(r'(Summary for.*?)(Lab and Collection.*?)(Result )', flags=re.DOTALL)

    for subject_file in subject_files:
        with open(ip_subject_files_dir + os.sep + subject_file,'r') as thefile:
            print "starting lab search for", thefile
            data = thefile.read().decode('ascii','ignore')
            result_matches = result_pattern.findall(data)
            for match in result_matches:
                for word in relevant_labs:
                    if word in match[1]:
                        print match[0]
                        print match[1]

            # for key, value in datapoints.iteritems():
            #     values = value.split("\n")
            #     values = [value
            #               for value in values
            #               if value.strip() != ""
            #               if value.find("2016") == -1
            #               if value.find("Page") == -1
            #               if value.find("blank") == -1
            #               ]
            #     print("Key: {}".format(key))
            #     print("Value: {}".format(values))
            #     print "__________________________________________"
            #     print


def getMeds(ip_subject_files_dir):
    import getcommonnames
    antibiotics = getcommonnames.getAbxNames()
    antivirals = getcommonnames.getAntiviralNames()

    current_files = os.listdir(ip_subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("Medications") != -1
                     if subject_file.find(".txt") != -1
                     ]
    print("Med Files are", subject_files)
    for subject_file in subject_files:
        with open(ip_subject_files_dir + os.sep + subject_file, 'r') as thefile:
            print "starting med search for", thefile
            medications = defaultdict(list)
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
                        print med
                        print admins
                        print "______________"



def main():
    directories = process.getDirectories()
    ip_subject_files_dir = directories['IP Subject Dir']
    getVitals(ip_subject_files_dir)
    # getMeds(ip_subject_files_dir)
    # getLabs(ip_subject_files_dir)


if __name__ == "__main__":
    main()