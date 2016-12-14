from collections import defaultdict
import os
import process
import re


def makePattern(pattern):
    template = r'({pattern}[ \n]+)(.*?)({pattern}[ \n]+)'.format(pattern=pattern)
    return re.compile(template,flags=re.IGNORECASE | re.DOTALL )

def getVitals(subject_files_dir):
    current_files = os.listdir(subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("Vitals") != -1
                     if subject_file.find(".txt") != -1
                     ]
    print("Vitals Files are", subject_files)
    search_words = ['Temperature', 'Heart Rate', 'BP \(cuff\)', 'GCS', 'SpO2',
                     'O2 Device', r'O2 Flow Rate \(L/min\)'
                    ]
    search_patterns = [makePattern(word)
                       for word in search_words
                       ]
    for item in subject_files:
        item = subject_files_dir + os.sep + item
        with open(item, 'r') as thefile:
            print("starting vitals search for {}".format(item))
            data = thefile.read()
            datapoints = defaultdict(dict)
            for pattern in search_patterns:
                result_match = pattern.search(data)
                if result_match is not None:
                    vital_type = result_match.group(1)
                    vital_result = result_match.group(2)
                    datapoints[vital_type] = vital_result

            for key, value in datapoints.iteritems():
                values = value.split("\n")
                values = [value
                          for value in values
                          if value.strip() != ""
                          if value.find("2016") == -1
                          if value.find("Page") == -1
                          if value.find("blank") == -1
                          ]
                print("Key: {}".format(key))
                print("Value: {}".format(values))
                print "__________________________________________"
                print



def getLabs(subject_files_dir):
    search_words = ['White Blood Cell Count', 'Urea Nitrogen','Sodium', 'Glucose',
                    'Hematocrit','Bacterial Culture/Smear, Respiratory', 'Gram Stain',
                    'Viral Culture, Respiratory','Influenza A NAT', 'Influenza B NAT',
                    'Parainfluenza 1 NAT','Parainfluenza 2 NAT', 'Parainfluenza 3 NAT',
                    'RSV NAT', 'Resp Virus Complex, NP Swab', 'Adenovirus NAT',
                    'Metapneumovirus NAT', 'pH, Non-Arterial', 'pH, Arterial'
                    ]
    search_patterns = [makePattern(word)
                       for word in search_words
                       ]

    current_files = os.listdir(subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("Labs") != -1
                     if subject_file.find(".txt") != -1
                     ]
    print("Lab Files are", subject_files)
    for item in subject_files:
        item = subject_files_dir + os.sep + item
        with open(item, 'r') as thefile:
            print("starting lab search for {}".format(item))
            data = thefile.read()
            datapoints = defaultdict(dict)
            for pattern in search_patterns:
                result_match = pattern.search(data)
                if result_match is not None:
                    lab_type = result_match.group(1)
                    lab_result = result_match.group(2)
                    datapoints[lab_type] = lab_result

            for key, value in datapoints.iteritems():
                values = value.split("\n")
                values = [value
                          for value in values
                          if value.strip() != ""
                          if value.find("2016") == -1
                          if value.find("Page") == -1
                          if value.find("blank") == -1
                          ]
                print("Key: {}".format(key))
                print("Value: {}".format(values))
                print "__________________________________________"
                print


def getMeds(subject_files_dir):
    import getcommonnames
    antibiotics = getcommonnames.getAbxNames()
    antivirals = getcommonnames.getAntiviralNames()

    med_pattern = re.compile(r'^([a-z]+.*?\n)(Dose:.*?\n)(Last Dose:.*\n){0,1}(Start: .*? End: .*?$)', flags= re.I | re.M)
    #pattern to remove words that are added when you save document
    patterns_to_remove = [r'(Page.*?$)', r'(about:blank)', r'^(\d{1,2}/\d{1,2}/\d{4})']
    patterns_to_remove = [re.compile(pattern, flags= re.I | re.M)
                          for pattern in patterns_to_remove]
    current_files = os.listdir(subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("Medications") != -1
                     if subject_file.find(".txt") != -1
                     ]
    print("Med Files are", subject_files)
    for item in subject_files:
        item = subject_files_dir + os.sep + item
        with open(item, 'r') as thefile:
            item = subject_files_dir + os.sep + item
            print("starting Med search for {}".format(item))
            data = thefile.read()
            for pattern in patterns_to_remove:
                data = pattern.sub("",data)
            datapoints = defaultdict(dict)
            matches = med_pattern.findall(data)
            for match in matches:
                name, dose, lastdose, time = match
                for item in name.split():
                    if item in antibiotics:
                        print("Abx - Name: {} Dose: {} Last Dose: {}\n Time: {}".format(name, dose, lastdose, time))
                    if item in antivirals:
                        print("Antiviral - Name: {} Dose: {} Last Dose: {}\n Time: {}".format(name, dose, lastdose, time))



def main():
    directories = process.getDirectories()
    ip_subject_files_dir = directories['IP Subject Dir']
    getVitals(ip_subject_files_dir)
    getMeds(ip_subject_files_dir)
    getLabs(ip_subject_files_dir)


if __name__ == "__main__":
    main()