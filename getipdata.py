from collections import defaultdict
import os
import process


def getVitals(subject_files_dir):
    current_files = os.listdir(subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("Vitals") != -1
                     ]
    print("Vitals Files are", subject_files)
    vitalpatterns = ['Temperature', 'Heart Rate', 'BP (cuff)', 'GCS', 'SpO2',
                     'O2 Device']

    for item in subject_files:
        with open(item, 'r') as thefile:
            for line in thefile:
                for pat in vitalpatterns:
                    if line.find(pat) != -1:
                        if pat == "BP (cuff)":
                            print pat
                            data = line.split()
                            data = [item
                                    for item in data
                                    if item.split("/")[0].isdigit()]
                            print data
                        else:
                            print pat
                            data = line.split()
                            data = [item
                                    for item in data
                                    if item.replace(".", "").isdigit()]
                            print data


def getLabs(subject_files_dir):
    current_files = os.listdir(subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("Labs") != -1
                     ]
    print("Lab Files are", subject_files)
    for item in subject_files:
        with open(item, 'r') as thefile:
            testfile = iter(list(thefile))
            datapoints = defaultdict(dict)
            line = testfile.next().strip()
            while line != "CBC":
                line = testfile.next().strip()
            header = line
            for line in testfile:
                line = line.strip()
                if line == "":
                    continue
                possible_lab_start = line
                possible_value = testfile.next().strip()
                possible_lab_end = testfile.next().strip()

                if possible_value == "" and possible_lab_end == "":
                    header = possible_lab_start
                    continue
                if possible_lab_start == possible_lab_end:
                    datapoints[header][possible_lab_start] = possible_value

            for key, value in datapoints.iteritems():
                print key
                print value
                print "__________________________________________"
                print


def getMeds(subject_files_dir):
    current_files = os.listdir(subject_files_dir)
    subject_files = [subject_file
                     for subject_file in current_files
                     if subject_file.find("IP") != -1
                     if subject_file.find("Medications") != -1
                     ]
    print("Med Files are", subject_files)
    for item in subject_files:
        with open(item, 'r') as thefile:
            testfile = iter(list(thefile))
            datapoints = defaultdict(dict)
            line = testfile.next()

            while not line.startswith("Medications"):
                line = testfile.next()
                # start scheduled meds
            meds = defaultdict(dict)
            med_name = None
            count = 0

            for line in testfile:
                line = line.strip()
                if line == "":
                    continue
                if med_name == None:
                    med_name = line
                    med_dose_info = testfile.next().strip()
                    med_time_info = testfile.next().strip()
                    line = testfile.next().strip()
                meds[med_name] = {'dosage': med_dose_info,
                                  'time': med_time_info,
                                  'admins': []}
                # look for administrations
                while True:
                    if line == "":
                        line = testfile.next().strip()
                        continue
                    if line.isdigit():
                        meds[med_name]['admins'].append(line)
                        line = testfile.next().strip()
                        continue
                    # dced and cancelled
                    if line.find("D/C'd") != -1 or line.find("[C]") != -1:
                        line = testfile.next().strip()
                        continue
                    # held
                    if line.replace(")", "").replace("(", "").isdigit():
                        line = testfile.next().strip()
                        continue
                    # done
                    if line.startswith("Medications"):
                        break
                    # given med
                    if meds.get(line, None) != None:
                        med_name = line + str(count)
                        count += 1
                    else:
                        med_name = line
                    med_dose_info = testfile.next().strip()
                    med_time_info = testfile.next().strip()
                    break

                if line.startswith("Medications"):
                    print "lets stop here"
                    break
            for key, value in sorted(meds.iteritems()):
                print key, meds[key]['time'], meds[key]['dosage'], \
                    meds[key]['admins']
                print


def main():
    directories = process.getDirectories()
    ip_subject_files_dir = directories['IP Subject Dir']
    getVitals(ip_subject_files_dir)
    getMeds(ip_subject_files_dir)
    getLabs(ip_subject_files_dir)


if __name__ == "__main__":
    main()




