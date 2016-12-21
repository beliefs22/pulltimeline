import re
import logging
from datetime import datetime
from collections import defaultdict

def makeNumPattern(word):
    """Helper function to create patterns used to find results.
    Args:
        word (str): result to search for
    """
    #re structure finds - White Blood Count: 3.78/37/Thirtyseven
    template = r"(%s:\s*)(\d+\W{0,1}\d*)|(%s:\s+$)|(%s:\s*)([\(\w\s\)]+)"
    return re.compile(template % (word, word, word),re.M)

def makeSearchPattern(word):
    """Helper function to make patterns used tostart result search
    Args:
        word (str): word that triggers search
    """
    #re structure finds - 12:38  Vitals Assessment or 12:39:12  Assessment
    template = r"(\d+:\d+:*\d*\s*)(%s)"
    return re.compile(template % (word,),re.M)

def makeStringPattern(word):
    """Helper function to create patterns used to find string results

    Args:
        word (str): result to search for
    """
    #re structure finds - Influenza A NAT: No Detected
    template = r"(%s:\s*)([\w\s]+)"
    return re.compile(template % (word,),re.M)

class InsertData(object):
    """Base Class for inserting data into the databse created for each subject"""
    def __init__(self, conn, data):
        self.conn = conn
        self.data = data
        self.cur = self.conn.cursor()



class InsertVitalData(InsertData):
    def __init__(self, conn, data):
        InsertData.__init__(self, conn, data)


    def extractData(self):
        for vital_type, values in self.data.iteritems():
            for pair in values:
                time = pair[0]
                value = pair[1]
                self.cur.execute('''
                INSERT INTO vitals
                (time, type, value)
                VALUES
                (?, ?, ?)
                ''', (time, vital_type, value))

                self.conn.commit()




class InsertLabData(InsertData):
    def __init__(self, conn, data):
        InsertData.__init__(self, conn, data)
        self.search_words = ['White Blood Cell Count','Hematocrit','Sodium', 'Potassium',
                             'pH, Non-Arterial', 'pH, Arterial', 'Urea Nitrogen', 'Influenza A NAT',
                             'RSV NAT', 'Rhinovirus NAT', 'Parainfluenzae 1 NAT','Parainfluenzae 2 NAT',
                             'Parainfluenzae 3 NAT', 'Metapneumo NATT','Adenovirus NAT', 'Blood Culture\n\t',
                             'Urine Culture\n\t', 'Respiratory Culture\n\t', 'Viral Culture, Respiratory\n\t',
                             'Blood Culture \(High Panic\)\n\t']

    def extractData(self):
        date_pat = re.compile(r'(\d+/\d+/\d+)')
        result_pattern_template = r'({}.*?$)'
        for lab_type, values in self.data.iteritems():
            for pair in values:
                time = pair[1]
                date_match = date_pat.search(time)
                if date_match:
                    time = date_match.group(1)
                value = pair[0]
                component_results = defaultdict(list)
                for word in self.search_words:
                    search_pattern = re.compile(result_pattern_template.format(word),
                                                flags=re.MULTILINE | re.IGNORECASE)
                    match = search_pattern.search(value)
                    if match:
                        component_results[lab_type].append(match.group(1))
                value = ",".join(component_results[lab_type])
                if value == "":
                    value = pair[0]
                self.cur.execute('''
                INSERT INTO labs
                (time, type, value)
                VALUES
                (?, ?, ?)
                ''', (time, lab_type, value))

                self.conn.commit()


class InsertMedicationData(InsertData):
    def __init__(self, conn, data):
        InsertData.__init__(self, conn, data)
        pass

    def extractData(self):
        for med_type, values in self.data.iteritems():
            time = ",".join(values)
            self.cur.execute('''
                INSERT INTO medications
                (time, type)
                VALUES
                (?, ?)
                ''', (time, med_type))

            self.conn.commit()

class InsertDispositionData(InsertData):
    def __init__(self, conn, data):
        InsertData.__init__(self, conn, data)
        pass

    def extractData(self):
        for dispo_type, values in self.data.iteritems():
            time = ",".join(values)
            self.cur.execute('''
                INSERT INTO dispo
                (time, type)
                VALUES
                (?, ?)
                ''', (time, dispo_type))

            self.conn.commit()