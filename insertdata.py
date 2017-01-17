import re
import logging
import getcommonnames


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
    """Base Class for inserting data into the databse created for each subject

    Attr:
        matchpatterns (list): re pattern objects to identify results
        headerpatterns (list): re pattern objects used to start result search
        stop (str): determines when to stop result search
        tablename (str): table data should be inserted into
    Args:
        cur (sqlite3 cursor): cur connected to database to insert data into
        data (list): lines from text file containing subject information
    """
        
    def __init__(self, conn, data, logfilename):
        
        #connection to the database for a unique subject
        self.connection = conn
        #cursor in that database connection
        self.cursor = self.connection.cursor()
        #list of data created from the text file containing subjects data
        self.data = data
        #words that indicate a result was found
        self.resultpatterns = []
        #words that indicate to start search for reseults
        self.startpatterns = []
        #word that indicates to stop result search
        self.stop_word = ""
        #table name used in create and update functions
        self.tablename = ""
        #logging file
        self.logfilename = logfilename
        logging.basicConfig(filename=self.logfilename,level=logging.DEBUG)

    def create_row(self, row_id):
        """Creates base entry into the table to be updated as results are found
        Args:
            row_id (str): uniqe timepoint for the result
        """
        row_id = unicode(row_id.strip().decode('ascii','ignore'))
        template = '''
        Insert INTO %s
        (time)
        VALUES (?)''' % self.tablename
        logging.debug(
            "Created row in table %s with id %s" % (self.tablename,row_id,))
        self.cursor.execute(template, (row_id,))
        self.connection.commit()

    def update_row(self,field,value,row_id):
        """Updates the row with given paramaters with given value
        Args:
            field (str): field to update
            value (str): value to update field to
            row_id (str): record to update
        """
        value = unicode(value.strip().decode('ascii','ignore'))
        row_id = unicode(row_id.strip().decode('ascii','ignore'))
        field = field.lower().strip().replace(" ","_").replace(":","")
        field = field.replace(",","_").replace("/","").replace("(","")
        field = field.replace(")","").replace("-","_").replace("__","_")
        field = unicode(field.decode('ascii','ignore'))
        logging.debug(
            "row_id is %s field name is %s value is %s" % (
                row_id, field, value))
        #insert given field and value into the template
        template = '''
        Update %s
        Set %s= ?
        Where time= ?''' %\
        (self.tablename, field)
        self.cursor.execute(template,(value, row_id))
        self.connection.commit()


class InsertOtherData(InsertData):
    """Base class for inserting non lab value results like vitals"""

    def __init__(self,conn, data, logfilename):
        InsertData.__init__(self, conn, data, logfilename)
    
    def extractData(self):
        """Inserts data into the appropriate table for the inheriting class"""
        logging.debug("starting other insert")
        #create iterator of data so we can move through lines as we want
        data = iter(self.data)
        #loop through each line of the file to seach for relevant words
        #Count is used to identify uniqe time points for differen types
        count = 0
        for line in data:
            #look for a line that indicates to start search
            for pattern in self.startpatterns:
                startmatch = pattern.search(line)
                #found a word - start search
                if startmatch is not None:
                    #uniqe timepoint for the entry
                    row_id = startmatch.group(1) + "-" + str(count)
                    count += 1
                    self.create_row(row_id)
                    #look for results until you see stop keyword
                    while line.find(self.stop_word) == -1:
                        #Only true if we need to look for results at end of line
                        new_line = False
                        for pattern2 in self.resultpatterns:
                            resultmatch = pattern2.search(line)
                            #determine which match group the pattern found
                            if resultmatch is not None:
                                #Temp: 37.4
                                if resultmatch.group(1) and\
                                   resultmatch.group(2):
                                    field_name = resultmatch.group(1)
                                    value = resultmatch.group(2)
                                    self.update_row(field_name, value, row_id)
                                
                                #Temp: end of line
                                elif resultmatch.group(3):
                                    valuept = re.compile(r'(^\s*\d+\S{0,1}\d*)')
                                    field_name = resultmatch.group(3).strip()
                                    #indicate you moved to new line
                                    if new_line == False:
                                        next_line = data.next()
                                        new_line = True
                                    valuesearch = valuept.search(next_line)
                                    if valuesearch is not None:
                                        value = valuesearch.group(1).strip()
                                        self.update_row(field_name,value,row_id)
                                elif resultmatch.group(4) and\
                                     resultmatch.group(5):
                                    field_name = resultmatch.group(4)
                                    value = resultmatch.group(5)
                                    self.update_row(field_name,value,row_id)                                    
                        #move to next line if you didn't see stop keyword
                        if new_line == False:
                            line = data.next()
                        else:
                            line = next_line


class Medications(InsertData):
    """Inserts Medications Given during stay"""
    def __init__(self, conn, data, logfilename):
        InsertData.__init__(self, conn, data, logfilename)

        self.startpatterns = [makeSearchPattern(word)
                               for word in ['Medication Given',
                                            'Medication New Bag/Given',
                                            'Medication New']
                               ]
        self.stop_word1 = 'Dose:'
        self.stop_word2 = 'Route:'
        self.tablename = 'medications'
        
    def extractData(self):
        """Inserts data into the appropriate table for the inheriting class"""
        logging.debug("starting medication insert")
        #create iterator of data so we can move through lines as we want
        data = iter(self.data)
        #loop through each line of the file to search for relevant words
        previous_line = ""
        #Count is used to identify unique time points
        count = 0
        for line in data:
            previous_line = line
            row_id = None
            #look for a line that indicates to start search
            for pattern in self.startpatterns:                
                startmatch = pattern.search(line)
                #found a word - start search
                if startmatch is not None:
                    #uniqe timepoint for the entry
                    row_id = startmatch.group(1).strip() + "-" + str(count)
                    count += 1
                    self.create_row(row_id)
                if row_id is not None:
                    med_result_pattern = re.compile(
                        r'(Medication Given)(.*?)(Dose:)|\
(Medication Given)(.*?)(Route:)|(Medication New)(.*?)(Dose:)|\
(Medication New)(.*?)(Route:)')
                    while line.find(self.stop_word1) == -1 and\
                          line.find(self.stop_word2) == -1:
                        line = data.next()
                    results = med_result_pattern.search(line)
                    
                    if results is not None:
                        #Lines = "Medication given ..... Dose:
                        if results.group(1) is not None and \
                                        results.group(2) is not None and \
                                        results.group(3) is not None:
                            field = 'name'
                            value = results.group(2).lower() +\
                                    previous_line.lower()
                            self.update_row(field, value.strip(), row_id)
                        #Lines = "Medication given ..... Route:
                        if results.group(4) is not None and \
                                        results.group(5) is not None and \
                                        results.group(6) is not None:
                            field = 'name'
                            value = results.group(5).lower() +\
                                    previous_line.lower()
                            self.update_row(field, value.strip(), row_id)
                        #Lines = "Medication New ..... Dose:
                        if results.group(7) is not None and \
                                        results.group(8) is not None and \
                                        results.group(9) is not None:
                            field = 'name'
                            value = results.group(8).lower() +\
                                    previous_line.lower()
                            self.update_row(field, value.strip(), row_id)
                        #Lines = "Medication New ..... Dose:
                        if results.group(10) is not None and \
                                        results.group(11) is not None and \
                                        results.group(12) is not None:
                            field = 'name'
                            value = results.group(11).lower() +\
                                    previous_line.lower()
                            self.update_row(field, value.strip(), row_id)
                    else:
                        #Line = .... Dose: or ....Route:
                        med_result_pattern = re.compile(
                            r'(\A.*?)(Dose:)|(\A.*?)(Route:)')
                        results = med_result_pattern.search(line)
                        if results is not None:
                            if results.group(1) is not None and \
                                            results.group(2) is not None:
                                field = 'name'
                                value = results.group(1).lower() +\
                                        previous_line.lower()
                                self.update_row(field, value.strip(), row_id)
                            if results.group(3) is not None and \
                                            results.group(4) is not None:
                                field = 'name'
                                value = results.group(3).lower() +\
                                        previous_line.lower()
                                self.update_row(field, value.strip(), row_id)                            
                
                            
class InsertLabData(InsertData):
    """Base class for inserting lab data with numerical results"""

    def __init__(self, conn, data, logfilename):
        InsertData.__init__(self, conn, data, logfilename)
    
    def extractData(self):
        """Inserts data into the appropriate table for the inheriting class"""
        logging.debug("starting lab data insert")
        #create iterator of data so we can move through lines as we want
        data = iter(self.data)
        #loop through each line of the file to seach for relevant words
        count = 0
        for line in data:
            #look for a line that indicates to start search
            for pattern in self.startpatterns:
                startmatch = pattern.search(line)
                #found a word - start search
                if startmatch is not None:
                    #container for results to add once you identify row_id
                    toadd = []
                    #Pulls row_id when you see the collected key word
                    stopmatch = re.compile(
                        r'(Collected:\s*)(\d+/\d+/\d+\s*\d+:\d+)')
                    if line.find(self.stop_word) != -1:
                        for pattern2 in self.resultpatterns:
                            resultmatch = pattern2.search(line)
                            if resultmatch is not None:
                                if resultmatch.group(1) and\
                                   resultmatch.group(2):
                                    toadd.append((resultmatch.group(1),
                                                  resultmatch.group(2)))
                    else:
                        #start looking for results until you reach stop keyword
                        while line.find(self.stop_word) == -1:
                            for pattern2 in self.resultpatterns:
                                resultmatch = pattern2.search(line)
                                #White Blood Count: 3.78
                                if resultmatch is not None:
                                    if resultmatch.group(1) and\
                                       resultmatch.group(2):
                                        toadd.append((resultmatch.group(1),
                                                      resultmatch.group(2)))
                            #move to next line for while loop

                            line = data.next()
                        for pattern3 in self.resultpatterns:
                            resultmatch = pattern3.search(line)
                            if resultmatch is not None:
                                if resultmatch.group(1) and\
                                   resultmatch.group(2):
                                    toadd.append((resultmatch.group(1),
                                                  resultmatch.group(2)))
                    #determine if next line has collected keyword
                    stop = stopmatch.search(line)
                    if stop:
                        result_time_pattern= re.compile(
                            r'(Last updated:\s*)(\d+/\d+/\d+\s*\d+:\d+)')
                        result_time_match = result_time_pattern.search(line)
                        if result_time_match is not None:
                            result_time = result_time_match.group(2)
                            toadd.append(('result_time', result_time))
                        else:
                            #move to the next lien to find stop word
                            line = data.next()
                            result_time_match = result_time_pattern.search(line)
                            if result_time_match is not None:
                                result_time = result_time_match.group(2)
                                toadd.append(('result_time', result_time))                               
                        #Found stop keyword and row_id
                        #create entry and add results for that entry
                        row_id = stop.group(2) + "-" + str(count)
                        count += 1
                        self.create_row(row_id)
                        for field, value in toadd:
                            self.update_row(field, value, row_id)
        logging.debug("finished lab insert")


class Vitals(InsertOtherData):
    """Insert Data from Vital Sign Assessements"""

    def __init__(self, conn, data, logfilename):
        InsertOtherData.__init__(self, conn, data, logfilename)        
        self.resultpatterns = [makeNumPattern(word)
                              for word in ['BP','Temp','Rate','Resp',
                                           'SpO2','Pain Score', 'Device',
                                           'Rate \(L/min\)'
                                           ]
                               ]
        self.startpatterns = [makeSearchPattern(word)
                               for word in ['Vitals']
                              ]
        self.stop_word = 'Custom Formula'
        self.tablename = 'vitals'


class Cbc(InsertLabData):
    """Insert data from CBC labs"""

    def __init__(self, conn, data, logfilename):
        InsertLabData.__init__(self, conn, data, logfilename)
        self.resultpatterns = [makeNumPattern(word)
                              for word in ['White Blood Cell Count',
                                           'Red Blood Cell Count','Hemoglobin',
                                           'Hematocrit','Platelet Count']]
        self.startpatterns = [makeNumPattern(word)
                               for word in ['White Blood Cell Count']]        
        self.stop_word = 'Collected'
        self.tablename = 'cbc'


class Cmp(InsertLabData):
    """Insert Data from CMP lab test"""
    def __init__(self, conn, data, logfilename):
        InsertLabData.__init__(self, conn, data, logfilename)
        self.resultpatterns = [makeNumPattern(word)
                              for word in ['Sodium','Potassium','Chloride',
                                           'Carbon Dioxide','Urea Nitrogen',
                                           'Creatinine,Serum',
                                           'Creatinine, Serum','Glucose',
                                           'Albumin','Bilirubin,Total',
                                           'Bilirubin,Total']]
        self.startpatterns = [makeSearchPattern(word)
                               for word in ['CMP','Comprehensive','BMP',
                                            'Basic']]        
        self.stop_word = 'Collected'
        self.tablename = 'cmp'


class BloodGas(InsertLabData):
    """Insert Data from Blood Gas Lab test"""
    def __init__(self, conn, data, logfilename):
        InsertLabData.__init__(self, conn, data, logfilename)
        self.resultpatterns = [makeNumPattern(word)
                               for word in ['pH, Arterial', 'pCO2, Arterial',
                                            'pO2, Arterial', 'pH, Non-Arterial',
                                            'pCO2, Non-Arterial',
                                            'pO2, Non-Arterial']]
        self.startpatterns = [makeSearchPattern(word)
                              for word in ['Blood Gases']]
        self.stop_word = 'Collected'
        self.tablename = 'bloodgas'


class Influenza(InsertLabData):
    """Inserts data on Influenza Testing"""
    def __init__(self, conn, data, logfilename):
        InsertData.__init__(self, conn, data, logfilename)
        self.startpatterns = [makeSearchPattern(word)
                              for word in ['Influenza A NAT', 'Influenza A/B']]
        self.resultpatterns = [makeStringPattern(word)
                               for word in ['Influenza A NAT',
                                            'Influenza B NAT','RSV NAT']]
        self.stop_word = 'Collected'
        self.tablename = 'influenza'


class Disposition(InsertData):
    """Inserts Data on Subjects Final disposition"""
    def __init__(self, conn, data, logfilename):
        InsertData.__init__(self, conn, data, logfilename)
        self.startpatterns = [makeSearchPattern(word)
                              for word in ['Patient discharged',
                                           'Patient admitted',
                                           'ED Observation']]        
    def extractData(self):
        #Observation status
        other_disp = None
        logging.debug("startting disposition insert")
        #get start and end times
        data = iter(self.data)
        #Make sure the first line of the file has the right info
        while True:
            firstline = data.next()
            if firstline.find("Patient Care Timeline") != -1:
                break
        firstline = firstline.split()
        arrival_date = firstline[3].replace("(","")
        arrival_time = firstline[4].strip()
        departure_date = firstline[6].strip()
        departure_time = firstline[7].replace(")","").strip()
        #Subject can't be admitted and discharged so only one should match
        for line in data:
            if line.find('admitted') != -1:
                disposition = "admitted"
            if line.find('discharged') != -1:
                disposition = "discharged"
            if line.find('ED Observation') != -1:
                other_disp = "observation"
        self.create_row(arrival_date, arrival_time,departure_date,
                        departure_time, disposition)
        #Was subject in obs?
        if other_disp:
            self.create_row(arrival_date, arrival_time, departure_date,
                            departure_time, other_disp)

    def create_row(self, arrival_date,arrival_time,departure_date,
                   departure_time, disposition):
        """Creates new row in disposition table
        Args:
            arrival_date (str) : Date subject arrived
            arrival_time (str) : Time subject arrived
            departure_date (str) : Date subject left
            departure_time (str) : Date subject time
            disposition (str) : Disposition of subject
        """
        
        self.cursor.execute('''
        INSERT INTO disposition
        (arrival_date, arrival_time, departure_date,departure_time,disposition)
        VALUES
        (?, ?, ?, ?, ?)
        ''', (arrival_date, arrival_time,departure_date,departure_time,
              disposition))
        
        logging.debug(
            "Created row in table %s with id %s" % ('disposition',arrival_date))

        self.connection.commit()


class Assessment(InsertData):
    """Inserts Data on subjects Assessment"""
    def __init__(self, conn, data, logfilename):
        InsertData.__init__(self, conn, data, logfilename)
        self.startpatterns = [makeSearchPattern(word)
                              for word in ['Assessment']]
        self.resultpatterns = [makeNumPattern(word)
                               for word in ['Glasgow Coma Scale Score']]
        self.stop_word = 'Glasgow Coma Scale Score'
        self.tablename = 'assessments'

    def extractData(self):
        logging.debug("starting assessment insert")
        data = iter(self.data)
        count = 0
        for line in data:
            for pattern in self.startpatterns:
                startmatch = pattern.search(line)
                #skips braden assessments
                if startmatch is not None and line.find('Braden') == -1:
                    try:
                        row_id = startmatch.group(1) + "-" + str(count)
                        while line.find(self.stop_word) == -1:
                            line = data.next()
                        for pattern2 in self.resultpatterns:
                            resultmatch = pattern2.search(line)
                            if resultmatch is not None:
                                if resultmatch.group(1) and\
                                   resultmatch.group(2):
                                    count += 1
                                    self.create_row(row_id)
                                    field_name = resultmatch.group(1)
                                    result = resultmatch.group(2)
                                    self.update_row(field_name,result,row_id)
                    except StopIteration:
                        return


class InsertDataWithoutResults(InsertData):
    """Base class for inserting data that doesn't have results"""
    def __init__(self, conn, data, logfilename):
        InsertData.__init__(self, conn, data, logfilename)

    def create_row(self, row_id, resulttype):
        row_id = unicode(row_id.strip().decode('ascii','ignore'))
        resulttype = unicode(resulttype.strip().decode('ascii','ignore'))
        sql_template ='''
        Insert INTO %s
        (time, type)
        VALUES
        (?, ?)''' % self.tablename
        self.cursor.execute(sql_template, (row_id, resulttype))      
        logging.debug(
            "Created row in table %s with id %s" % (self.tablename, row_id))
        self.connection.commit()


class Imaging(InsertDataWithoutResults):
    """Inserts Imaging Data"""
    def __init__(self, conn, data, logfilename):
        InsertDataWithoutResults.__init__(self, conn, data, logfilename)
        self.startpatterns = [makeSearchPattern(word)
                              for word in ['XR Chest','CT Chest']]
        self.tablename = 'imaging'

    def extractData(self):
        logging.debug("starting imaging insert")
        data = iter(self.data)
        for line in data:
            for pattern in self.startpatterns:
                startmatch = pattern.search(line)
                if startmatch is not None:
                    if startmatch.group(1) and startmatch.group(2):
                        time = startmatch.group(1)
                        imagetype = startmatch.group(2)
                        self.create_row(time, imagetype)


class Cultures(InsertDataWithoutResults):
    def __init__(self, conn, data, logfilename):
        InsertDataWithoutResults.__init__(self, conn, data, logfilename)
        self.startpatterns = [makeSearchPattern(word)
                              for word in ['Culture Order']]
        self.tablename = 'cultures'
        
    def extractData(self):
        logging.debug("starting culture insert")
        data = iter(self.data)
        count = 0
        for line in data:
            for pattern in self.startpatterns:
                startmatch = pattern.search(line)
                if startmatch is not None:
                    if startmatch.group(1) and startmatch.group(2):
                        row_id = startmatch.group(1) + "-" + str(count)
                        count += 1
                        type_pattern = re.compile(
                            r'(\d+:\d+:*\d*\s*)(Culture Order)(.*$)')
                        type_search = type_pattern.search(line)
                        if type_search is not None:
                            culture_type = type_search.group(3)
                            self.create_row(row_id,culture_type)


class RespVirus(InsertDataWithoutResults):
    def __init__(self, conn, data, logfilename):
        InsertDataWithoutResults.__init__(self, conn, data, logfilename)
        self.startpatterns = [makeSearchPattern(word)
                              for word in ['Collect Resp Virus']]
        self.tablename = 'respvirus'

    def extractData(self):
        logging.debug("starting resp insert")
        data = iter(self.data)
        count = 0
        for line in data:
            for pattern in self.startpatterns:
                startmatch = pattern.search(line)
                if startmatch is not None:
                    row_id = startmatch.group(1) + "-" + str(count)
                    count += 1
                    self.create_row(row_id,'Resp Virus')


class Xpert(InsertDataWithoutResults):
    def __init__(self, conn, data, logfilename):
        InsertDataWithoutResults.__init__(self, conn, data, logfilename)
        self.startpatterns = [makeSearchPattern(word)
                              for word in ['Orders Placed	XPERT']]
        self.tablename = 'xpert'

    def extractData(self):
        logging.debug("starting xpert insert")
        data = iter(self.data)
        count = 0
        for line in data:
            line = line.replace("  "," ")
            for pattern in self.startpatterns:
                startmatch = pattern.search(line)
                if startmatch is not None:
                    row_id = startmatch.group(1) + "-" + str(count)
                    count += 1
                    self.create_row(row_id,'Xpert Flu')


class DischargeOrders(InsertData):
    def __init__(self, conn, data, logfilename):
        InsertData.__init__(self, conn, data, logfilename)
        self.startpatterns = [makeSearchPattern(word)
                              for word in ['Discharge Orders']]
        self.tablename = 'discharge_orders'

        self.antibiotic_names = getcommonnames.getAbxNames()
        self.antiviral_names = getcommonnames.getAntiviralNames()

    def extractData(self):
        logging.debug("starting discharge order insert")
        data = self.data.read()
        count = 1
        # Need two patterns to remove meds that were discontinued after order
        discharge_med_patttern = re.compile(r'(Discharge Orders Placed)([\w\Wn]*?\n)')
        discharge_discontinued_pat = re.compile(r'(Discharge Orders Discontinued)([\w\Wn]*?\n)')

        result_match = discharge_med_patttern.findall(data)
        negative_match = discharge_discontinued_pat.findall(data)
        negative_match = [group[1] for group in negative_match]
        if result_match is not None:
            for group in result_match:
                # Remove negative matches so you only match once
                if group[1] in negative_match:
                    negative_match.remove(group[1])
                    continue
                meds_strings = group[1].replace("(","").replace(")","").split()
                for med in meds_strings:
                        if med in self.antibiotic_names or med in self.antiviral_names:
                            row_id = str(count)
                            field = 'name'
                            value = med
                            self.create_row(row_id)
                            self.update_row(field, value, row_id)
                            count += 1
                            break


