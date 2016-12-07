import re
import logging
from datetime import datetime

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
        
    def __init__(self,conn,data,logfilename):
        
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
        row_id = row_id.strip()
        template = '''
        Insert INTO %s
        (time)
        VALUES (?)''' % self.tablename
        logging.debug(
            "Created row in table %s with id %s" % (self.tablename,row_id))
        self.cursor.execute(template, (row_id,))
        self.connection.commit()

    def update_row(self,field,value,row_id):
        """Updates the row with given paramaters with given value
        Args:
            field (str): field to update
            value (str): value to update field to
            row_id (str): record to update
        """
        value = value.strip()
        row_id = row_id.strip()
        field = field.lower().strip().replace(" ","_").replace(":","")
        field = field.replace(",","_").replace("/","").replace("(","")
        field = field.replace(")","").replace("-","_").replace("__","_")
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
        self.connection.commit

class InsertVitalData(InsertData):

    def __init__(self, conn, data, logfilename):
        InsertData.__init__(self, conn, data, logfilename)
        self.startpatterns = ['Temperature', 'Heart Rate', 'BP (cuff)',
                              'GCS', 'SpO2', 'O2 Device']
