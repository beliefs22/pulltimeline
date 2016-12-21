import sqlite3
import os

def maketables(subject_id, cleaned_file_dir):
    
    """Creates or connects to a database for given subject_id
    and creates various tables in that database that holds data
    pulled from the patients chart

    Args:
        subject_id (str): Unique ID for the subject

    Returns:
        conn (sqlit3 connection): connection to the created database
    """
    #Directories
    sep = os.sep
    database_path = cleaned_file_dir + sep + subject_id + ".db"
    
    #Connects or creates database
    conn = sqlite3.connect(database_path)
    cur = conn.cursor()
    
    # Vitals Tables
    cur.execute('DROP TABLE IF EXISTS vitals')
    cur.execute('CREATE TABLE vitals (time, type, value)')

    # Labs Tables
    cur.execute('DROP TABLE IF EXISTS labs')
    cur.execute('CREATE TABLE labs (time, type, value)')

    # Medication Tables
    cur.execute('DROP TABLE IF EXISTS medications')
    cur.execute('CREATE TABLE medications (time, type)')

    # Disposition Table
    cur.execute('DROP TABLE IF EXISTS dispo')
    cur.execute('CREATE TABLE dispo (time, type)')


    conn.commit()
    
    return conn   


def makeTables2():
    sep = os.sep
    subject_files_dir = os.getcwd() + sep + 'Subject_Files'
    cleaned_files_dir = subject_files_dir + sep + 'Cleaned_Files'
    myfiles = os.listdir(cleaned_files_dir)
    for afile in myfiles:
        if afile.endswith("clean.txt"):
            filename = afile.replace(".txt","")
            _maketables(cleaned_files_dir + sep + filename)
    
def main():
    makeTables()
    


if __name__ == "__main__":
    main()  
