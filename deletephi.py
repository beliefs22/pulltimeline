from shutil import copy

import os
def deletePHI(ids, subject_files_dir, cleaned_files_dir, completed_files_dir):
    """Removes files containing PHI and saves de-identified source document
    Args:
        ids (list) : string of ids that had files created from PHI
        subject_files_dir (str) : location of subject files
        completed_files_dir (str) : location of completed files
        cleaned_files_dir (str) : location of clean files
    """
    sep = os.sep
    current_subject_files = os.listdir(subject_files_dir)
    current_cleaned_files = os.listdir(cleaned_files_dir)
    #Remove PDFs and Text Files
    for subject_file in current_subject_files:
        for subject_id in ids:
            if subject_file.find(subject_id) != -1:
                print("removing {}".format(subject_file))
                #remove pdfs and text documents
                os.remove(subject_files_dir + sep + subject_file)
    #Remove databases but keep text databases were created from
    for cleaned_file in current_cleaned_files:
        for subject_id in ids:
            if cleaned_file.find(subject_id) != -1 and cleaned_file.find(".db") != -1:
                print("removing {}".format(cleaned_file))
                os.remove(cleaned_files_dir + sep + cleaned_file)
            if cleaned_file.find(subject_id) != -1 and cleaned_file.find(".txt") != -1:
                print("moving cleaned text file to completed files directory {}".format(cleaned_file))
                copy(cleaned_files_dir + sep + cleaned_file, completed_files_dir + sep)
                os.remove(cleaned_files_dir + sep + cleaned_file)
