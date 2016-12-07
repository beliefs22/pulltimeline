import os
import re


def cleanFiles(cleaned_files_dir, subject_files_dir):
    """Cleans files by combining make sure each line is formated correctly
    Args:
        cleaned_files_dir (str) : location to save cleaned files to
        subject_files_dir (str) : locations of raw files
        """
    num_pattern = re.compile(r'^(\d+:\d+\s*)$|^(\d+:\d+:\d+\s*)$')
    sep = os.sep
    clean_name = lambda filename: cleaned_files_dir + sep + \
                                  filename.replace(".txt", "_clean.txt")
    currentfiles = os.listdir(subject_files_dir)

    for filename in currentfiles:
        if filename.startswith('01_11_A') and filename.endswith('.txt'):
            with open(subject_files_dir + sep + filename, 'r') as \
                    subjectfile, open(clean_name(filename), 'w') as outfile:
                print "cleaning", filename
                subjectfile = iter(subjectfile)
                line = subjectfile.next()
                while not line.startswith('Patient Care Timeline'):
                    line = subjectfile.next()
                outfile.write(line + "\n")
                for line in subjectfile:
                    line = line.replace("\n", "")
                    match = num_pattern.search(line)
                    if match is not None:
                        line = line + " " + subjectfile.next()
                    outfile.write(line + "\n")


def main():
    pass


if __name__ == "__main__":
    main()
