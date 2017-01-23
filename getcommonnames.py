def getAbxNames():
    with open(r'H:\Pycharm Projects\pulltimeline\abxnames.txt', 'r') as myfile:
        myfile = list(myfile)
        towrite = []
        for line in myfile:
            line = line.lower().replace("\n", "")
            if line.find('generation') != -1:
                continue
            line = line.replace("(", "").replace(")", "").strip().split(" ")
            for item in line:
                if item != "" and len(item) > 4:
                    towrite.append(item)

        abx = {item: item
               for item in towrite
               }

    return abx


def getAntiviralNames():
    with open(r'H:\Pycharm Projects\pulltimeline\antiviralnames.txt', 'r') as myfile:
        myfile = list(myfile)
        towrite = []
        for line in myfile:
            line = line.lower().replace("\n", "")
            line = line.replace("(", "").replace(")", "").strip().split(" ")
            for item in line:
                if item != "" and len(item) > 4:
                    towrite.append(item)

        antivirals = {item: item
                      for item in towrite
                      }
    return antivirals


def main():
    abx = getAbxNames()
    print 'b' in abx, abx['b']
    antivirals = getAntiviralNames()
    print 'tamiflu' in antivirals


if __name__ == "__main__":
    main()
