import requests

def importRecords(TOKEN, records):
    #Hopkins REDCap URL
    URL = 'https://mrprcbcw.hosts.jhmi.edu/redcap/api/'
    #API to pull reports
    payload = {'token': TOKEN, 'format': 'csv', 'content': 'record',
               'type' : 'flat', 'overwriteBehavior' : 'normal',
               'data' : records, 'dateFormat': 'MDY'}

    response = requests.post(URL, data=payload, verify=True)
    print response.status_code
    #Sucessful call
    if response.status_code == 200:
        print "Sucessfully Imported Records"
        print response.text
        
    else:
        print "Failed"
        print response.status_code
        data = response.text.split("\n")
        for line in data:
            print line
        raise RuntimeError

def main():
    pass


if __name__ == "__main__":
    main()
    
