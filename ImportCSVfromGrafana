
"""
csv file export from Grafana
read csv file
convert to txt file: one row is one measurement point:
    measurement name value timestamp
this txt file can be imported in influx.
have influx installed
in terminal window (ubuntu) do:
    curl -i -XPOST 'http://localhost:8086/write?db=algist_db' --data-binary @/home/ubuntu/Documenten/data_sets/ingest.txt
(with ingest.txt the file of measurement points
"""


import csv
import string
import datetime
import time

def stringSplitter(str, divider):
    """split string in 2 parts, divided by the first occurrence of the 'devider',
    2 parts are stripped from leading and tailing white spaces and '"' (quotes)."""
    dividerIndex = str.find(divider)
    strPart1 = str[0:dividerIndex].strip('" "')
    strPart2 = str[dividerIndex+1:len(str)].strip('" ')
    #return [strPart1, strPart2]
    return [strPart1]

def time_RFC3339_to_Influx_epoch(timeString):
    """ only valid format : "2018-02-13T06:44:25.000Z"
        epoch in nanoseconds
        REMARK : time conversion from RFC3339 is according to Python standard modules
                        probably not correct !!!"""
    """
    https://stackoverflow.com/questions/969285/
    how-do-i-translate-a-iso-8601-datetime-string-into-a-python-datetime-object:

    import dateutil.parser
    datestring = "2018-02-13T06:44:35.000000000Z"
    yourdate = dateutil.parser.parse(datestring)
    print(yourdate)
    """
    timeEpoch = int(time.mktime(time.strptime(timeString[:19], '%Y-%m-%dT%H:%M:%S')))
    #print(timeEpoch)
    decimalSeconds = timeString[19:].strip('.Z')
    #print(decimalSeconds)
    decimalSeconds = '{:0<9}'.format(decimalSeconds)
    #print(decimalSeconds)
    timeInflux = str(timeEpoch) + decimalSeconds
    return(timeInflux)




### START ###
# location of data file to ingest is hard coded
csvDataFileDir = "c:\TEMP\\"
csvDataFileName = "77-3h-grafana_data_export.csv"
ingestFileName = "ingest-3h.txt"

csvDataFile = csvDataFileDir + csvDataFileName
ingestFile = csvDataFileDir + ingestFileName
lineCounter = 0
m = []
mList = []
mValue = []
mValueList = []
with open(csvDataFile, 'r') as csvFile:
    data = csv.reader(csvFile, delimiter=';')
    for row in data:
        lineCounter += 1
        m = row
        mList.append(m)
print(len(mList[2]))
#print(mList)
print(lineCounter)

tagList = []
for m in mList[1][1:]:
    tag = ''.join(stringSplitter(m, " "))
    tagList.append(tag)
print(tagList)

for row in mList[3:]:
    timestamp = time_RFC3339_to_Influx_epoch(row[0])
    tagNr = 0
    #print(tagList[tagNr])
    for column in row[1:]:
        valueStr = "value=" + column
        mValue = [tagList[tagNr], valueStr, timestamp]
        mValueList.append(mValue)
        tagNr += 1

with open(ingestFile, "w", newline="") as f:
    lines = []
    for element in mValueList:
        line = (element[0] + " " + str(element[1]) + " " + element[2] + "\n")
        lines.append(line)
    print(''.join(lines))
    f.writelines(lines)



