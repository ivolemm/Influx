# from sys import argv
# script, filename = argv

import csv

# Structure of config file:
# Header starting with [input]
"""
[input]
url             = "opc.tcp://3.100.90.64:55101"
failoverTimeout = 5000 # time to wait before reconnection in case of failure

[output]
name             = "influx_1"
type             = "influxdb"
host             = "3.100.90.65"
port             = 8086
protocol         = "http"
username         = "factrylogger"
password         = "factrylogger"
database         = "testdb"
failoverTimeout  = 10000
bufferMaxSize    = 64
writeInterval    = 2000
writeMaxPoints   = 1500
"""

# measurement
"""
[[measurements]]
name               = "LT.39.10.01_L_AI"
dataType           = "number"
tags               = { description = "Niveaumeting mengtank ammoniumsulfaat 3V" }
nodeId             = "ns=4;s=S7:[AU_39_03]DB504,REAL48"
collectionType     = "polled"
pollRate           = 60
monitorResolution  = 500
deadbandAbsolute   = 0
-------------------------------
retentionPolicy    = 3 weeks
Remarque :
if collectionType = "polled", monitorResolution = null
if collectionType = "monitored", pollRate = null

RetentionPolicy : number + " " + "weeks" or "years"
"""

"""
The config.toml is a plain text file with no line feed characters and no delimiter separated columns.
So:
Read config.toml as string: configtomlString
Split configtomlString into:
inputString: starts from '[input]' (included) till '[output]' (not included)
    convert inputString into list of settings: inputSettingsList
outputString: starts from '[output]' (included) till '[[measurements]]' (not included)
    convert outputString into list of settings: outputSettingsList
measurementsString: starts from '[[measurements]]' till end of measurementsString
    next '[[measurements]]' or end of measurementsString
    and store every '[[measurements]]' as a measurementsString
    convert the measurements into measurementsDict with key,values
"""

"""
convert string to key:value
"""

def stringSplitter(str, divider):
    """split string in 2 parts, divided by the first occurrence of the 'devider',
    2 parts are stripped from leading and tailing white spaces and '"' (quotes)."""
    dividerIndex = str.find(divider)
    strPart1 = str[0:dividerIndex].strip('" "')
    strPart2 = str[dividerIndex+1:len(str)].strip('" ')
    return [strPart1, strPart2]



### START ###
# location of config.toml to convert is hard coded
configtomlDirectory = "c:\TEMP\\"
configtomlFileToConvert = "config20180222.toml"

# output file has same directory, same file name but with extention '.csv'
configtomlFileToConvertWoExtention = configtomlFileToConvert.rsplit(".", 1)
configtomlCSVOutputFile = configtomlDirectory + configtomlFileToConvertWoExtention[0] + ".csv"
# print("configtomlCSVOutputFile: ", configtomlCSVOutputFile)

configtomlFile = open(configtomlDirectory + configtomlFileToConvert, "r")
# return list of lines
configtomlListOfLines = configtomlFile.readlines()
configtomlFile.close()
# print(configtomlListOfLines)
# print(len(configtomlListOfLines))

configtomlListOfLinesStripped = []
for xLine in range(0, len(configtomlListOfLines)):
    # print(xLine, configtomlListOfLines[xLine].rstrip('\n'))
    configtomlLine = configtomlListOfLines[xLine].rstrip('\n')    # right strip line break '\n'
    configtomlLine = configtomlLine.lstrip()    # left strip white space
    configtomlLine = configtomlLine.rstrip()    # right strip white space
    # print(configtomlLine)
    if configtomlLine != '': configtomlListOfLinesStripped.append(configtomlLine) # omit blank lines
# print(configtomlListOfLinesStripped)

configtomlListHeader = []
for xLine in range(0,len(configtomlListOfLinesStripped)):
    if configtomlListOfLinesStripped[xLine] != '[[measurements]]':
        configtomlListHeader.append(configtomlListOfLinesStripped[xLine])
    else: break
# print(configtomlListHeader)

configtomlListMeasurements = list(configtomlListOfLinesStripped)    # copy list
if len(configtomlListOfLinesStripped) > 0:
    del configtomlListMeasurements[0:len(configtomlListHeader)]
# print(configtomlListMeasurements)


measurementId = 0
measurementList = []  # list of measurements
measurementItem = 0
measurementItemsList = []
for x in range(0, len(configtomlListMeasurements)):
    if configtomlListMeasurements[x] == '[[measurements]]':
        measurementList.append(measurementItemsList)
        measurementId += 1
        measurementItem = 0
        measurementItemsList = []  # list of items of one measurement
        measurementItemsList.append(configtomlListMeasurements[x])
    else:
        measurementItem += 1
        mmentItem = stringSplitter(configtomlListMeasurements[x],"=")    # use stringSplitter to split measurementItems in 2 parts
        #measurementItemsList.append(configtomlListMeasurements[x])
        measurementItemsList.append(mmentItem)
measurementList.append(measurementItemsList)

# print(measurementItemsList)
# print("measurementList: ", measurementList)
# print(len(measurementList))


"""initialise the data dictionary"""
measurementSpecs = ["measurementId",\
                    "name", \
                    "dataType", \
                    "tags", \
                    "nodeId", \
                    "collectionType", \
                    "pollRate", \
                    "monitorResolution", \
                    "deadbandAbsolute", \
                    "deadbandRelative", \
                    "retentionPolicy", \
                    "unknownValues"\
                    ]

lst_measurements = []
lst_specs =[]
measurementId = 0
# print(measurementList[1]) # measurement
#print(measurementList[1][0])
# print(measurementList[1][1]) # spec
# print(measurementList[1][1][1])  # specData

for mNr in range(0,len(measurementList)):
    mList = measurementList[mNr]
    # print("mNr: ",mNr)
    # print("mList: ", mList)
    # print("len(mList)", len(mList))
    if len(mList) > 0:
        lst_specs = ['', '', '', '', '', '', '', '', '', '', '', '']
        #print("mList[mNr]: ", mList[mNr])
        #len_mList = len(mList[mNr])
        #print("len_mList: ", len_mList)
        for specNr in range(0, len(mList)):
            # print("specNr: ", specNr, "; len(mList): ", len(mList))
            specData = mList[specNr]
            # print("specData: ", specData)
            if specData == '[[measurements]]':
                measurementId +=1
                lst_specs[0] = measurementId
                # print("lst_specs", lst_specs)
            elif specData[0] == measurementSpecs[1]:
                lst_specs[1] = specData[1]
            elif specData[0] == measurementSpecs[2]:
                lst_specs[2] = specData[1]
            elif specData[0] == measurementSpecs[3]:
                lst_specs[3] = specData[1]
            elif specData[0] == measurementSpecs[4]:
                lst_specs[4] = specData[1]
            elif specData[0] == measurementSpecs[5]:
                lst_specs[5] = specData[1]
            elif specData[0] == measurementSpecs[6]:
                lst_specs[6] = specData[1]
            elif specData[0] == measurementSpecs[7]:
                lst_specs[7] = specData[1]
            elif specData[0] == measurementSpecs[8]:
                lst_specs[8] = specData[1]
            elif specData[0] == measurementSpecs[9]:
                lst_specs[9] = specData[1]
            elif specData[0] == measurementSpecs[10]:
                lst_specs[10] = specData[1]
            elif specData[0] == measurementSpecs[11]:
                lst_specs[11] = specData[1]
        # print("lst_specs:", lst_specs)
        lst_measurements.append(lst_specs)

for x in range(0, len(lst_measurements)):
    print(lst_measurements[x])


with open(configtomlCSVOutputFile, "w", newline='') as f:
    wr = csv.writer(f, delimiter = ",")
    wr.writerow(measurementSpecs)
    wr.writerows(lst_measurements)

