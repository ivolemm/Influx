#! C:\Users\me\AppData\Local\Programs\Python\Python36\python.exe
# author : Ivo Lemmens ; March 2018
# background : script based on "RunningHours.py" (private) by Ivo Lemmens
# This script writes the last value before or at request_dt (dt = datetime)
# of all measurements in the request_list to a csv-file (delimiter ";")
# output file name = output-request_dt.csv


from influxdb import InfluxDBClient
import time


host = "my.influxdb.ip.address"   # replace my.influxdb.ip.address with the ip-address of your influxdb server
port = 8086
user = "me"                     # replace me with your user name for influxdb
password = "myInfluxdbPassword" # replace myInfluxdbPassword with your password for your user name for influxdb
dbname = "myInfluxdbName"       # replace myInfluxdbName with the influx database name you want to request
client = InfluxDBClient(host, port, user, password, database=dbname)

# request list
request_list = ["measurement1", "measurement2", "measurement3", "measurement4"]

# request date time
request_dt = "2018-03-08T08:00:00"

# location of data file to ingest is hard coded
Dir = "\\\\your.server.com\\full\\path\\"
csvOutName = "output-" + request_dt.replace(":","-") + ".csv"

def RFC3339_n_Z(precision = 9):
    """
    :return: actual time in Influx time format e.g. : "2018-02-25T15:21:13.123456789Z"
    """
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(time.time()))\
           + "."\
           + str(abs(round(time.time() - int(time.time()),precision))).split(".")[-1] \
           + "Z"

def time_ISO8601_to_Influx_epoch(timeString):
    """
    convert time format ISO8601 to epoch time, precision nanoseconds
    :param timeString: format: "2018-02-13T06:44:25.123Z" decimal precision my vary
    (:param precision : 9 (seconds) to maximal 9 (nanoseconds))
    :return: time format Epoch - integer e.g. 1518504265123000000
    """
    timeEpoch = int(time.mktime(time.strptime(timeString[:19], '%Y-%m-%dT%H:%M:%S')))
    decimalSeconds = timeString[19:].strip('.Z')
    decimalSeconds = '{:0<9}'.format(decimalSeconds)
    timeInflux = str(timeEpoch) + decimalSeconds
    return(timeInflux)


# PRINT REQUEST DATE TO output console -
print("request_dt human readable: ", request_dt)
request_dt_epoch = time_ISO8601_to_Influx_epoch((request_dt))
print("request_dt_epoch:          ", request_dt_epoch)

# MAKE REQUESTS TO INFLUXDB AND PRINT TO output console
resultList= []
for m in (request_list):
    result_m = client.query('select last(*) from \"{0}\" where time <= {1}'.format(m, request_dt_epoch))
    result_mList = list(result_m.get_points(measurement='{0}'.format(m)))
    resultList.append([m, result_mList])
print(resultList)

# OUTPUT TO FILE and print to output console
csvData = []
for tag in resultList:
    for m in tag[1]:
        csvData.append([tag[0], m["last_value"], m["time"]])
csvDataString = ""
for m in csvData:
    csvDataString += m[0] + ";" + str(m[1]) + ";"+ m[2] + "\n"
print(csvDataString)

outFile = Dir + csvOutName
with open(outFile, "w") as fh:
    fh.write(csvDataString)
