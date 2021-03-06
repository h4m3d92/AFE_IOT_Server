import os, re
from databaseModel import *


fileConnections = 'connection'
fileTransmission = 'transmission'
fileEvents = 'event'
fileSizeLimit = 1000000
maxFileNumber = 10

def writeConnection(inStr):
    # if the number of files reaches maxFileNumber
    if len(re.findall(fileConnections, str(os.listdir(configData["logPath"])))) > maxFileNumber:
        return

    file = open(configData["logPath"] + fileConnections + '.csv', 'a')
    if not os.path.getsize(configData["logPath"] + fileConnections + ".csv"):
        file.write('Date and Time, Type, IP, Port, GW Number, Fail Reason' + '\n')

    file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ', ')
    for item in inStr:
        file.write(str(item) + ', ')
    file.write('\n')
    file.close()
    # if the size of file reaches fileSizeLimit
    if os.path.getsize(configData["logPath"] + fileConnections + ".csv") > fileSizeLimit:
        os.rename(r'' + configData["logPath"] + fileConnections + '.csv',
                  r'' + configData["logPath"] + fileConnections + ' ' + datetime.now().strftime("%Y-%m-%d %H-%M-%S") + '.csv')
        fd = open(configData["logPath"] + fileConnections + '.csv', 'a')
        fd.close()

def writeTransmission(inStr):
    # if the number of files reaches maxFileNumber
    if len(re.findall(fileTransmission, str(os.listdir(configData["logPath"])))) > maxFileNumber:
        return

    file = open(configData["logPath"] + fileTransmission + '.csv', 'a')
    if not os.path.getsize(configData["logPath"] + fileTransmission + ".csv"):
        file.write('Date and Time, Type, IP, Port, GW Number, Data' + '\n')

    file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ', ')
    for item in inStr:
        file.write(str(item) + ', ')
    file.write('\n')
    file.close()
    # if the size of file reaches fileSizeLimit
    if os.path.getsize(configData["logPath"] + fileTransmission + ".csv") > fileSizeLimit:
        os.rename(r'' + configData["logPath"] + fileTransmission + '.csv',
                  r'' + configData["logPath"] + fileTransmission + ' ' + datetime.now().strftime("%Y-%m-%d %H-%M-%S") + '.csv')
        fd = open(configData["logPath"] + fileTransmission + '.csv', 'a')
        fd.close()

def writeEvent(inStr):
    # if the number of files reaches maxFileNumber
    if len(re.findall(fileEvents, str(os.listdir(configData["logPath"])))) > maxFileNumber:
        return

    file = open(configData["logPath"] + fileEvents + '.csv', 'a')
    if not os.path.getsize(configData["logPath"] + fileEvents + ".csv"):
        file.write('Date and Time, Type, Title, Description' + '\n')

    file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ', ')
    for item in inStr:
        file.write(str(item) + ', ')
    file.write('\n')
    file.close()
    # if the size of file reaches fileSizeLimit
    if os.path.getsize(configData["logPath"] + fileEvents + ".csv") > fileSizeLimit:
        os.rename(r'' + configData["logPath"] + fileEvents + '.csv',
                  r'' + configData["logPath"] + fileEvents + ' ' + datetime.now().strftime("%Y-%m-%d %H-%M-%S") + '.csv')
        fd = open(configData["logPath"] + fileEvents + '.csv', 'a')
        fd.close()


# print(len(re.findall(fileConnections, str(os.listdir(configData["logPath"])))))
# writeConnection([2,'3'])


# fd = open(configData["logPath"] + fileName + '.csv', 'a')
# if not os.path.getsize(configData["logPath"] + fileName + ".csv"):
#     fd.write('0, 0' + '\n')
# for i in range(40000):
#     fd.write(str(i) + ',' + str (i+2) + '\n')
# fd.close()
# if os.path.getsize(configData["logPath"] + fileName + ".csv") > fileSizeLimit:
#     os.rename(r'' + configData["logPath"] + fileName + '.csv', r'' + configData["logPath"] + fileName + ' ' + datetime.now().strftime("%Y-%m-%d %H-%M-%S") + '.csv')
#     fd = open(configData["logPath"] + fileName + '.csv', 'a')
#     fd.close()


# with open('outputTable.csv','a') as fd:
#     fd.write('[3,4]\n')

# with open("outputTable.csv", 'w', newline='') as csvfile:
#     # creating a csv writer object
#     csvwriter = csv.writer(csvfile)
#     # writing the fields
    # csvwriter.writerow([2,3])
    # writing the data rows
    # csvwriter.writerows([[2,3], [2,3]])
