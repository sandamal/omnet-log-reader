
# analyse vec file from omnet
# *.hostL[*].capture
from builtins import print
from collections import defaultdict
from operator import itemgetter

import numpy as np
import pandas

d = defaultdict(list)
complexDict = defaultdict(dict)
combinedDict = defaultdict(dict)
constantDict = defaultdict(dict)

rndNumber = '619'
uniqueText = 'clean'

outFile = 'General-#0_{}_{}.out'.format(rndNumber,uniqueText)
csvFile = '{}_{}.csv'.format(rndNumber,uniqueText)
csvOutput = '{}_{}_op.csv'.format(rndNumber,uniqueText)

fieldnames = ['label', 'carrierFrequency', 'bandwidth',
              'bitrate', 'duration', 'messageLength', 'iat']

f = open(outFile, "r")
for line in f:
    if line != '\n':
        tokens = line.split()
        if tokens[0] == '[FATAL]':

            rowdata = []
            host = tokens[1]
            listenerIds = host.split('.')

            if listenerIds[1].startswith('hostL'):
                # hostId
                listenerId = listenerIds[1]
                rowdata.append(listenerId)
                # packet name
                packet = tokens[5].split(')')[1]
                # sender name
                senderName = packet.split('-')[0]
                packet = packet.split('-')[1]
                rowdata.append(senderName)
                rowdata.append(packet)

                # duration
                rowdata.append(tokens[7])
                # start time
                rowdata.append(tokens[9])
                # end time
                rowdata.append(tokens[36].replace(',', ''))
                # power
                rowdata.append(tokens[15])
                # carrierFrequency
                rowdata.append(tokens[19])
                # bandwidth
                rowdata.append(tokens[23])
                # bit rate
                rowdata.append(tokens[74])

                d[listenerId].append(rowdata)
f.close()

# read the messageLengths, srcAddresses and destAddresses
f = open(csvFile, "r")
for line in f:
    if line != '\n':
        tokens = line.split(',')
        if tokens[0].startswith('General') and tokens[4] == 'VALUE':
            host = tokens[2].split('.')[-2]
            values = tokens[5:]
            complexDict[host] = values
f.close()

# combine the 2 data sets
for k, v in d.items():
    for i, item in enumerate(v):
        messageLengths = complexDict[k]
        length = messageLengths[i].replace('"', '')
        length = length.replace('\n', '')
        item.append(length)

        try:
            value = combinedDict[k][item[1]]
        except KeyError:
            combinedDict[k][item[1]] = []
            pass
        combinedDict[k][item[1]].append(item)

rows = []

for topKey, topValue in combinedDict.items():
    for key, items in topValue.items():
        for j, item in enumerate(items):
            if j == 0:
                initialArrival = item[4]
                item.append(0)
            else:
                iat = float(item[4]) - float(initialArrival)
                iatAsString = '{:f}'.format(iat)
                item.append(iatAsString)
                initialArrival = item[4]
                rows.append(item)

complexDict = defaultdict(dict)

for row in rows:
    if row[1].startswith('Civ_'):
        label = '1'
    else:
        label = '-1'

    listener = row[0]
    senderName = row[1]
    duration = row[3]
    power = float(row[6])
    carrierFrequency = row[7]
    bandwidth = row[8]
    bitrate = row[9]
    messageLength = row[10]
    iat = row[11]
    start = float(row[4])

    df = pandas.DataFrame({'duration': [duration],
                           'messageLength': [messageLength],
                           'iat': [iat],
                           'power': [power],
                           'start': [start]}, dtype=float)

    staticValues = {'senderName': senderName,
                    'carrierFrequency': carrierFrequency,
                    'bandwidth': bandwidth,
                    'bitrate': bitrate,
                    'label': label}

    if listener in complexDict[senderName].keys():
        result = complexDict[senderName][listener].append(df, ignore_index=True)
        complexDict[senderName][listener] = result
    else:
        complexDict[senderName][listener] = df
        constantDict[senderName] = staticValues

rows = []
# make 1 row per srcAddress by averaging
for senderName, listeners in complexDict.items():

    staticValues = constantDict[senderName]
    rowData = [staticValues['label'], staticValues['carrierFrequency'], staticValues['bandwidth'],
               staticValues['bitrate']]

    listenersOfSender = defaultdict(dict)

    for listenerName, dataFrame in listeners.items():
        avgPower = dataFrame['power'].mean()
        listenersOfSender[avgPower] = dataFrame

    sortedKeys = sorted(listenersOfSender.keys(), reverse=True)
    numOfListeners = 3
    # bins = np.linspace(0, 3600, 13)
    bins = np.linspace(0, 3600, 13)

    if len(sortedKeys) >= numOfListeners:
        for i in range(0, numOfListeners):
            df = listenersOfSender.get(sortedKeys[i])
            groups = df.groupby(pandas.cut(df.start, bins)).mean()

            for value in groups['duration']:
                rowData.append('{:f}'.format(value))

            for value in groups['messageLength']:
                rowData.append('{:f}'.format(value))

            for value in groups['iat']:
                rowData.append('{:f}'.format(value))

        rows.append(rowData)

with open(csvOutput, 'w') as out_file:
    print('{},{}'.format(fieldnames[0], ','.join(fieldnames[1:])), file=out_file)
    for row in rows:
        print('{},{}'.format(row[0], ','.join(row[1:])), file=out_file)
