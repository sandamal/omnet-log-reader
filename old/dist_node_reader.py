from builtins import print
from collections import defaultdict
import pandas
import numpy as np

perListenerDict = defaultdict(dict)
complexDict = defaultdict(dict)
newPerListenerDict = defaultdict(dict)

finalDict = defaultdict(dict)

fieldnames = ['label', 'carrierFrequency', 'bandwidth', 'bitrate',
              'duration','messageLength', 'iat']

# read the messageLengths
f = open("vec.csv", "r")
for line in f:
    if line != '\n':
        tokens = line.split(',')
        if(tokens[0].startswith('General') and tokens[4]=='VALUE'):
            host = tokens[2].split('.')[-2]
            type = tokens[3].split(':')[0]
            values = tokens[5:]
            complexDict[host] = values
f.close()

f = open("General-#0.out", "r")
for line in f:
    if line != '\n':
        tokens = line.split()
        if tokens[0] == '[FATAL]':

            host = tokens[1]
            listenerIds = host.split('.')

            if listenerIds[1].startswith('hostL'):
                # hostId
                listenerId = listenerIds[1]
                # packet name
                packet = tokens[5].split(')')[1]
                host = packet.split('-')[0]
                packet = packet.split('-')[1]
                # duration
                duration = tokens[7]
                # start time
                start = tokens[9]
                # end time
                end = tokens[36].replace(',', '')
                # power
                power = tokens[15]
                # carrierFrequency
                carrierFrequency = tokens[19]
                # bandwidth
                bandwidth = tokens[23]
                # bitrate
                bitrate = tokens[74]

                messageLength = complexDict[listenerId].pop(0)

                df = pandas.DataFrame({'duration': [duration],
                                       'carrierFrequency':[carrierFrequency],
                                       'bandwidth':[bandwidth],
                                       'bitrate':[bitrate],
                                       'messageLength':[messageLength],
                                       'start': [start]}, dtype=float)

                try:
                    value = perListenerDict[listenerId][host]
                except KeyError:
                    perListenerDict[listenerId][host] = defaultdict(dict)
                    pass
                perListenerDict[listenerId][host][packet] = df


f.close()



# calculate IATs
for listenerKey,hostValues in perListenerDict.items():
    for hostKey, packet in hostValues.items():
        for j, packetID in enumerate(packet.keys()):

            df = packet[packetID]

            if j==0:
                initialArrival= df['start']
            else:
                iat = df['start'] - initialArrival
                initialArrival = df['start']
                df1 = df.assign(iat=iat)
                try:
                    value = newPerListenerDict[listenerKey][hostKey]
                except KeyError:
                    newPerListenerDict[listenerKey][hostKey] = defaultdict(dict)
                    pass
                newPerListenerDict[listenerKey][hostKey][packetID] = df1

bins = np.linspace(0, 3600, 13)
binSize = len(bins)-1

# get all host keys
hostKeySet = set()
for listenerKey,hostValues in newPerListenerDict.items():
    for key in hostValues.keys():
        hostKeySet.add(key)

# stack listener results
for listenerKey,hostValues in newPerListenerDict.items():
    for hostKey in hostKeySet:

        if hostKey.startswith('Civ_'):
            label = '1'
        else:
            label = '-1'

        packets = hostValues.get(hostKey)
        rowData = []
        if packets is None:
            initialRow = ['0','0','0','0']
            for j in range(0,3*binSize):
                rowData.append('0')
        else:
            concatDf = None
            for packetID, packet in packets.items():
                if concatDf is None:
                    concatDf = packet
                else:
                    concatDf = concatDf.append(packet, ignore_index=True)

            groups = concatDf.groupby(pandas.cut(concatDf.start, bins)).mean()

            initialRow = [label, '{:f}'.format(groups['carrierFrequency'][0]),'{:f}'.format(groups['bandwidth'][0]),'{:f}'.format(groups['bitrate'][0])]

            for value in groups['duration']:
                rowData.append('{:f}'.format(value))

            for value in groups['messageLength']:
                rowData.append('{:f}'.format(value))

            for value in groups['iat']:
                    rowData.append('{:f}'.format(value))

            hostExists = True

        if hostKey in finalDict.keys():
            currentList = finalDict[hostKey]
            finalDict[hostKey] = currentList + rowData

            if hostExists:
                finalDict[hostKey][0] = label
                finalDict[hostKey][1] = '{:f}'.format(groups['carrierFrequency'][0])
                finalDict[hostKey][2] = '{:f}'.format(groups['bandwidth'][0])
                finalDict[hostKey][3] = '{:f}'.format(groups['bitrate'][0])
                hostExists = False
        else:
            finalDict[hostKey] = initialRow + rowData

with open('data.csv', 'w') as out_file:
    print('{},{}'.format(fieldnames[0], ','.join(fieldnames[1:])), file=out_file)
    for key, row in finalDict.items():
        print('{},{}'.format(row[0], ','.join(row[1:])), file=out_file)