import csv
from builtins import print
from collections import defaultdict
from operator import itemgetter



d = defaultdict(list)
complexDict = defaultdict(dict)
combinedDict = defaultdict(dict)

fieldnames = ['host', 'senderName','packet', 'duration', 'start', 'end', 'power', 'carrierFrequency', 'bandwidth', 'bitrate',
              'messageLength', 'iat']

f = open("General-#0.out", "r")
for line in f:
    if line != '\n':
        tokens = line.split()
        if tokens[0] == '[FATAL]':

            rowdata = []
            host = tokens[1]
            hostIds = host.split('.')

            if hostIds[1].startswith('hostL'):
                # hostId
                hostId = hostIds[1]
                rowdata.append(hostId)
                # packet name
                # packet name
                packet = tokens[5].split(')')[1]
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

                d[hostId].append(rowdata)
f.close()

# read the messageLengths, srcAddresses and destAddresses
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

# combine the 2 data sets
for k,v in d.items():
    for i, item in enumerate(v):
        messageLengths = complexDict[k]
        length = messageLengths[i].replace('"','')
        length = length.replace('\n','')
        item.append(length)

        try:
            value = combinedDict[k][item[1]]
        except KeyError:
            combinedDict[k][item[1]] = []
            pass
        combinedDict[k][item[1]].append(item)

rows = []

for topKey,topValue in combinedDict.items():
    for key, items in topValue.items():
        for j, item in enumerate(items):
            if j==0:
                initialArrival= item[4]
                item.append(0)
            else:
                iat = float(item[4]) - float(initialArrival)
                iatAsString = '{:f}'.format(iat)
                item.append(iatAsString)
                initialArrival = item[4]
                rows.append(item)


rows = sorted(rows, key=itemgetter(0))
with open('op.csv', 'w') as out_file:
    # print('{},{}'.format(fieldnames[0], ','.join(fieldnames[1:])), file=out_file)
    for row in rows:
        print('{},{}'.format(row[0],','.join(row[1:])), file=out_file)

# #Assuming res is a list of lists
# with open('op.csv', "w") as output:
#     writer = csv.writer(output, lineterminator='\n')
#     writer.writerows(rows)