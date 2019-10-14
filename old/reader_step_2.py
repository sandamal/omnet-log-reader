from builtins import print
from collections import defaultdict
from operator import itemgetter
import pandas
import numpy as np

rows = []
complexDict = defaultdict(dict)
constantDict = defaultdict(dict)

fieldnames = ['senderName', 'label', 'carrierFrequency', 'bandwidth', 'bitrate', 'duration',
              'messageLength', 'iat']


f = open("op.csv", "r")
for line in f:
    tokens = line.split(',')
    if tokens[1] != 'senderName':
        if tokens[1].startswith('Civ_'):
            label = '-1'
        else:
            label = '1'
        senderName = tokens[1]

        duration = tokens[3]
        carrierFrequency = tokens[7]
        bandwidth = tokens[8]
        bitrate = tokens[9]
        messageLength = tokens[10]
        iat = tokens[11]
        start = float(tokens[4])
        df = pandas.DataFrame({'duration': [duration],
                               'messageLength':[messageLength],
                               'iat':[iat],
                               'start':[start]},dtype=float)

        staticValues = {'senderName': senderName,
                               'carrierFrequency':carrierFrequency,
                               'bandwidth':bandwidth,
                               'bitrate':bitrate,
                               'label':label}

        if senderName in complexDict.keys():
            result = complexDict[senderName].append(df, ignore_index=True)
            complexDict[senderName] = result
        else:
            complexDict[senderName] = df
            constantDict[senderName] = staticValues

f.close()


# make 1 row per srcAddress by averaging
for key,df in complexDict.items():
    staticValues = constantDict[key]

    rowData = [staticValues['label'],staticValues['carrierFrequency'],staticValues['bandwidth'],staticValues['bitrate']]

    bins = np.linspace(0, 3600, 13)
    groups = df.groupby(pandas.cut(df.start, bins)).mean()

    for value in groups['duration']:
        rowData.append('{:f}'.format(value))

    for value in groups['messageLength']:
        rowData.append('{:f}'.format(value))

    for value in groups['iat']:
            rowData.append('{:f}'.format(value))

    rows.append(rowData)

with open('data.csv', 'w') as out_file:
    print('{},{}'.format(fieldnames[0], ','.join(fieldnames[1:])), file=out_file)
    for row in rows:
        print('{},{}'.format(row[0], ','.join(row[1:])), file=out_file)
