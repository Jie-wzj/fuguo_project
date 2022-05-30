#/bin/python3

import pandas as pd
import numpy as np
import os
from argparse import ArgumentParser
import logging

logging.basicConfig(level=logging.INFO,
                    filename='logs/splitShareValue.log',
                    format='%(asctime)s - %(name)s - %(levelname)-9s - %(filename)-8s : %(lineno)s line - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def parseArgs():
    parser = ArgumentParser()
    parser.add_argument('-i', '--input', help='input path', required=True)
    parser.add_argument('-o', '--output', help='output path', required=True)
    return parser.parse_args()


def splitSingleFile(filename, outputpath):
    if not os.path.exists(filename):
        logging.error('file not exists: %s' % filename)
        return
    df = pd.read_csv(filename)
    logging.info('split file: {}'.format(filename))
    codes = df['Code'].unique()
    files = {}
    data = {}
    for code in codes:
        if os.path.exists(os.path.join(outputpath, code+'.csv')):
            files[code] = open(os.path.join(outputpath, code+'.csv'), 'a')
        else:
            files[code] = open(os.path.join(outputpath, code+'.csv'), 'w')
            files[code].write('Date,ShareValue\n')
        data[code] = []
    for index, row in df.iterrows():
        code = row['Code']
        #files[code].write(str(row['Date'])+','+str(row['s_val_mv'])+'\n')
        data[code].append((row['Date'], row['s_val_mv']))
    for code in codes:
        data[code].sort(key=lambda x: x[0])
        for item in data[code]:
            files[code].write(str(item[0])+','+str(item[1])+'\n')
    for code in codes:
        files[code].close()


def splitAllFiles(inputpath, outputpath):
    files = os.listdir(inputpath)
    files = files[4:]
    for file in files:
        splitSingleFile(os.path.join(inputpath, file), outputpath)


if __name__ == '__main__':
    args = parseArgs()
    splitAllFiles(args.input, args.output)