#/bin/python3

import pandas as pd
import numpy as np
import os
from argparse import ArgumentParser
import logging

logging.basicConfig(level=logging.INFO,
                    filename='logs/splitBookValue.log',
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
    codes = df['WIND_CODE'].unique()
    files = {}
    data = {}
    for code in codes:
        if os.path.exists(os.path.join(outputpath, code+'.csv')):
            files[code] = open(os.path.join(outputpath, code+'.csv'), 'a')
        else:
            files[code] = open(os.path.join(outputpath, code+'.csv'), 'w')
            files[code].write('ANN_DT,REPORT_PERIOD,bookvalue\n')
        data[code] = []
    for index, row in df.iterrows():
        code = row['WIND_CODE']
        #files[code].write(str(row['Date'])+','+str(row['s_val_mv'])+'\n')
        data[code].append((row['ANN_DT'], row['REPORT_PERIOD'], row['TOT_SHRHLDR_EQY_EXCL_MIN_INT']))
    for code in codes:
        data[code].sort(key=lambda x: x[1])
        for item in data[code]:
            files[code].write(str(item[0])+','+str(item[1])+','+str(item[2])+'\n')
    for code in codes:
        files[code].close()


def splitAllFiles(inputpath, outputpath):
    files = os.listdir(inputpath)
    for file in files:
        splitSingleFile(os.path.join(inputpath, file), outputpath)


if __name__ == '__main__':
    args = parseArgs()
    splitSingleFile(args.input, args.output)