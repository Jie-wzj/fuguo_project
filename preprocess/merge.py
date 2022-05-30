from sre_parse import parse_template
import pandas as pd
import numpy as np
import logging
import os

rootPath = '..'

logging.basicConfig(level=logging.INFO,
                    filename='logs/merge.log',
                    format='%(asctime)s - %(name)s - %(levelname)-9s - %(filename)-8s : %(lineno)s line - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

def getTTM(time):
    year = time // 10000
    month = (time % 10000) // 100
    day = time % 100
    report_date = [331, 630, 930, 1231]
    if month < 3 or month == 3 and day != 31:
        ttm = [(year - 1) * 10000 + report_date[i] for i in range(4)]
    elif month < 6 or month == 6 and day != 30:
        ttm = [(year - 1) * 10000 + report_date[i] for i in range(1, 4)] + [year * 10000 + report_date[i] for i in range(0, 1)]
    elif month < 9 or month == 9 and day != 30:
        ttm = [(year - 1) * 10000 + report_date[i] for i in range(2, 4)] + [year * 10000 + report_date[i] for i in range(0, 2)]
    else:
        ttm = [(year - 1) * 10000 + report_date[i] for i in range(3, 4)] + [year * 10000 + report_date[i] for i in range(0, 3)]
    return ttm


def getLF(date):
    year = date // 10000
    month = (date % 10000) // 100
    day = date % 100
    report_date = [331, 630, 930, 1231]
    if month < 3 or month == 3 and day != 31:
        lf = (year - 1) * 10000 + report_date[3]
    elif month < 6 or month == 6 and day != 30:
        lf = year * 10000 + report_date[0]
    elif month < 9 or month == 9 and day != 30:
        lf = year * 10000 + report_date[1]
    else:
        lf = year * 10000 + report_date[2]
    return lf


def getBookvalueLF(code, date, data):
    lf = getLF(date)
    if (lf not in data.keys()):
        logging.warning(f'no bookvalue data of {code} at {lf}')
        return np.nan
    return data[lf]


def getCashflowTTM(code, date, data):
    ttm = getTTM(date)
    cashflowTTM = []
    for i in range(len(ttm)):
        if ttm[i] in data.keys():
            cashflowTTM.append(data[ttm[i]])
        else:
            logging.warning(f'no cashflow data of {code} at {ttm[i]}')
    if len(cashflowTTM) < 4:
        logging.warning(f'no enough cashflow data of {code} at {date}')
        return np.nan
    return np.average(cashflowTTM)


def getSaleTTM(code, date, data):
    year = date // 10000
    month = (date % 10000) // 100
    day = date % 100
    report_date = [331, 630, 930, 1231]
    saleTTM = 0.0
    try:
        if month < 3 or (month == 3 and day != 31):  # last year
            saleTTM = data[(year - 1) * 10000 + report_date[3]]
        elif month < 6 or (month == 6 and day != 30):
            saleTTM = data[(year-1)*10000+report_date[3]] \
                    - data[(year-1)*10000+report_date[0]] \
                    + data[year*10000+report_date[0]]
        elif month < 9 or (month == 9 and day != 30):
            saleTTM = data[(year-1)*10000+report_date[3]] \
                    - data[(year-1)*10000+report_date[1]] \
                    + data[year*10000+report_date[1]]
        else:
            saleTTM = data[(year-1)*10000+report_date[3]] \
                    - data[(year-1)*10000+report_date[2]] \
                    + data[year*10000+report_date[2]]
    except IndexError:
        logging.warning("Index Error:No sale data for %s in %d", code, date)
    except KeyError:
        logging.warning("key Error: No sale data for %s in %d", code, date)
    if saleTTM == 0.0:
        saleTTM = np.nan
        logging.warning("sale_TTM data for %s in %d is 0", code, date)
    return saleTTM


def getEarningTTM(code, date, data):
    date = (date // 10000 - 1) * 10000 + 1231
    if not date in data.keys():
        #logging.warning(f'no earning data of {code} at {date}')
        return np.nan
    return data[date]


def getBookvalue(code):
    try:
        df = pd.read_csv(os.path.join(rootPath, 'cleanedData/bookvalue', code+'.csv'))
    except FileNotFoundError:
        logging.warning(f'no bookvalue data of {code}')
        return {}
    bookvalue = {}
    for index, row in df.iterrows():
        bookvalue[int(row['REPORT_PERIOD'])] = row['bookvalue']
    return bookvalue


def getSale(code):
    try:
        df = pd.read_csv(os.path.join(rootPath, 'cleanedData/sale', code+'.csv'))
    except FileNotFoundError:
        logging.warning(f'no sale data of {code}')
        return {}
    sale = {}
    for index, row in df.iterrows():
        sale[int(row['REPORT_PERIOD'])] = row['sale']
    return sale


def getEarning(code):
    try:
        df = pd.read_csv(os.path.join(rootPath, 'cleanedData/earning', code+'.csv'))
    except FileNotFoundError:
        logging.warning(f"no earning data for {code}")
        return {}           
    earning = {}
    for index, row in df.iterrows():
        earning[int(row['REPORT_PERIOD'])] = row['earning']
    return earning


def getCashflow(code):
    try:
        df = pd.read_csv(os.path.join(rootPath, 'cleanedData/cashflow', code+'.csv'))
    except FileNotFoundError:
        logging.warning(f'no cashflow data of {code}')
        return {}
    cashflow = {}
    for index, row in df.iterrows():
        cashflow[int(row['REPORT_PERIOD'])] = row['cashflow']
    return cashflow


def merge(codes, outputpath):
    for code in codes:
        fd = open(os.path.join(outputpath, code+'.csv'), 'w')
        fd.write('date, sharevalue, bookvalueLF, saleTTM, earningTTM, cashflowTTM, PB_LF, PS_TTM, PE_LF, PCF_LF\n')
        df = pd.read_csv(os.path.join('cleanedData/capital', code+'.csv'))
        bookValue = getBookvalue(code)
        sale = getSale(code)
        earning = getEarning(code)
        cashflow = getCashflow(code)
        for index, row in df.iterrows():
            share = float(row['ShareValue'])
            date = int(row['Date'])
            if date < 20100101:
                continue
            bookvalueLF = PB_LF = np.nan
            if len(bookValue) > 0:
                bookvalueLF = getBookvalueLF(code, date, bookValue)
                if not np.isnan(bookvalueLF):
                    PB_LF = share / bookvalueLF
            saleTTM = PS_TTM = np.nan
            if len(sale) > 0:
                saleTTM = getSaleTTM(code, date, sale)
                if not np.isnan(saleTTM):
                    try:
                        PS_TTM = share / saleTTM
                    except ZeroDivisionError:
                        logging.warning(f'{code} at {date}')
            earningTTM = PE_TTM = np.nan
            if len(earning) > 0:
                earningTTM = getEarningTTM(code, date, earning)
                if not np.isnan(earningTTM):
                    PE_TTM = share / earningTTM
            cashflowTTM = PCF_TTM = np.nan
            if len(cashflow) > 0:
                cashflowTTM = getCashflowTTM(code, date, cashflow)
                if not np.isnan(cashflowTTM):
                    PCF_TTM = share / cashflowTTM
            fd.write('{}, {}, {}, {}, {}, {}, {}, {}, {}, {}\n'.format(date, share, bookvalueLF, saleTTM, earningTTM, cashflowTTM, PB_LF, PS_TTM, PE_TTM, PCF_TTM))
        fd.close()


if __name__ == '__main__':
    codes = os.listdir(os.path.join(rootPath, 'cleanedData/capital'))
    codes = [code[:-4] for code in codes]
    merge(codes, os.path.join(rootPath, 'cleanedData/merged'))