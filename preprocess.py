# from ctypes.wintypes import INT
import shutil

import pandas as pd
import os
import numpy as np
# import sys
import logging
import tqdm

from multiprocessing import Process, Manager

logging.basicConfig(level=logging.DEBUG  # 设置日志输出格式
                    , filename="demo.log"  # log日志输出的文件位置和文件名
                    , format="%(asctime)s - %(name)s - %(levelname)-9s - %(filename)-8s : %(lineno)s line - %(message)s"
                    # 日志输出的格式
                    # -8表示占位符，让输出左对齐，输出长度都为8位
                    , datefmt="%Y-%m-%d %H:%M:%S"  # 时间输出的格式
                    )

# NET_PROFIT_INCL_MIN_INT_INC = 净利润

# T_CODE = ['000001.SZ', '000002.SZ']
T_CODE = None


def read_price_data(file_name):
    """
    Reads the price data from the file and returns a pandas dataframe.
    :param file_name: The name of the file to read.
    :return: A pandas dataframe containing the price data.
    """
    return pd.read_csv(file_name)


def get_price_files(dir_name):
    return [f for f in os.listdir(dir_name) if f.endswith('.csv')]


def get_cashflow_data(filename):  # 季度数据
    df = pd.read_csv(filename)
    if T_CODE is not None:
        codes = T_CODE
    else:
        codes = df['S_INFO_WINDCODE'].unique()

    cashflow_data = {}
    for code in codes:
        # cashflow_data[code] = df[df['S_INFO_WINDCODE'] == code].to_dict()
        cashflow_data[code] = df[df['S_INFO_WINDCODE'] == code]
    return cashflow_data


def get_rev_profit_data(filename):  # 季度数据
    df = pd.read_csv(filename)
    codes = df['S_INFO_WINDCODE'].unique()
    rev_profit_data = {}
    for code in codes:
        rev_profit_data[code] = df[df['S_INFO_WINDCODE'] == code]
    return rev_profit_data


def get_TTM(time):
    year = time // 10000
    month = (time % 10000) // 100
    day = time % 100
    report_date = [331, 630, 930, 1231]
    if month < 3 or month == 3 and day != 31:
        ttm = [(year - 1) * 10000 + report_date[i] for i in range(4)]
    elif month < 6 or month == 6 and day != 30:
        ttm = [(year - 1) * 10000 + report_date[i] for i in range(1, 4)] + [year * 10000 + report_date[i] for i in
                                                                            range(0, 1)]
    elif month < 9 or month == 9 and day != 30:
        ttm = [(year - 1) * 10000 + report_date[i] for i in range(2, 4)] + [year * 10000 + report_date[i] for i in
                                                                            range(0, 2)]
    else:
        ttm = [(year - 1) * 10000 + report_date[i] for i in range(3, 4)] + [year * 10000 + report_date[i] for i in
                                                                            range(0, 3)]
    return ttm


def get_data_TTM(code, date, dataframe, key):
    """
    Calculates the cashflow for a given code and time.
    :param code: The code of the stock.
    :param time: The time of the cashflow.
    :param cashflow_data: The cashflow data.
    :return: The cashflow for the given code and time.
    """
    # if date < 20100000: #cashflow data start from 2019Q1
    #     logging.warning("No enough data before 2010")
    #     return 0
    year = date // 10000
    month = (date % 10000) // 100
    day = date % 100
    report_date = [331, 630, 930, 1231]
    total = 0.0
    try:
        if month < 3 or (month == 3 and day != 31):  # last year
            total = dataframe[dataframe['REPORT_PERIOD'] == (year - 1) * 10000 + report_date[3]][key].values[0]
        elif month < 6 or (month == 6 and day != 30):
            total = dataframe[dataframe['REPORT_PERIOD'] == (year - 1) * 10000 + report_date[3]][key].values[0] \
                    - dataframe[dataframe['REPORT_PERIOD'] == (year - 1) * 10000 + report_date[0]][key].values[0] \
                    + dataframe[dataframe['REPORT_PERIOD'] == year * 10000 + report_date[0]][key].values[0]
        elif month < 9 or (month == 9 and day != 30):
            total = dataframe[dataframe['REPORT_PERIOD'] == (year - 1) * 10000 + report_date[3]][key].values[0] \
                    - dataframe[dataframe['REPORT_PERIOD'] == (year - 1) * 10000 + report_date[1]][key].values[0] \
                    + dataframe[dataframe['REPORT_PERIOD'] == year * 10000 + report_date[1]][key].values[0]
        else:
            total = dataframe[dataframe['REPORT_PERIOD'] == (year - 1) * 10000 + report_date[3]][key].values[0] \
                    - dataframe[dataframe['REPORT_PERIOD'] == (year - 1) * 10000 + report_date[2]][key].values[0] \
                    + dataframe[dataframe['REPORT_PERIOD'] == year * 10000 + report_date[2]][key].values[0]
    except IndexError:
        logging.warning("Index Error:No %s data for %s in %d", key, code, date)
    except KeyError:
        logging.warning("key Error: No %s data for %s in %d", key, code, date)
    if total == 0.0:
        logging.warning("%s_TTM data for %s in %d is 0", key, code, date)
    return total


def get_book_TTM(code, date, dataframe, key):  # 获取营业总收入，用于计算市销率PS
    """
    Calculates the book value for a given code and time.
    :param code: The code of the stock.
    :param date: The time of the revenue.
    :param rev_profit_data: The revenue data.
    :return: The revenue for the given code and time.
    """
    # if date < 20100000:  # rev_profit data start from 2019Q1
    #     logging.warning("No enough rev_profit data before 2010")
    #     return 0
    TTM = get_TTM(date)
    total_bv = 0.0
    count = 0
    for t in TTM:
        try:
            bv = dataframe[dataframe['REPORT_PERIOD'] == t]['TOT_SHRHLDR_EQY_INCL_MIN_INT'].values[0]
            count += 1
        except IndexError:
            logging.warning("No rev data for %s in %d", code, t)
            bv = 0.0
        except KeyError:
            logging.warning("key Error: No TOT_SHRHLDR_EQY_INCL_MIN_INT data for %s in %d", code, date)
            bv = 0.0
        total_bv += bv
    total_bv = total_bv / count if count != 0 else 0
    if total_bv == 0.0:
        logging.warning("book value TTM for %s in %d is 0", code, date)
    return total_bv


def calc_valuation(fr_path, fw_path, net_val_path):
    net_data = np.load(net_val_path, allow_pickle=True).tolist()
    net_keys = net_data.keys()

    fw = open(fw_path, 'w')
    with open(fr_path, 'r') as fr:
        lines = tqdm.tqdm(fr.readlines(), unit='MB', desc=f'pid:{os.getpid():5d}')
        for line in lines:
            line = line.strip(',').strip('\n').split(',')
            if T_CODE is not None and line[3] not in T_CODE:
                continue
            if line[0].startswith('s_val_mv'):
                continue
            date = int(line[4])
            if date < 20100000:  # cashflow data start from 2019Q1
                continue
            if line[1] == '':
                continue
            shizhi = float(line[1])
            code = line[3]
            if code not in net_keys:
                continue

            # df = pd.DataFrame(data[code])
            df = net_data[code]
            profit_TTM = get_data_TTM(code, date, df, 'NET_PROFIT_INCL_MIN_INT_INC')
            shrhld_TTM = get_book_TTM(code, date, df, 'TOT_SHRHLDR_EQY_INCL_MIN_INT')
            revenue_TTM = get_data_TTM(code, date, df, 'TOT_OPER_REV')
            cashflow_TTM = get_data_TTM(code, date, df, 'NET_CASH_FLOWS_OPER_ACT')
            # print("%s, %d, %.2f, %.2f, %.2f, %.2f, %.2f" % (code, date, shizhi, profit_TTM, shrhld_TTM, revenue_TTM, cashflow_TTM))
            PE = shizhi / profit_TTM if profit_TTM != 0 else 0
            PB = shizhi / shrhld_TTM if shrhld_TTM != 0 else 0
            PS = shizhi / revenue_TTM if revenue_TTM != 0 else 0
            PCF = shizhi / cashflow_TTM if cashflow_TTM != 0 else 0
            # print("%s,%d,%.2f,\t%.2f,\t%.2f,\t%.2f,\t%.2f" % (code, date, shizhi, PE, PB, PS, PCF))
            fw.write("%s,%d,%.2f,%.2f,%.2f,%.2f\n" % (code, date, PE, PB, PS, PCF))
            # fw.write("%s,%d,%.2f,%.2f,%.2f,%.2f\n" % (code, date, 0, PB, 0, 0))
            # print("%s,%d,%.2f\n" % (code, date, shizhi))
    # fr.close()
    fw.close()


def market_val_split(f_path, split_tmp_paths, tmp_file='tmp', n=4):
    os.makedirs(tmp_file, exist_ok=True)

    # split_tmp_paths = [os.path.join(tmp_file, f"split_tmp_{i}.csv") for i in range(n)]

    fws = [open(path, 'w') for path in split_tmp_paths]
    print('\n', '=' * 30, ' Split Files ', '=' * 30)
    with open(f_path, 'r') as fr:
        for i, line in enumerate(tqdm.tqdm(fr.readlines(), unit='MB')):
            # if i >= 750000:
            #     break
            fws[i % n].write(line)

    for fw in fws:
        fw.close()
    return split_tmp_paths


def market_val_comb(save_tmp_paths, columns, save_path='res.csv', n=4):
    # columns = ['code', 'date', 'PE', 'PB', 'PS', 'PCF']
    df = pd.DataFrame(columns=columns)
    # print('\n', '=' * 30, ' Combine Files ', '=' * 30)
    for i in range(n):
        df_tmp = pd.read_csv(save_tmp_paths[i], names=columns, header=None)

        # df = pd.merge(df, df_tmp, on=columns[0:2], how='outer', sort=True)
        df = df.append(df_tmp)

    df.to_csv(save_path, index=False)
    print('Save data to ', os.path.abspath(save_path))


def main(market_val_path='data/shizhi.csv', net_val_path='data/data_save.npy', save_path='res.csv', tmp_file='tmp',
         nproc=4):
    split_tmp_paths = [os.path.join(tmp_file, f"split_tmp_{i}.csv") for i in range(nproc)]
    save_tmp_paths = [os.path.join(tmp_file, f"save_tmp_{i}.csv") for i in range(nproc)]
    columns = ['code', 'date', 'PE', 'PB', 'PS', 'PCF']

    market_val_split(market_val_path, split_tmp_paths, tmp_file, nproc)

    print('\n', '=' * 30, ' Valuation Calculation ', '=' * 30)
    # data = np.load(net_val_path, allow_pickle=True).tolist()
    # keys = data.keys()
    # with Manager() as manager:
    #     # net_data = manager.dict(data)
    #     net_keys = manager.list(keys)
    #     # net_keys.append(keys)
    #     net_val = manager.dict()
    #     net_val['data'] = data
    #     # net_val['keys'] = keys

    process_list = []
    for i in range(nproc):
        p = Process(target=calc_valuation, args=(split_tmp_paths[i], save_tmp_paths[i], net_val_path))
        p.start()
        process_list.append(p)

    for p in process_list:
        p.join()

    market_val_comb(save_tmp_paths, columns, save_path, nproc)

    # shutil.rmtree(tmp_file)

if __name__ == '__main__':
    # price_files = get_price_files('capital')
    # cashflow_data = get_cashflow_data('data/xianjinliu.csv')
    # print("load cashflow data finished")
    # rev_profit_data = get_rev_profit_data('data/jingzhi.csv')
    # print("load rev_profit data finished")
    # data = get_cashflow_data('data/merged.csv')
    # np.save('data/data_save.npy', data, allow_pickle=True)

    main(market_val_path='data/shizhi.csv', net_val_path='data/data_save.npy', save_path='res_new.csv', tmp_file='tmp',
         nproc=8)