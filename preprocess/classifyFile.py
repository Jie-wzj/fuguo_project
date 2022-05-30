import os
import pandas as pd
import shutil


def getL1List(dir):
    """
    Get the list of L1 names from the filepath
    """
    L1List = []
    for name in os.listdir(dir):
        if name.endswith("_wet"):
            L1List.append(name[:-4])
    return L1List


def getL1ToCode(dir, L1List):
    """
    Get the list of L1 codes from the filepath
    """
    Codes = {}
    for l1 in L1List:
        Codes[l1] = []
    for name in L1List:
        curdir = os.path.join(dir, name+"_wet")
        files = os.listdir(curdir)
        for file in files:
            df = pd.read_csv(os.path.join(curdir, file))
            codes = df['Code'].unique()
            for code in codes:
                if code not in Codes[name]:
                    Codes[name].append(code)
    return Codes


def getCodeToL1(dir, L1List):
    """
    Get the list of L1 codes from the filepath
    """
    Codes = {}
    for name in L1List:
        curdir = os.path.join(dir, name+"_wet")
        files = os.listdir(curdir)
        for file in files:
            df = pd.read_csv(os.path.join(curdir, file))
            codes = df['Code'].unique()
            for code in codes:
                if code not in Codes.keys():
                    Codes[code] = name
    return Codes


if __name__ == '__main__':
    L1List = getL1List("../rawData/citics_l1_info/")
    for l1 in L1List:
        if not os.path.exists("../cleanedData/classified/"+l1):
            os.makedirs("../cleanedData/classified/"+l1)
    codeToL1 = getCodeToL1("../rawData/citics_l1_info/", L1List)
    fileList = os.listdir("../cleanedData/merged/")
    notClassified = open("../notClassified.txt", "w")
    for file in fileList:
        if file.endswith(".csv"):
            code = file[:-4]
            try:
                l1 = codeToL1[code]
            except KeyError:
                notClassified.write(code+"\n")
                continue
            if not os.path.exists(os.path.join("../cleanedData/classified/", l1, file)):
                shutil.copyfile(os.path.join("../cleanedData/merged/", file),
                                os.path.join("../cleanedData/classified/", l1, file))
