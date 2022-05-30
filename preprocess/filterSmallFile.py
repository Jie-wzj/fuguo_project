import os
import pandas as pd
import smallFiles


rootPath = '..'
#fw = open('smallFiles.py', 'w')
#fw.write('smallFiles = [')
#files = os.listdir('cleanedData/merged')
for file in smallFiles.smallFiles:
    #df = pd.read_csv(os.path.join('cleanedData/merged', file))
    #if os.path.getsize('cleanedData/merged/'+file) < 200000 or int(df.head(1)['date']) > 20170101:
    #   fw.write(f'\'{file}\',')
    #   print(file)
    if not os.path.exists(os.path.join(rootPath, 'cleanedData/merged/'+file)):
        continue
    os.rename(os.path.join(rootPath, 'cleanedData/merged/'+file), os.path.join('cleanedData/merged/smallFiles/'+file))
print(len(smallFiles.smallFiles))
#fw.write(']')
#fw.close()
