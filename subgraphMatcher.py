import sys
from os import listdir
import subprocess
import matplotlib.pyplot as plt
import numpy as np

SUBDUE_DIR='/home/mikel/programas/subdue-5.2.2/bin/'

INPUT_DIR = sys.argv[1]
OUTPUT_FILE = sys.argv[2]

result_dict = {}

for subdir in listdir(INPUT_DIR):
    sourceG = subdir.split('-')[0]
    targetG = subdir.split('-')[1]
    sourcePath = INPUT_DIR + '/' + subdir + '/' + sourceG
    if sourceG not in result_dict:
        result_dict[sourceG] = {}
    
    targetPath = INPUT_DIR + '/' + subdir + '/' + targetG
    #if sourcePath != targetPath:
    proc = subprocess.Popen([SUBDUE_DIR + "gm", sourcePath, targetPath], stdout=subprocess.PIPE)
    stdout = proc.stdout.read()
    value = stdout[stdout.find('Match Cost = ') + len('Match Cost = '):stdout.find('\n')]
    result_dict[sourceG][targetG] = value.split('.')[0]
    
print result_dict

csv = open(OUTPUT_FILE, 'w')

keys = result_dict.keys()
strKeys = 'Datasets'
for key in keys:
    strKeys += ',' + key.split('.')[0]

csv.write(strKeys + '\n')

for sourceKey in keys:
    newRow = sourceKey.split('.')[0]
    for targetKey in keys:
        newRow += ',' + result_dict[sourceKey][targetKey]
    print newRow
    csv.write(newRow + '\n')

csv.close()
