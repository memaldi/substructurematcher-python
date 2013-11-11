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
    sourceG = subdir.split('.g-')[0] + '.g'
    targetG = subdir.split('.g-')[1]
    sourcePath = INPUT_DIR + '/' + subdir + '/' + sourceG
    f = open(sourcePath, 'r')
    total1 = 0
    for line in f:
        if line.find('v') == 0 or line.find('d') == 0:
            total1 += 1
    f.close()
    if sourceG not in result_dict:
        result_dict[sourceG] = {}
    
    targetPath = INPUT_DIR + '/' + subdir + '/' + targetG
    f = open(targetPath, 'r')
    total2 = 0
    for line in f:
        if line.find('v') == 0 or line.find('d') == 0:
            total2 += 1
    f.close()
    #if sourcePath != targetPath:
    proc = subprocess.Popen([SUBDUE_DIR + "gm", sourcePath, targetPath], stdout=subprocess.PIPE)
    stdout = proc.stdout.read()
    value = stdout[stdout.find('Match Cost = ') + len('Match Cost = '):stdout.find('\n')]
    normalized_cost = float(value.split('.')[0]) / (total1 + total2)
    result_dict[sourceG][targetG] = normalized_cost
    
#print result_dict

csv = open(OUTPUT_FILE, 'w')

keys = result_dict.keys()
keys.sort()
strKeys = 'Datasets'
for key in keys:
    strKeys += ',' + key.split('.')[0]

csv.write(strKeys + '\n')

for sourceKey in keys:
    newRow = sourceKey.split('.')[0]
    for targetKey in keys:
        if sourceKey == targetKey:
            newRow += ',' + str(0)
        else:
            newRow += ',' + str(result_dict[sourceKey][targetKey])
    #print newRow
    csv.write(newRow + '\n')

csv.close()
