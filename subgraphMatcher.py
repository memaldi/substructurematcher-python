import sys
from os import listdir
import subprocess
import matplotlib.pyplot as plt
import numpy as np

SUBDUE_DIR='/home/mikel/programas/subdue-5.2.2/bin/'

INPUT_DIR = sys.argv[1]
OUTPUT_FILE = sys.argv[2]

THRESHOLD = 0.8

tp = 0
fp = 0
fn = 0
un = 0

result_dict = {}

test_list = ['budapest.g', 'courseware.g', 'deepblue.g', 'deploy.g', 'eurecom.g', 'ft.g', 'hedatuz.g', 'ibm.g', 'ieee.g', 'irit.g', 'jisc.g', 'newcastle.g', 'darmstadt.g', 'reegle.g', 'risk.g', 'roma.g', 'sdg.g', 'southampton.g', 'ulm.g']

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
    #if sourceG not in result_dict:
        #result_dict[sourceG] = {}
    
    targetPath = INPUT_DIR + '/' + subdir + '/' + targetG
    f = open(targetPath, 'r')
    total2 = 0
    for line in f:
        if line.find('v') == 0 or line.find('d') == 0:
            total2 += 1
    f.close()
    #if sourcePath != targetPath:
    if total1 > 1 and total2 > 1:
        if sourceG not in result_dict:
                result_dict[sourceG] = {}
        proc = subprocess.Popen([SUBDUE_DIR + "gm", sourcePath, targetPath], stdout=subprocess.PIPE)
        stdout = proc.stdout.read()
        value = stdout[stdout.find('Match Cost = ') + len('Match Cost = '):stdout.find('\n')]
        normalized_cost = 1 - (float(value.split('.')[0]) / (total1 + total2))
        if sourceG == targetG:
            pass
        if normalized_cost >= THRESHOLD and sourceG in test_list and targetG in test_list:
            tp += 1
            print 'True positive! %s - %s (%s)' % (sourceG, targetG, normalized_cost)
        elif normalized_cost >= THRESHOLD and ((sourceG not in test_list and targetG in test_list) or (sourceG in test_list and targetG not in test_list)):
            fp += 1
            print 'False positive! %s - %s (%s)' % (sourceG, targetG, normalized_cost)
        elif normalized_cost < THRESHOLD and sourceG in test_list and targetG in test_list:
            fn += 1
            print 'False negative! %s - %s (%s)' % (sourceG, targetG, normalized_cost)
        else:
            print 'Unknown! %s - %s (%s)' % (sourceG, targetG, normalized_cost)
            un += 1
        result_dict[sourceG][targetG] = normalized_cost
    
print 'True positives: %s' % tp
print 'False positives: %s' % fp
print 'False negatives: %s' % fn
print 'Unknown: %s' % un

precision = float(tp) / (tp + fp)
print 'Precision: %s' % str(precision)

recall = float(tp) / (tp + fn)
print 'Recall: %s' % str(recall)
#print result_dict

f1 =  2 * float(precision * recall) / (precision + recall)
print 'F1 Score: %s' % str(f1)

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
            newRow += ',' + str(1)
        else:
            newRow += ',' + str(result_dict[sourceKey][targetKey])
    #print newRow
    csv.write(newRow + '\n')

csv.close()

test_dict = {}

for item1 in test_list:
    test_dict[item1] = {}
    for item2 in test_list:
        if item1 == item2:
            test_dict[item1][item2] = 1
        else:
            test_dict[item1][item2] = result_dict[item1][item2]

csv = open('test-alignment.csv', 'w')
keys = test_dict.keys()
keys.sort()
strKeys = 'Datasets'
for key in keys:
    strKeys += ',' + key.split('.')[0]

csv.write(strKeys + '\n')

for sourceKey in keys:
    newRow = sourceKey.split('.')[0]
    for targetKey in keys:
        if sourceKey == targetKey:
            newRow += ',' + str(1)
        else:
            newRow += ',' + str(test_dict[sourceKey][targetKey])
    #print newRow
    csv.write(newRow + '\n')

csv.close()
