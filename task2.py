#####################################
#SAS786 | Assignment 2 
####################################
from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    #standard code to read csv
    parking_v = sc.textFile(sys.argv[1], 1)
    parking_v = parking_v.mapPartitions(lambda x: reader(x))
    #extract violation code i.e column 2
    req_col = parking_v.map(lambda line: (line[2]))
    #calculate frequence of violation code | similar to bigram.py
    freq_violation_types = req_col.map(lambda x: (x,1)).reduceByKey(add)
    # map and save 
    result = freq_violation_types.map(lambda x: x[0] + '\t' + str(x[1]))
    result.saveAsTextFile("task2.out")

    sc.stop()
