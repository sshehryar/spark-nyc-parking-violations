#####################################
#SAS786 | Assignment 2 
#####################################
from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    
    #process parking-violations.csv
    parking_v = sc.textFile(sys.argv[1], 1)
    parking_v = parking_v.mapPartitions(lambda x: reader(x))
    parking_v = parking_v.filter(lambda line: len(line)>1) \
        .map(lambda line: (line[0], str(line[14]) + ', ' + str(line[6]) + ', ' + str(line[2]) + ', ' + str(line[1])))
    #process open-violation.csv
    open_v = sc.textFile(sys.argv[2],1)
    open_v = open_v.mapPartitions(lambda x: reader(x)) 
    open_v = open_v.filter(lambda line: len(line)>1) \
        .map(lambda line: (line[0], str(line[1]) + ', ' + str(line[5]) + ', ' + str(line[7]) + ', ' + str(line[9])))
    #join both csvs and do a subtract by Summon number
    join_open_park=parking_v.join(open_v)

    get_result=parking_v.subtractByKey(join_open_park)
    
    output = get_result.map(lambda r:"\t".join([str(c) for c in r]))
    
    output.saveAsTextFile("task1.out")
    
    sc.stop()
