############################################
# SAS786 | Assignment 2 | Task5
###########################################
# Notes: simple wordcount then sort then use take to get highest val
# sort in descending!
#####################################################################

from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    parking_v = sc.textFile(sys.argv[1], 1)
    req_col = parking_v.mapPartitions(lambda x: reader(x))
    


    plateCount = req_col.map(lambda x: ((x[14],x[16]),1))\
        .reduceByKey(add).takeOrdered(1, key  = lambda x: -x[1])
    plateCount = sc.parallelize(plateCount)
    result = plateCount.map(lambda x: str(x[0]).replace("'","").replace('(','').replace(')','') + '\t' + str(x[1]))

    result.saveAsTextFile("task5.out")

    sc.stop()
