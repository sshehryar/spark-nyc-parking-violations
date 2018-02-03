##############################################
# SAS786 | Assignment 2 | Task 6
##############################################
#Notes : No Rocket Science - Same as task5 , take 20
#############################################

from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    parking_v = sc.textFile(sys.argv[1], 1)
    req_col = parking_v.mapPartitions(lambda x: reader(x))
    

    plateCount = req_col.map(lambda x: ((x[14],x[16]),1)) \
        .reduceByKey(add).takeOrdered(20, key  = lambda x: -x[1])
    plateCount = sc.parallelize(plateCount)
    
    result = plateCount.map(lambda x: str(x[0]).replace("'","").replace('(','').replace(')','') + '\t' + str(x[1]))
    result.saveAsTextFile("task6.out")

    sc.stop()
