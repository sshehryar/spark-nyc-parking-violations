######################################
#SAS786 | Assignment2 | Task 4
######################################
#Note to self: use the method format 
# in lab 4 slides for if else
# Alternatives: filter 
# Alternative2 : when().otherwise()
#####################################

from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    parking_v = sc.textFile(sys.argv[1], 1)
    req_col = parking_v.mapPartitions(lambda x: reader(x))
    req_col = req_col.map(lambda x: x[16])
    
    def state(x):
        if x == 'NY': 
            return ('NY', 1)
        else:
            return ('Other', 1)

    #####################################################################################
    #Alternative ways:     
    #ny = rows.filter(lambda x: x == 'NY')
    #ny_count = ny.count()
    #Alternative3: ny_count = ny.flatMap(lambda doc: [(x, 1) for x in doc.split(' ')]).reduceByKey(add)
    #other = rows.filter(lambda x: x!='NY')
    #other_count = other.count()
    ####################################################################################  


    freq = req_col.map(lambda x : state(x))

    result = freq.reduceByKey(add)
    result = result.map(lambda x: x[0] + '\t' + str(x[1]))
    result.saveAsTextFile("task4.out")

    sc.stop()
