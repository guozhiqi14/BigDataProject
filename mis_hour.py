from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
import os
from csv import reader

#Count the total misdemeanor complaints by hour of day 2006 - 2016
#output: key = hour, value = count


if __name__ == "__main__":
        if len(sys.argv) != 2:
        exit(-1)

        def is_misdemeanor(x):
                if 'MISDEMEANOR' in x:
                        return True
                else:
                        return False
        sc = SparkContext()
        lines= sc.textFile(sys.argv[1], 1)
        thisfile  = lines.mapPartitions(lambda x: reader(x))
        thismap   = thisfile.map(lambda col: (col[2], col[11])).filter(lambda x: is_misdemeanor(x[1])).map(lambda x: (x[0].split(':')[0], 1))
        result    = thismap.reduceByKey(lambda x,y: x+y).sortByKey(ascending=True).saveAsTextFile("mis_hour.out")
