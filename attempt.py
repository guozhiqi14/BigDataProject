from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
        if len(sys.argv) != 2:
                exit(-1)

        sc = SparkContext()

        lines = sc.textFile(sys.argv[1], 1)
        thisfile  = lines.mapPartitions(lambda x: reader(x))
  
        pv = thisfile.map(lambda col: (col[10],1)).reduceByKey(lambda x,y: x+y)
        pv.map(lambda x: '%s\t%d' %(x[0],x[1])).saveAsTextFile("attempt.out")
    
        sc.stop()
