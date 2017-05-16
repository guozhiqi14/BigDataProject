from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
        if len(sys.argv) != 2:
                exit(-1)
                
        def mapper(entry):
                return ('%s,%s' % (entry[8],entry[9]), 1)

        sc = SparkContext()

        lines = sc.textFile(sys.argv[1], 1)
        thisfile  = lines.mapPartitions(lambda x: reader(x))
  
        pv = thisfile.map(mapper).reduceByKey(lambda x,y: x+y).sortBy(lambda x: eval(x[0][0:3]),False)
        pv.map(lambda x: '%s\t%d' %(x[0],x[1])).saveAsTextFile("desc1.out")
    
        sc.stop()
