from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader



if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: bigram <file>", file=sys.stderr)
		exit(-1)
	sc = SparkContext()
	crime = sc.textFile(sys.argv[1], 1)
	
	crime = crime.mapPartitions(lambda x: reader(x)).\
map(lambda x: (x[23],1)).filter(lambda x: x[0]=='')
	a=crime.reduceByKey(lambda x,y: x+y)
	a=a.map(lambda x:x[0]+str('\t')+str(x[1]))
	a.saveAsTextFile('crime_23.out')
	sc.stop()
