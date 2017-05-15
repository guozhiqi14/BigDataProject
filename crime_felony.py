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
	crime = crime.mapPartitions(lambda x: reader(x)).filter(lambda x: x[11]=='FELONY')
	crime = crime.map(lambda x:(x[1],1)).reduceByKey(lambda x,y:x+y)
	crime = crime.map(lambda x: x[0]+str(',')+str(x[1]))
	crime.saveAsTextFile('crime_felony.out')
	sc.stop()
