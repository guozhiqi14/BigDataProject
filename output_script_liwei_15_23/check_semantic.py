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
	
	crime = crime.map(lambda x:x.split(" ")).map(lambda x:(x[-2],1)).reduceByKey(lambda x,y: x+y)
	crime.saveAsTextFile('check_semantic.out')
	sc.stop()