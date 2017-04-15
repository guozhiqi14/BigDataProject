from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def find_hd(x):
	if x=='':
		return (x,'TEXT','HADEVELOPT','Null')

	else:
		return(x,'TEXT','HADEVELOPT','Valid')
           

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: bigram <file>", file=sys.stderr)
		exit(-1)
	sc = SparkContext()
	crime = sc.textFile(sys.argv[1], 1)
	
	crime = crime.mapPartitions(lambda x: reader(x)).map(lambda x: x[18]).map(lambda x:find_hd(x))
	a=crime.map(lambda x: x[0]+' '+x[1]+' '+x[2]+' '+x[3])
	a.saveAsTextFile('crime_hd.out')
	sc.stop()