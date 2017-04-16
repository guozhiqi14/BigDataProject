from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re

sc = SparkContext()

def match_ll(x):
	if x[2]=='':
		return (x[2],'Location','Lat_Lon','null')
	try:
		filter_x=re.findall("\d+\.\d+", x[2])
		if len(filter_x)!=2:
			return (x[2],'Location','Lat_Lon','invalid')
		elif filter_x[0]!=x[0] or '-'+filter_x[1]!=x[1]:
			return (x[2],'Location','Lat_Lon','invalid')
		else:
			return (x[2],'Location','Lat_Lon','valid')
	except TypeError:
		return (x[2],'Location','Lat_Lon','invalid')

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: bigram <file>", file=sys.stderr)
		exit(-1)
	crime = sc.textFile(sys.argv[1],1)
	header = crime.first() 
	crime=crime.filter(lambda line: line != header)
	ll = crime.mapPartitions(lambda x: reader(x)).map(lambda x: [x[21],x[22],x[23]]).map(lambda x:match_ll(x))
	ll_clean=ll.map(lambda x: x[0]+' '+x[1]+' '+x[2]+' '+x[3])
	ll_clean.saveAsTextFile('crime_ll.out')
    

