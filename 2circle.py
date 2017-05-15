from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader



if __name__ == "__main__":

	if len(sys.argv) != 3:
		print("Usage: bigram <file>", file=sys.stderr)
		exit(-1)
	sc = SparkContext()
	crime = sc.textFile(sys.argv[1], 1)
	station= sc.textFile(sys.argv[2], 1)
	header = crime.first() 
	crime=crime.filter(lambda line: line != header)
	
	crime = crime.mapPartitions(lambda x: reader(x)).filter(lambda x: x[21]!='').map(lambda x: (float(x[21]),float(x[22])))
	station = station.mapPartitions(lambda x: reader(x)).map(lambda x: x[3]).map(lambda x: x.split()).map(lambda x:(float(x[2][:-1]),float(x[1][1:])))

	station_crime=station.cartesian(crime)
	station_crime=station_crime.filter(lambda x: ((x[0][0]-x[1][0])**2+(x[0][1]-x[1][1])**2)>0.00001)
	station_crime=station_crime.filter(lambda x: ((x[0][0]-x[1][0])**2+(x[0][1]-x[1][1])**2)<0.00002)
	station_crime.map(lambda x: (x[0],1)).reduceByKey(add).saveAsTextFile('2circle.out')

	#a=crime.reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1],ascending=False)
	#a=a.map(lambda x:(x[0]+str('\t')+str(x[1])))
	#a.saveAsTextFile('crime_18.out')
	#sc.stop()
