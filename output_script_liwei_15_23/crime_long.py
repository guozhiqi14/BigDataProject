from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


sc = SparkContext()


def find_long(x):
    if x=='':
        return (x,'float','Longitude','Null')
    try:
        long=float(x)
        if float(long)<-73.699215 and float(long)>-74.257159:
            return (x,'float','Longitude','valid')
        else:
            return (x,'float','Longitude','invalid')
    except ValueError:
        return (x,'float','Longitude','invalid')

if __name__ == "__main__":
    crime = sc.textFile('NYPD_Complaint_Data_Historic.csv',1)
    header = crime.first() 
    crime=crime.filter(lambda line: line != header)
    long_ = crime.mapPartitions(lambda x: reader(x)).map(lambda x: x[22])
    long_clean=long_.map(lambda x: find_long(x)).map(lambda x: x[0]+' '+x[1]+' '+x[2]+' '+x[3])
    long_clean.saveAsTextFile('crime_long.out')

    sc.stop()