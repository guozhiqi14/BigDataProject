from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re


sc = SparkContext()

### Column Latitude
def find_yc(x):
    if x=='':
        return (x,'float','Y_cordinate','Null')
    try:
        y_c=float(x)
        if y_c<272844.293640 and y_c>120121.779352:
            return (x,'float','Y_cordinate','valid')
        else:
            return (x,'float','Y_cordinate','invalid')
    except ValueError:
        return (x,'float','Y_cordinate','invalid')

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bigram <file>", file=sys.stderr)
        exit(-1)
    crime = sc.textFile(sys.argv[1],1)
    header = crime.first() 
    crime=crime.filter(lambda line: line != header)
    yc = crime.mapPartitions(lambda x: reader(x)).map(lambda x: x[19]).map(lambda x:find_yc(x))
    yc_clean=yc.map(lambda x: x[0]+' '+x[1]+' '+x[2]+' '+x[3])
    yc_clean.saveAsTextFile('crime_yc.out')
    sc.stop()