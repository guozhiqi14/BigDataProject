from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re


sc = SparkContext()

### Column Latitude
def find_xc(x):
    if x=='':
        return (x,'float','X_cordinate','Null')
    try:
        x_c=float(x)
        if float(x)<1067382.508423 and float(x)>913174.999355:
            return (x,'float','X_cordinate','valid')
        else:
            return (x,'float','X_cordinate','invalid')
    except ValueError:
        return (x,'float','X_cordinate','invalid')

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bigram <file>", file=sys.stderr)
        exit(-1)
    crime = sc.textFile(sys.argv[1],1)
    header = crime.first() 
    crime=crime.filter(lambda line: line != header)
    xc = crime.mapPartitions(lambda x: reader(x)).map(lambda x: x[19]).map(lambda x:find_xc(x))
    xc_clean=xc.map(lambda x: x[0]+' '+x[1]+' '+x[2]+' '+x[3])
    xc_clean.saveAsTextFile('crime_xc.out')
    sc.stop()
