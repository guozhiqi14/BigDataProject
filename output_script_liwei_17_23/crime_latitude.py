from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re


sc = SparkContext()

### Column Latitude
def find_lat(x):
    if x=='':
        return (x,'float','Latitude','Null')
    try:
        lat=float(x)
        if float(lat)<40.915568 and float(x)>40.495992:
            return (x,'float','Latitude','valid')
        else:
            return (x,'float','Latitude','invalid')
    except ValueError:
        return (x,'float','Latitude','invalid')

if __name__ == "__main__":
    crime = sc.textFile('NYPD_Complaint_Data_Historic.csv',1)
    header = crime.first() 
    crime=crime.filter(lambda line: line != header)
    lat = crime.mapPartitions(lambda x: reader(x)).map(lambda x: x[21])
    lat_clean=lat.map(lambda x: find_lat(x)).map(lambda x: x[0]+' '+x[1]+' '+x[2]+' '+x[3])
    lat_clean.saveAsTextFile('crime_latitude.out')
    sc.stop()