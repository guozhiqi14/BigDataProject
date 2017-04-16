from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


sc = SparkContext()

### Column Latitude
def find_lat(x):
    if x==None:
        return ('NA','float','Latitude','Null')
    try:
        lat=float(x)
        if float(lat)<40.915568 and float(x)>40.495992:
            return (x,'float','Latitude','valid')
        else:
            return (x,'float','Latitude','invalid')
    except ValueError:
        return ('Not float','float','Latitude','invalid')

def latitude_output():
	crime = sc.textFile('NYPD_Complaint_Data_Historic.csv',1)
	header = crime.first() 
	crime=crime.filter(lambda line: line != header)
	lat = crime.mapPartitions(lambda x: reader(x)).map(lambda x: x[21])
	lat_clean=lat.map(lambda x: find_lat(x)).map(lambda x: x[0]+' '+x[1]+' '+x[2]+' '+x[3])
	lat_clean.saveAsTextFile('Latitude.out')

### Column Longitude
def find_long(x):
    if x==None:
        return ('NA','float','Longitude','Null')
    try:
        long=float(x)
        if float(long)<40.915568 and float(long)>40.495992:
            return (x,'float','Longitude','valid')
        else:
            return (x,'float','Longitude','invalid')
    except ValueError:
        return ('Not float','float','Longitude','invalid')

def longitude_output():
	crime = sc.textFile('NYPD_Complaint_Data_Historic.csv',1)
	header = crime.first() 
	crime=crime.filter(lambda line: line != header)
	long = crime.mapPartitions(lambda x: reader(x)).map(lambda x: x[21])
	long_clean=lat.map(lambda x: find_long(x)).map(lambda x: x[0]+' '+x[1]+' '+x[2]+' '+x[3])
	long_clean.saveAsTextFile('Longitude.out')