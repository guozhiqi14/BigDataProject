from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

sc = SparkContext()

def outlier_date(CMPLNT_FR_DT,CMPLNT_TO_DT):
    flag = ""
	#CMPLNT_FR_DT = datetime.strptime(CMPLNT_FR_DT,'%m/%d/%Y')
	#CMPLNT_TO_DT = datetime.strptime(CMPLNT_TO_DT,'%m/%d/%Y')

    try:
        if CMPLNT_FR_DT != "" and CMPLNT_TO_DT != "" :
            if datetime.strptime(CMPLNT_FR_DT,'%m/%d/%Y') > datetime.strptime(CMPLNT_TO_DT,'%m/%d/%Y'):
                flag = 'Invalid'
                return (flag,1)
            else:
                flag = 'valid'
                return (flag,1)
        else:
            return ('',1)
    except:
        return ("",1)

header = crimedata.first() #header
invalid_date = crimedata.filter(lambda line: line != header)
#invalid_date = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: (outlier_date(x[1],x[3])[0],(x[1],x[3])))
#invalid_date.groupByKey().coalesce(1).collect()

invalid_date_count = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: (outlier_date(x[1],x[3])))
invalid_date_count.reduceByKey(add).coalesce(1).saveAsTextFile('invalid_date_count')
