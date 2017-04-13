import sys
from pyspark import SparkContext
from csv import reader
from operator import add


crimeline = sc.textFile("/Users/guozhiqi-seven/Google Drive/NYU Master/Big Data/project/NYPD_Complaint_Data_Historic.csv")
header = crimeline.first() #header
crimeline=crimeline.filter(lambda line: line != header)
crimeline = crimeline.mapPartitions(lambda x: reader(x)).map(lambda x: (x[0],1))
crimeline = crimeline.map(lambda x: ('NotNaN',1) if x[0] != None else ('NaN',1))



'''
Count null value in each column
'''
crime_df = sqlContext.read.csv("/Users/guozhiqi-seven/Google Drive/NYU Master/Big Data/project/NYPD_Complaint_Data_Historic.csv",header=True)
from pyspark.sql.functions import col, count, sum

def count_null(c):
    """Use conversion between boolean and integer
    - False -> 0
    - True ->  1
    """
    return sum(col(c).isNull().cast("integer").alias(c))

exprs = [count_null(c) for c in crime_df.columns]
crime_df.agg(*exprs)

crime_df.agg(*exprs).toPandas()



crimedata = sc.textFile("/Users/guozhiqi-seven/Google Drive/NYU Master/Big Data/project/NYPD_Complaint_Data_Historic.csv")
#crimedata.count() 

CMPLNT_FR_DT_year = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: (x[1][-4:],1))
CMPLNT_FR_DT_year.reduceByKey(add).collect()

CMPLNT_FR_DT_month = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: (x[1][:2],1))
CMPLNT_FR_DT_month.reduceByKey(add).collect()

CMPLNT_FR_DT_date = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: (x[1][3:5],1))
CMPLNT_FR_DT_date.reduceByKey(add).collect()

CMPLNT_FR_TM_hour = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: (x[2][:2],1))
CMPLNT_FR_TM_hour.reduceByKey(add).collect()

CMPLNT_FR_TM_min = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: (x[2][3:5],1))
CMPLNT_FR_TM_min.reduceByKey(add).collect()


offense_classification = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: ((x[6],x[7]),1))
sorted(offense_classification.reduceByKey(add).collect(),key=lambda x:x[1],reverse=True)

offense_classification = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: ((x[6],x[7]),1))
sorted(offense_classification.reduceByKey(add).collect(),key=lambda x:x[0][0],reverse=True)

offense_classification = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: ((x[6],x[7]),1))
sorted(offense_classification.reduceByKey(add).collect(),key=lambda x:x[0][1],reverse=True)




