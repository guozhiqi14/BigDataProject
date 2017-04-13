import sys
from pyspark import SparkContext
from csv import reader
from operator import add
from datetime import datetime
import re

'''
CMPLNT_FR_DT_label
'''
def CMPLNT_FR_DT_label(time):
	validity = None 
	base_type = 'DATETIME'
	semantic_type = 'Date of Occurrence'

	year = time[-4:]
	if year.isdigit():
		year = int(year)
	else:
		pass

	if time != "" and year in range(1900,2018):
		
		validity = "Valid"
		return (base_type,semantic_type,validity)
	else:
		validity = "Null"
		base_type = "Null"
		semantic_type = "Null"
		return (base_type,semantic_type,validity)

	#return (base_type,semantic_type,validity)

CMPLNT_FR_DT = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: (x[1],CMPLNT_FR_DT_label(x[1])))
CMPLNT_FR_DT.saveAsTextFile("output.out")



'''
CMPLNT_FR_TM_label
'''
def CMPLNT_FR_TM_label(time):
	validity = None 
	base_type = 'DATETIME'
	semantic_type = 'Time of occurrence'
	hour = time[:2] 

	if hour.isdigit():
		hour = int(hour)
	else:
		pass
	if time != "" and hour in range(0,25):
		
		validity = "Valid"
		return (base_type,semantic_type,validity)
	else:
		validity = "Null"
		base_type = "Null"
		semantic_type = "Null"
		return (base_type,semantic_type,validity)

CMPLNT_FR_TM = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: (x[2],CMPLNT_FR_DT_label(x[2])))
CMPLNT_FR_TM.saveAsTextFile("output.out")


'''
CMPLNT_TO_DT_label
'''
def CMPLNT_TO_DT_label(time):
	validity = None 
	base_type = 'DATETIME'
	semantic_type = 'Ending Date of Occurrence'

	year = time[-4:]
	if year.isdigit():
		year = int(year)
	else:
		pass

	if time != "" and year in range(1900,2018):
		validity = "Valid"
		return (base_type,semantic_type,validity)
	elif time == "":
		validity = "Null"
		base_type = "Null"
		semantic_type = "Null"
		return (base_type,semantic_type,validity) 
	else:
		validity = "Invalid"
		base_type = "Null"
		semantic_type = "Null"
		return (base_type,semantic_type,validity)

CMPLNT_TO_DT = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: (x[3],CMPLNT_TO_DT_label(x[3])))
CMPLNT_TO_DT.saveAsTextFile("output.out")


'''
CMPLNT_TO_TM_label
'''
def CMPLNT_TO_TM_label(time):
	validity = None 
	base_type = 'DATETIME'
	semantic_type = 'Ending Time of Occurrence'
	hour = time[:2] 

	if hour.isdigit():
		hour = int(hour)
	else:
		pass
	if time != "" and hour in range(0,25):
		
		validity = "Valid"
		return (base_type,semantic_type,validity)

	elif time == "":
		validity = "Null"
		base_type = "Null"
		semantic_type = "Null" 
		return (base_type,semantic_type,validity)
	else:
		validity = "Invalid"
		base_type = "Null"
		semantic_type = "Null"
		return (base_type,semantic_type,validity)


CMPLNT_TO_TM = crimedata.mapPartitions(lambda x: reader(x)).map(lambda x: (x[4],CMPLNT_TO_TM_label(x[4])))
CMPLNT_TO_TM.saveAsTextFile("output.out")


'''
RPT_DT_label
'''
def RPT_DT_label(time):
	validity = None 
	base_type = 'DATETIME'
	semantic_type = 'Date event was reported to police '

	if time == "":
		validity = "Null"
		base_type = "Null"
		semantic_type = "Null"
		return (base_type,semantic_type,validity)
	elif re.match("(\d{2})[/](\d{2})[/](\d{4})$",time) != None:
		month  = int(time.split('/')[0])
		date   = int(time.split('/')[1])
		year   = int(time.split('/')[2])

		if month in range(1,13) and date in range(1,32) and year in range(1900,2018):
			validity = "Valid"
			return (base_type,semantic_type,validity)
		else:
			validity = "Invalid"
			return (base_type,semantic_type,validity)
	else:
		validity = "Invalid"
		return (base_type,semantic_type,validity)

header = crimedata.first() #header
RPT_DT=crimedata.filter(lambda line: line != header)
RPT_DT = RPT_DT.mapPartitions(lambda x: reader(x)).map(lambda x: (x[5],CMPLNT_TO_TM_label(x[5])))
RPT_DT.saveAsTextFile("output.out")


'''
KY_CD_Label
'''
def KY_CD_Label(code):
	validity = None 
	base_type = 'INT'
	semantic_type = 'Offense Classification Code'

	if code == "" or len(code) == 0:
		validity = 'Null'
		base_type = 'Null'
		semantic_type = 'Null'
		return (base_type,semantic_type,validity)

	elif len(code) > 3:
		validity = 'Invalid'
		base_type = 'INT'
		semantic_type = 'Offense Classification Code'
		return (base_type,semantic_type,validity)

	else:
		validity = "Valid"
		return (base_type,semantic_type,validity)

header = crimedata.first() #header
KY_CD=crimedata.filter(lambda line: line != header)
KY_CD = KY_CD.mapPartitions(lambda x: reader(x)).map(lambda x: (x[6],KY_CD_Label(x[6])))
KY_CD.saveAsTextFile("output.out")
