import sys
from pyspark import SparkContext
from csv import reader
from operator import add
from datetime import datetime
import re


crimedata = sc.textFile(sys.argv[1], 1)
#crimedata = sc.textFile("/Users/guozhiqi-seven/Google Drive/NYU Master/Big Data/project/NYPD_Complaint_Data_Historic.csv")


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
CMPLNT_FR_DT.saveAsTextFile("CMPLNT_FR_DT_output.out")



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
CMPLNT_FR_TM.saveAsTextFile("CMPLNT_FR_TM_output.out")


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
CMPLNT_TO_DT.saveAsTextFile("CMPLNT_TO_DT_output.out")


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
CMPLNT_TO_TM.saveAsTextFile("CMPLNT_TO_TM_output.out")


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
RPT_DT.saveAsTextFile("RPT_DT_output.out")


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
KY_CD.saveAsTextFile("KY_CD_output.out")



'''
OFNS_DESC_label
'''
def OFNS_DESC_label(text):
	validity = None 
	base_type = 'TEXT'
	semantic_type = 'Description of Offense'

	if len(text) == 0 or text == "":
		validity = 'Null' 
		base_type = 'Null'
		semantic_type = 'Null'
		return (base_type,semantic_type,validity)

	else:
		validity = 'Valid' 
		base_type = 'TEXT'
		semantic_type = 'Description of Offense'
		return (base_type,semantic_type,validity)


header = crimedata.first() #header
OFNS_DESC=crimedata.filter(lambda line: line != header)
OFNS_DESC = OFNS_DESC.mapPartitions(lambda x: reader(x)).map(lambda x: (x[7],OFNS_DESC_label(x[7])))
OFNS_DESC.saveAsTextFile("OFNS_DESC_output.out")



'''
PD_CD_Label
'''
def PD_CD_Label(code):
	validity = None 
	base_type = 'INT'
	semantic_type = 'Internal Classification Code'

	if code == "":
		validity = 'Null'
		base_type = 'Null'
		semantic_type = 'Null'
		return (base_type,semantic_type,validity)

	else:
		code = int(code)
		if code not in range(1000):
			validity = 'Invalid'
			base_type = 'INT'
			semantic_type = 'Offense Classification Code'
			return (base_type,semantic_type,validity)
		else:
			validity = 'Valid'
			return (base_type,semantic_type,validity)
	

header = crimedata.first() #header
PD_CD=crimedata.filter(lambda line: line != header)
PD_CD = PD_CD.mapPartitions(lambda x: reader(x)).map(lambda x: (x[8],PD_CD_Label(x[8])))
PD_CD.saveAsTextFile("PD_CD_output.out")


'''
PD_DESC_label
'''
def PD_DESC_label(text):
	validity = None 
	base_type = 'TEXT'
	semantic_type = 'Description of Internal Classification'

	if len(text) == 0 or text == "":
		validity = 'Null' 
		base_type = 'Null'
		semantic_type = 'Null'
		return (base_type,semantic_type,validity)

	else:
		validity = 'Valid' 
		base_type = 'TEXT'
		semantic_type = 'Description of Offense'
		return (base_type,semantic_type,validity)


header = crimedata.first() #header
PD_DESC = crimedata.filter(lambda line: line != header)
PD_DESC = PD_DESC.mapPartitions(lambda x: reader(x)).map(lambda x: (x[9],PD_DESC_label(x[9])))
PD_DESC.saveAsTextFile("PD_DESC_output.out")



'''
CRM_ATPT_CPTD_CD_label
'''
def CRM_ATPT_CPTD_CD_label(indicator):
	validity = None 
	base_type = 'TEXT'
	semantic_type = 'Indicator of completeness' 

	indicator_type = ['COMPLETED','ATTEMPTED']

	if indicator == "" or len(indicator) == 0:
		validity = 'Null' 
		base_type = 'Null'
		semantic_type = 'Null'
		return (base_type,semantic_type,validity)

	elif indicator in indicator_type:
		validity = 'Valid'
		return (base_type,semantic_type,validity)

	else:
		validity = 'Invalid'
		return (base_type,semantic_type,validity)

header = crimedata.first() #header
CRM_ATPT_CPTD_CD = crimedata.filter(lambda line: line != header)
CRM_ATPT_CPTD_CD = CRM_ATPT_CPTD_CD.mapPartitions(lambda x: reader(x)).map(lambda x: (x[10],CRM_ATPT_CPTD_CD_label(x[10])))
CRM_ATPT_CPTD_CD.saveAsTextFile("CRM_ATPT_CPTD_CD_output.out")


'''
LAW_CAT_CD_label
'''
def LAW_CAT_CD_label(level):
	validity = None 
	base_type = 'TEXT'
	semantic_type = 'Level of Offense' 
	level_of_offense = ['FELONY', 'MISDEMEANOR', 'VIOLATION']

	if level == "" or len(level) == 0:
		validity = 'Null' 
		base_type = 'Null'
		semantic_type = 'Null'
		return (base_type,semantic_type,validity)
	elif level not in level_of_offense:
		validity = 'Invalid'
		return (base_type,semantic_type,validity)
	else:
		validity = 'Valid'
		return (base_type,semantic_type,validity)

header = crimedata.first() #header
LAW_CAT_CD = crimedata.filter(lambda line: line != header)
LAW_CAT_CD = LAW_CAT_CD.mapPartitions(lambda x: reader(x)).map(lambda x: (x[11],LAW_CAT_CD_label(x[11])))
LAW_CAT_CD.saveAsTextFile("LAW_CAT_CD_output.out")


'''
JURIS_DESC_label
'''
def JURIS_DESC_label(descript):
	validity = None 
	base_type = 'TEXT'
	semantic_type = 'Jurisdiction for Incident'

	if len(descript) == 0 or descript == "":
		validity = 'Null' 
		base_type = 'Null'
		semantic_type = 'Null'
		return (base_type,semantic_type,validity)
	else:
		validity = 'Valid'
		return (base_type,semantic_type,validity)

header = crimedata.first() #header
JURIS_DESC = crimedata.filter(lambda line: line != header)
JURIS_DESC = JURIS_DESC.mapPartitions(lambda x: reader(x)).map(lambda x: (x[12],JURIS_DESC_label(x[12])))
JURIS_DESC.saveAsTextFile("JURIS_DESC_output.out")


'''
BORO_NM_label
'''
def BORO_NM_label(borough,precinct):
	
	boro_precinct = {'BRONX':list(range(40,51))+[52], 'QUEENS':list(range(100,116)), \
	'MANHATTAN':[1,5,6,7,9,10,13,17,18,19,20,22,23,24,25,26,28,30,32,33,34],\
	'BROOKLYN':[60,61,62,63,66,67,68,69,70,71,72,73,75,76,77,78,79,81,83,84,88,90,94],\
	'STATEN ISLAND':list(range(120,124))}

	validity = None 
	base_type = 'TEXT'
	semantic_type = 'Borough Where Incident Occurred'
	if precinct != "":
		precinct = int(precinct) 

	if borough == "" or len(borough) == 0:
		validity = 'Null' 
		base_type = 'Null'
		semantic_type = 'Null'
		return (base_type,semantic_type,validity)

	elif precinct not in boro_precinct[borough]:
		validity = 'Invalid'
		return (base_type,semantic_type,validity)
	else:
		validity = 'Valid'
		return (base_type,semantic_type,validity)

header = crimedata.first() #header
BORO_NM = crimedata.filter(lambda line: line != header)
BORO_NM = BORO_NM.mapPartitions(lambda x: reader(x)).map(lambda x: (x[13],BORO_NM_label(x[13],x[14])))
BORO_NM.saveAsTextFile("BORO_NM_output.out")
