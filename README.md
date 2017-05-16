# BigDataProject
___
## Term Project for Big Data Course at NYU Center for Data Science
### Yanli Zhou, Liwei Song, Zhiqi Guo
___

Data Used:NYPD Complaint Data Historic

The dataset used for this project can be downloaded from the NYC Open Data at https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i.

## PartI

#### Column data issue output instruction
#For outputof column 15-23.   
To get output of column 15-23 , please open output_script_liwei_15_23.
```python
ADDR_PCT_CD-crime_addr.py   
LOC_OF_OCCUR_DESC-crime_loc.py   
PREM_TYP_DESC-crime_pt.py   
PARKS_NM-crime_park.py   
HADEVELOPT-crime_hd.py   
X_COORD_CD-crime_xc.py   
Y_COORD_CD-crime_yc.py   
Latitude-crime_latitude.py   
Longitude-crime_long.py    
Lat_Lon-crime_ll.py   
```

Enter   
```python
spark-submit *.py NYPD_Complaint_Data_Historic in dumbo
Then use hadoop fs -getmerge *.out *out   
```
For example: to get output of Y_COORD_CD-crime_yc.py   
We could use: 
```python
spark-submit crime_yc.py NYPD_Complaint_Data_Historic   
```
After pyspark successfully finsh the task, enter the following command: 
```python
hadoop fs -getmerge crime_yc.out crime_yc.out    
```

Then, we could use check.py to check the number of valid/missing/invalid numbers of column output.   
For example for crime_yc.out: 
```python
spark-submit check.py crime_yc.out     
hadoop fs -getmerge check.out check.out   
```

To check semantic type of column output,we could use check_semantic.py:   
For example:
```python
spark-submit check_semantic.py crime_yc.out     
hadoop fs -getmerge check.out check.out
```

####
Map Reduce Output for plots preparation
In order to make explorary plot of some columns, we also write some scripts to process different column.
Open mr_final
###For column 16-18:
PREM_TYP_DESC-crime_16.py output:crime_16.out
PARKS_NM-crime_17.py output:crime_17.out
HADEVELOPT-crime_18.py output:crime_18.out   

To run code:   
```python
spark-submit crime_##.py    
hadoop fs -getmerge crime_##.out crime_##.out
```

<br>
To get data column labels(basetype, semantic type and validility) for column 1-15, you should run labelscript.py as follow:   

```python
spark-submit labelscript.py NYPD_Complaint_Data_Historic.csv 
hadoop fs -getmerge [column-name]_output.out [column-name]_output.out 
```
All needed data/output for plotting data summary could be find by running **dataSummary1-9.py**


## PartII   
In part II, the team did analysis bewween crime rate and subway station location. Essentially, the team is trying to find whether the closer to the subway station, the higher crime rate. The team calculate the Euclidean Distance between subway's location and each crime listing. And draw two circles with the same area(one small circle, one donut-shape circle) to calculate the density. The result is statistically significant.   
To join the table of different subway station information and the table of NYC criminal data set. Cartesian join function by PySpark is primarily used. Scircle.py is the file used to perform the joining task and find crimes which occur in inner circle of all subway stations, and 2circle.py is the file used to find the outer circle of all subway stations. 

To run the script and get the statistics for small circle:   

```Python
spark-submit scircle.py NYPD_Complaint_Data_Historic.csv nysub.csv  *.out
hadoop fs -getmerge *.out *out   
```

To run the script and get the statistics for docut-shape circle:   

```Python
spark-submit 2_circle.py NYPD_Complaint_Data_Historic.csv nysub.csv  *.out
hadoop fs -getmerge *.out *out   
```

And subtract those two output we can compare two areas' density. The result is statistically significant. 

**Data Used**:
          NYPD Complaint Data Historic   
          NYC Subway Dataset http://web.mta.info/nyct/service/   
          NNDC Climate Date https://www7.ncdc.noaa.gov/CDO/dataproduct
          Unemployment Statistics data set  https://data.ny.gov/Economic- Development/Local-Area-Unemployment-Statistics-LAUS-Seasonally/dh9m-5v4d/data   
          OASAS-Medicaid data Set https://data.ny.gov/Human-Services/OASAS-Medicaid- Trend-Recipient-Summary-Profile-Beg/g4vm-hyyi/data   
          NYC police preccint data set http://www1.nyc.gov/site/planning/data-maps/open-data/districts-download- metadata.page   
          NYC borough data set https://geo.nyu.edu/?f%5Bdc_format_s%5D%5B%5D=Shapefile&f%5Bdc_subject_sm %5D%5B%5D=Boroughs&f%5Bdct_spatial_sm%5D%5B%5D=New+York+City%2C+N ew+York%2C+United+States   
          NYC population open data set https://data.cityofnewyork.us/City-Government/NYC-Population-by-Borough/h2bk- zmw6/data

