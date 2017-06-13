#!/bin/bash

# save current directory
MY_CWD=$(pwd)

# create staging directory
mkdir ~/staging
mkdir ~/staging/exercise_1
cd ~/staging/exercise_1

# get file from remote source and unzip
wget https://data.medicare.gov/views/bg9k-emty/files/0a9879e0-3312-4719-a1db-39fd114890f1?filename=Hospital_Revised_Flatfiles.zip -O Hospital_Revised_Flatfiles.zip
unzip Hospital_Revised_Flatfiles.zip -d Hospital_Revised_Flatfiles

# remove the column labels (the first line) from each file and rename
tail -n +2 "Hospital_Revised_Flatfiles/Hospital General Information.csv" > hospitals.csv
tail -n +2 "Hospital_Revised_Flatfiles/Timely and Effective Care - Hospital.csv" > effective_care.csv
tail -n +2 "Hospital_Revised_Flatfiles/Readmissions and Deaths - Hospital.csv" > readmissions.csv
tail -n +2 "Hospital_Revised_Flatfiles/Measure Dates.csv" >	Measures.csv
tail -n +2 "Hospital_Revised_Flatfiles/hvbp_hcahps_11_10_2016.csv" > surveys_responses.csv

# create the root directory and each subdirectory in HSFS
hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -mkdir /user/w205/hospital_compare/hospitals
hdfs dfs -mkdir /user/w205/hospital_compare/effective_care
hdfs dfs -mkdir /user/w205/hospital_compare/readmissions
hdfs dfs -mkdir /user/w205/hospital_compare/Measures
hdfs dfs -mkdir /user/w205/hospital_compare/surveys_responses

# copy each of the csv files into HDFS
hdfs dfs -put hospitals.csv /user/w205/hospital_compare/hospitals
hdfs dfs -put effective_care.csv /user/w205/hospital_compare/effective_care
hdfs dfs -put readmissions.csv /user/w205/hospital_compare/readmissions
hdfs dfs -put Measures.csv /user/w205/hospital_compare/Measures
hdfs dfs -put surveys_responses.csv /user/w205/hospital_compare/surveys_responses

# change directory back to original
cd $MY_CWD