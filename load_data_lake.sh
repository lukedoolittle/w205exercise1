#!/bin/bash
wget https://data.medicare.gov/views/bg9k-emty/files/0a9879e0-3312-4719-a1db-39fd114890f1?filename=Hospital_Revised_Flatfiles.zip -O Hospital_Revised_Flatfiles.zip

unzip Hospital_Revised_Flatfiles.zip -d Hospital_Revised_Flatfiles

sed 1d "Hospital_Revised_Flatfiles/Hospital General Information.csv" > hospitals.csv
sed 1d "Hospital_Revised_Flatfiles/Timely and Effective Care - Hospital.csv" > effective_care.csv
sed 1d "Hospital_Revised_Flatfiles/Readmissions and Deaths - Hospital.csv" > readmissions.csv
sed 1d "Hospital_Revised_Flatfiles/Measure Dates.csv" >	Measures.csv
sed 1d "Hospital_Revised_Flatfiles/hvbp_hcahps_11_10_2016.csv" > surveys_responses.csv

hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -mkdir /user/w205/hospital_compare/hospitals
hdfs dfs -mkdir /user/w205/hospital_compare/effective_care
hdfs dfs -mkdir /user/w205/hospital_compare/readmissions
hdfs dfs -mkdir /user/w205/hospital_compare/Measures
hdfs dfs -mkdir /user/w205/hospital_compare/surveys_responses

hdfs dfs -put hospitals.csv /user/w205/hospital_compare/hospitals
hdfs dfs -put effective_care.csv /user/w205/hospital_compare/effective_care
hdfs dfs -put readmissions.csv /user/w205/hospital_compare/readmissions
hdfs dfs -put Measures.csv /user/w205/hospital_compare/Measures
hdfs dfs -put surveys_responses.csv /user/w205/hospital_compare/surveys_responses
