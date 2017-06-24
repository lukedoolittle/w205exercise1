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

declare -A files = (["hospitals"] = "Hospital General Information.csv")


# remove the column labels (the first line) from each file and rename
tail -n +2 "Hospital_Revised_Flatfiles/Hospital General Information.csv" > hospitals.csv
tail -n +2 "Hospital_Revised_Flatfiles/Timely and Effective Care - Hospital.csv" > effective_care.csv
tail -n +2 "Hospital_Revised_Flatfiles/Readmissions and Deaths - Hospital.csv" > readmissions.csv
tail -n +2 "Hospital_Revised_Flatfiles/Measure Dates.csv" > measures.csv
tail -n +2 "Hospital_Revised_Flatfiles/hvbp_hcahps_11_10_2016.csv" > survey_responses.csv
tail -n +2 "Hospital_Revised_Flatfiles/Complications - Hospital.csv" > complications.csv
tail -n +2 "Hospital_Revised_Flatfiles/Healthcare Associated Infections - Hospital.csv" > infections.csv
tail -n +2 "Hospital_Revised_Flatfiles/Outpatient Imaging Efficiency - Hospital.csv" > imaging.csv
tail -n +2 "Hospital_Revised_Flatfiles/Structural Measures - Hospital.csv" > structural_measures.csv
tail -n +2 "Hospital_Revised_Flatfiles/READMISSION REDUCTION.csv" > readmission_reduction.csv
tail -n +2 "Hospital_Revised_Flatfiles/hvbp_ami_05_28_2015.csv" > heart_attack_scores.csv
tail -n +2 "Hospital_Revised_Flatfiles/hvbp_hai_05_28_2015.csv" > surgery_infections_scores.csv
tail -n +2 "Hospital_Revised_Flatfiles/hvbp_hf_05_28_2015.csv" > heart_failure_scores.csv
tail -n +2 "Hospital_Revised_Flatfiles/hvbp_pn_05_28_2015.csv" > pnumenoia_scores.csv
tail -n +2 "Hospital_Revised_Flatfiles/hvbp_scip_05_28_2015.csv" > surgery_scores.csv
tail -n +2 "Hospital_Revised_Flatfiles/hvbp_outcome_05_18_2015.csv" > outcome_scores.csv

# create the root directory and each subdirectory in HSFS
hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -mkdir /user/w205/hospital_compare/hospitals
hdfs dfs -mkdir /user/w205/hospital_compare/effective_care
hdfs dfs -mkdir /user/w205/hospital_compare/readmissions
hdfs dfs -mkdir /user/w205/hospital_compare/measures
hdfs dfs -mkdir /user/w205/hospital_compare/survey_responses
hdfs dfs -mkdir /user/w205/hospital_compare/complications
hdfs dfs -mkdir /user/w205/hospital_compare/infections
hdfs dfs -mkdir /user/w205/hospital_compare/imaging
hdfs dfs -mkdir /user/w205/hospital_compare/structural_measures
hdfs dfs -mkdir /user/w205/hospital_compare/readmission_reduction
hdfs dfs -mkdir /user/w205/hospital_compare/heart_attack_scores
hdfs dfs -mkdir /user/w205/hospital_compare/surgery_infections_scores
hdfs dfs -mkdir /user/w205/hospital_compare/heart_failure_scores
hdfs dfs -mkdir /user/w205/hospital_compare/pnumenoia_scores
hdfs dfs -mkdir /user/w205/hospital_compare/surgery_scores
hdfs dfs -mkdir /user/w205/hospital_compare/outcome_scores

# copy each of the csv files into HDFS
hdfs dfs -put hospitals.csv /user/w205/hospital_compare/hospitals
hdfs dfs -put effective_care.csv /user/w205/hospital_compare/effective_care
hdfs dfs -put readmissions.csv /user/w205/hospital_compare/readmissions
hdfs dfs -put measures.csv /user/w205/hospital_compare/measures
hdfs dfs -put survey_responses.csv /user/w205/hospital_compare/survey_responses
hdfs dfs -put complications.csv /user/w205/hospital_compare/complications
hdfs dfs -put infections.csv /user/w205/hospital_compare/infections
hdfs dfs -put imaging.csv /user/w205/hospital_compare/imaging
hdfs dfs -put structural_measures.csv /user/w205/hospital_compare/structural_measures
hdfs dfs -put readmission_reduction.csv /user/w205/hospital_compare/readmission_reduction
hdfs dfs -put heart_attack_scores.csv /user/w205/hospital_compare/heart_attack_scores
hdfs dfs -put surgery_infections_scores.csv /user/w205/hospital_compare/surgery_infections_scores
hdfs dfs -put heart_failure_scores.csv /user/w205/hospital_compare/heart_failure_scores
hdfs dfs -put pnumenoia_scores.csv /user/w205/hospital_compare/pnumenoia_scores
hdfs dfs -put surgery_scores.csv /user/w205/hospital_compare/surgery_scores
hdfs dfs -put outcome_scores.csv /user/w205/hospital_compare/outcome_scores

# change directory back to original
cd $MY_CWD