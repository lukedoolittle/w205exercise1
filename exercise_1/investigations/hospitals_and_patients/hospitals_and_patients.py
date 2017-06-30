from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql import HiveContext
import decimal

# Get the range of scores for each applicable survey metric and convert into a dictionary
rangesSql = 'select max(nurse_communication) max_nurse_communication, min(nurse_communication) min_nurse_communication, max(doctor_communication) max_doctor_communication, min(doctor_communication) min_doctor_communication, max(staff_responsiveness) max_staff_responsiveness, min(staff_responsiveness) min_staff_responsiveness, max(pain_management) max_pain_management, min(pain_management) min_pain_management, max(medicine_communication) max_medicine_communication, min(medicine_communication) min_medicine_communication, max(hospital_enviroment) max_hospital_enviroment, min(hospital_enviroment) min_hospital_enviroment, max(discharge_information) max_discharge_information, min(discharge_information) min_discharge_information, max(overall_rating) max_overall_rating, min(overall_rating) min_overall_rating from surveys'
ranges = context.sql(rangesSql).first().asDict()

# Normalize the score UNLESS the score does not exist and then return the minimum
adjust = lambda score, max, min: (score - min)/(max - min) if score is not None else min

# An adjustment to the score: normalize the score over [0,1]
surveys = context.sql('select * from surveys').map(lambda row: (row.provider_id, adjust(row.nurse_communication, ranges['max_nurse_communication'], ranges['min_nurse_communication']) + adjust(row.staff_responsiveness, ranges['max_staff_responsiveness'], ranges['min_staff_responsiveness']) + adjust(row.doctor_communication, ranges['max_doctor_communication'], ranges['min_doctor_communication']) + adjust(row.pain_management, ranges['max_pain_management'], ranges['min_pain_management']) + adjust(row.medicine_communication, ranges['max_medicine_communication'], ranges['min_medicine_communication']) + adjust(row.hospital_enviroment, ranges['max_hospital_enviroment'], ranges['min_hospital_enviroment']) + adjust(row.discharge_information, ranges['max_discharge_information'], ranges['min_discharge_information']))).toDF().selectExpr("_1 as provider_id", "_2 as scaled_survey_score")

scores = context.sql('select provider_id, avg(scaled_score) mean_scaled_score from intermediate_table group by provider_id')

# Join the two tables and look at the top 10
result = scores.join(surveys, scores.provider_id == surveys.provider_id).select(scores.provider_id, scores.mean_scaled_score, surveys.scaled_survey_score).stat.corr("mean_scaled_score", "scaled_survey_score")
result