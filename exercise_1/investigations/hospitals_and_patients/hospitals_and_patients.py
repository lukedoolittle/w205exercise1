from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import decimal

# Generate the scaled scores table
sqlContext = HiveContext(SparkContext(conf=SparkConf().setAppName("investigations")))
high = decimal.Decimal('1.0')
medium = decimal.Decimal('0.5')
low = decimal.Decimal('0.1')

# Create a dictionary that contains the user defined weights for each applicable measure
weights = dict([('READM_30_HOSP_WIDE', high), 
				('MORT_30_PN', high), 
				('MORT_30_HF', high), 
				('MORT_30_COPD', high), 
				('OP_22', high),
				('OP_25', high), 
				('READM_30_PN', medium), 
				('READM_30_COPD', medium), 
				('READM_30_HF', medium), 
				('IMM_2', medium), 
				('OP_17', medium), 
				('OP_12', medium), 
				('OP_10', low), 
				('OP_11', low), 
				('OP_9', low), 
				('ED_1b', low), 
				('ED_2b', low), 
				('OP_18b', low), 
				('OP_20', low), 
				('OP_21', low)])

# Get the range of scores for each applicable metric and convert into a dictionary
rangesSql = 'select metric_id, max(score) maximum, min(score) minimum from scores group by metric_id'
ranges = dict((range['metric_id'], range) for range in map(lambda row: row.asDict(), sqlContext.sql(rangesSql).collect()))

# An adjustment to the score: first we normalize the score over [0,1] and then
# we apply the pre-chosen weight for each metric
adjust = lambda score, metric: ((score - ranges[metric]['minimum'])
							    / (ranges[metric]['maximum'] - ranges[metric]['minimum'])
							    * weights[metric])

# Perform a map-reduce over all pre-selected scores to get the average of available
# scores. If the provider has scores for less than half of the selected metrics then
# do not include them in the ratings

# Generate a temp table with a map over the provider and scaled score, all the rows from the scores table where the metric is one of the chosen measures
scaledScoresSql = ('select * from scores where score is not null and metric_id in ({0})'
				   .format(','.join(['\'' + member + '\'' 
									for member 
									in weights.keys()])))
(sqlContext.sql(intermediateSql)
		   .map(lambda row: (row.provider_id, 
							 adjust(row.score, row.metric_id)))
		   .toDF()
		   .selectExpr("_1 as provider_id", 
					   "_2 as scaled_score")
		   .registerTempTable('scaled_scores'))

surveyMetrics = ['nurse_communication', 
				 'doctor_communication', 
				 'staff_responsiveness', 
				 'pain_management',
				 'medicine_communication',
				 'hospital_enviroment', 
				 'discharge_information', 
				 'overall_rating']

# Get the range of scores for each applicable survey metric and convert into a dictionary
rangesSql = ('select {0} from surveys'
			.format(', '.join(['max({0}) max_{0}, min({0}) min_{0}'.format(item) 
							  for item 
							  in surveyMetrics])))
ranges = (sqlContext.sql(rangesSql)
				    .first()
				    .asDict())

# Normalize the score UNLESS the score does not exist and then return the minimum
adjust = lambda score, metric: ((score - ranges['min_' + metric])
								/ (ranges['max_' + metric] - ranges['min_' + metric]) 
								if score is not None 
								else ranges['min_' + metric])

# An adjustment to the score: normalize the score over [0,1]
surveys = (sqlContext.sql('select * from surveys')
				  .map(lambda row: (row.provider_id, 
									sum([adjust(getattr(row, metric), metric) 
										for metric 
										in surveyMetrics])))
				  .toDF()
				  .selectExpr("_1 as provider_id", 
							  "_2 as scaled_survey_score"))

scores = sqlContext.sql('select provider_id, avg(scaled_score) mean_scaled_score from scaled_scores group by provider_id')

# Join the two tables and get the correlation between survey score and rating
result = (scores.join(surveys, scores.provider_id == surveys.provider_id)
				.select(scores.provider_id, 
						scores.mean_scaled_score, 
						surveys.scaled_survey_score)
				.stat
				.corr("mean_scaled_score", 
					  "scaled_survey_score"))
result