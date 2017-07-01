from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql import HiveContext
import decimal

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
ranges = dict((range['metric_id'], range) 
			   for range 
			   in map(lambda row: row.asDict(), 
								  sqlContext.sql(rangesSql).collect()))

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
(sqlContext.sql(scaledScoresSql)
		   .map(lambda row: (row.provider_id, 
							 adjust(row.score, row.metric_id)))
		   .toDF()
		   .selectExpr("_1 as provider_id", 
					   "_2 as scaled_score")
		   .registerTempTable('scaled_scores'))