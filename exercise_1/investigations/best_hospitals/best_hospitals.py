from pyspark.sql.types import *
from pyspark.sql import HiveContext
import decimal

context = HiveContext(sc)

# Create a dictionary that contains the user defined weights for each applicable measure
weights = dict([('READM_30_HOSP_WIDE', decimal.Decimal(1.0)), ('MORT_30_PN', decimal.Decimal(1.0)), ('MORT_30_HF', decimal.Decimal(1.0)), ('MORT_30_COPD', decimal.Decimal(1.0)), ('OP_22', decimal.Decimal(1.0)), ('OP_25', decimal.Decimal(1.0)), ('READM_30_PN', decimal.Decimal(0.5)), ('READM_30_COPD', decimal.Decimal(0.5)), ('READM_30_HF', decimal.Decimal(0.5)), ('IMM_2', decimal.Decimal(0.5)), ('OP_17', decimal.Decimal(0.5)), ('OP_12', decimal.Decimal(0.5)), ('OP_10', decimal.Decimal(0.1)), ('OP_11', decimal.Decimal(0.1)), ('OP_9', decimal.Decimal(0.1)), ('ED_1b', decimal.Decimal(0.1)), ('ED_2b', decimal.Decimal(0.1)), ('OP_18b', decimal.Decimal(0.1)), ('OP_20', decimal.Decimal(0.1)), ('OP_21', decimal.Decimal(0.1))])

# Get the range of scores for each applicable metric and convert into a dictionary
temp = context.sql('select metric_id, max(score) maximum, min(score) minimum from scores group by metric_id')
ranges = {range['metric_id']: range for range in map(lambda row: row.asDict(), temp.collect())}

# An adjustment to the score: first we normalize the score over [0,1] and then
# we apply the pre-chosen weight for each metric
adjust = lambda score, metric: (score - ranges[metric]['minimum'])/(ranges[metric]['maximum'] - ranges[metric]['minimum']) * weights[metric]

# Perform a map-reduce over all pre-selected scores to get the average of available
# scores. If the provider has scores for less than half of the selected metrics then
# do not include them in the ratings
minimumScoreCount = .5 * len(weights)

# Generate a temp table with a map over the provider and scaled score, all the rows from the scores table where the metric is one of the chosen measures
sqlStatement = 'select * from scores where score is not null and metric_id in ({0})'.format(','.join(['\'' + member + '\'' for member in weights.keys()]))
context.sql(sqlStatement).map(lambda row: (row.provider_id, adjust(row.score, row.metric_id))).toDF().selectExpr("_1 as provider_id", "_2 as scaled_score").registerTempTable('intermediate_table')

# Utilize the temporary table to create the average and variance of each scaled score
result = context.sql('select hospital_name, overall_score, mean_scaled_score, variance_scaled_score, sum_scaled_score from (select provider_id, avg(scaled_score) mean_scaled_score, sum(scaled_score) sum_scaled_score, var_samp(scaled_score) variance_scaled_score, count(*) score_count from intermediate_table group by provider_id) a join providers on a.provider_id = providers.provider_id where score_count > {0} order by mean_scaled_score desc'.format(minimumScoreCount))
result.show(10)