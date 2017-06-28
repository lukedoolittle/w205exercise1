from pyspark.sql.types import *
from pyspark.sql import HiveContext
import decimal

context = HiveContext(sc)

# Get the variance relative to the mean for only those scores that
# Have ratings for more than 1/4 of the providers
# Also we need to filter out any of the HAI scores since they (appear to)
# represent number of days/patients which isn't a relevant parameter
providerCount = context.sql('select count(*) from providers').first()[0]
result = context.sql('select * from (select metric_id, count(*) score_count, var_samp(score) / avg(score) score_variance_mean_relative from scores group by metric_id) a where score_count > {0} and metric_id not like \'HAI%\' order by score_variance_mean_relative desc'.format(providerCount / 4))

result.take(10)