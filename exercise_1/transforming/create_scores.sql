DROP TABLE scores;
CREATE TABLE scores AS SELECT 
provider_id,
measure_id parameter_id,
CAST(score AS DECIMAL(4,1)) score,
CAST(lower_estimate AS DECIMAL(4,1)) lower_estimate,
CAST(higher_estimate AS DECIMAL(4,1)) higher_estimate,
CAST(denominator AS DECIMAL(7,0)) sample_size
FROM readmissions
UNION
SELECT 
provider_id,
measure_id parameter_id,
case 
when score like 'Very High%' then 4
when score like 'High%' then 3
when score like 'Medium%' then 2
when score like 'Low%' then 1
when score like 'Not Available' then null
else CAST(score AS DECIMAL(4,1)) end score,
null lower_estimate,
null higher_estimate,
CAST(sample AS DECIMAL(7,0)) sample_size
FROM effective_care;