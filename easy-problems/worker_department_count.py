# Problem Link: https://platform.stratascratch.com/coding/9847-find-the-number-of-workers-by-department?code_type=6
# This code finds the number of workers in each department who started in April or later.

# Import necessary libraries
import pyspark
import pyspark.sql.functions as F

# Assuming `worker` is the DataFrame containing worker data.

# Filter workers who joined in April or later
april_df = worker.filter(F.month(F.to_date(worker['joining_date'])) >= 4)

# Group by department and count distinct worker IDs, order by the number of workers in descending order
df = april_df.groupBy(april_df['department']).agg(F.countDistinct(april_df['worker_id']).alias('num_workers')).orderBy(F.col('num_workers').desc())

# Update the worker DataFrame with the resulting DataFrame
worker = df

# Optionally, convert the resulting DataFrame to pandas for validation or further analysis
worker.toPandas()
