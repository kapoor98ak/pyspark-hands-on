# Problem Link: https://platform.stratascratch.com/coding/10353-workers-with-the-highest-salaries?code_type=6

# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Start writing code
# Assuming 'worker' and 'title' dataframes are already loaded

# Create a window specification to order by salary in descending order
windowSpec = Window.partitionBy(F.lit('1')).orderBy(df.salary.desc())

# Join 'worker' dataframe with 'title' dataframe on worker ID and filter the rows where salary is the highest
df = worker.join(title, worker.worker_id == title.worker_ref_id) \
    .filter(F.col('salary') == df.select(F.max('salary')).first()[0]) \
    .select(F.col('worker_title').alias('best_paid_title'))

# Show the result
df.show()

# Assign the resulting dataframe back to 'worker'
worker = df

# To validate your solution, convert your final pySpark df to a pandas df
worker.toPandas()
