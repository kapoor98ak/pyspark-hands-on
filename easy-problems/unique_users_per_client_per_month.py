# Problem Link: https://platform.stratascratch.com/coding/2024-unique-users-per-client-per-month?code_type=6

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Start writing code
# Assuming 'fact_events' dataframe is already loaded

# Add a new column 'month' extracted from the 'time_id' column
df = fact_events
df = df.withColumn('month', F.month(df.time_id))

# Group the dataframe by 'client_id' and 'month', and count the distinct 'user_id' for each group
df = df.groupBy(df['client_id'], df['month']).agg(F.countDistinct('user_id').alias('event_count'))

# Show the resulting dataframe
df.show()

# Assign the resulting dataframe back to 'fact_events'
fact_events = df

# To validate your solution, convert your final pySpark df to a pandas df
fact_events.toPandas()

