# Problem Link: https://platform.stratascratch.com/coding/9653-count-the-number-of-user-events-performed-by-macbookpro-users?code_type=6

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Start writing code
# Assuming 'playbook_events' dataframe is already loaded

# Filter the dataframe for MacBook Pro users and group by 'event_name'
df = playbook_events

# Filter events where the device is 'macbook pro' and group by event names
df = df.filter(df['device'] == 'macbook pro') \
       .groupBy(df['event_name']) \
       .agg(F.count(df['user_id']).alias('event_count')) \
       .orderBy(F.col('event_count').desc())  # Order by the count of events in descending order

df.show()  # Show the resulting dataframe

# Assign the result back to the 'playbook_events' variable
playbook_events = df

# To validate your solution, convert your final pySpark df to a pandas df
playbook_events.toPandas()

