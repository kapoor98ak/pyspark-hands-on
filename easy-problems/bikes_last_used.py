# Problem Link: https://platform.stratascratch.com/coding/10176-bikes-last-used?code_type=6
# Find the last time each bike was in use. Output the bike number and the timestamp of the last use.

# Import your libraries
import pyspark
from pyspark.sql import Window
import pyspark.sql.functions as F

# Assuming the DataFrame 'dc_bikeshare_q1_2012' is pre-loaded with schema:
# Columns: 'bike_number', 'end_time'

# Grouping by bike number and finding the maximum 'end_time' for each bike
df = dc_bikeshare_q1_2012.groupBy(
    x.bike_number  # Grouping by bike number
).agg(
    F.max('end_time').alias('last_used')  # Calculating the most recent end time for each bike
).select(
    'bike_number', 'last_used'  # Selecting only bike number and the last use timestamp
)

# Displaying the resultant DataFrame for validation
df.show()

# Overwriting 'dc_bikeshare_q1_2012' with the modified DataFrame containing last use timestamps
dc_bikeshare_q1_2012 = df

# Converting the PySpark DataFrame to a Pandas DataFrame for validation or further processing
dc_bikeshare_q1_2012.toPandas()
