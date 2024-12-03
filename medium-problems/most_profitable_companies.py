# Problem Link: https://platform.stratascratch.com/coding/10354-most-profitable-companies?code_type=6

# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Start writing code
# Assuming 'forbes_global_2010_2014' dataframe is already loaded

# Create a window specification to order by profits in descending order
windowSpec = Window.partitionBy(F.lit('1')).orderBy(df.profits.desc())

# Apply dense rank to rank companies based on profits
df = df.withColumn('new_rank', F.dense_rank().over(windowSpec))

# Order by the rank and filter the top 3 profitable companies
df = df.filter(df.rank <= 3).select('company', 'profits').orderBy(df.new_rank)

# Show the result
df.show()

# Assign the resulting dataframe back to 'forbes_global_2010_2014'
forbes_global_2010_2014 = df

# To validate your solution, convert your final pySpark df to a pandas df
forbes_global_2010_2014.toPandas()
