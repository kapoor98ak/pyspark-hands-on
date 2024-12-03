# Problem Link: https://platform.stratascratch.com/coding/9622-number-of-bathrooms-and-bedrooms?code_type=6

# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Start writing code
# Assuming 'airbnb_search_details' dataframe is already loaded

# Define window specification: partition by 'city' and 'property_type'
windowSpec = Window.partitionBy(df.city, df.property_type)

# Calculate average number of bathrooms and bedrooms for each group
df = df.withColumn('n_bathrooms_avg', F.round(F.avg(df.bathrooms).over(windowSpec), 2)) \
       .withColumn('n_bedrooms_avg', F.round(F.avg(df.bedrooms).over(windowSpec), 2)) \
       .select('city', 'property_type', 'n_bedrooms_avg', 'n_bathrooms_avg') \
       .distinct()  # Get distinct rows based on city and property type

df.show()  # Show the resulting dataframe

# Assign the result back to the 'airbnb_search_details' variable
airbnb_search_details = df

# To validate your solution, convert your final pySpark df to a pandas df
airbnb_search_details.toPandas()
