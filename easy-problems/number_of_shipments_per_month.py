# Problem Link: https://platform.stratascratch.com/coding/2056-number-of-shipments-per-month?code_type=6

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Start writing code
# Assuming 'amazon_shipment' dataframe is already loaded

# Add a new column 'year_month' to the dataframe, which formats the 'shipment_date' as 'yyyy-MM'
df = amazon_shipment
df = df.withColumn('year_month', F.date_format(F.col('shipment_date'), 'yyyy-MM'))

# Assign the resulting dataframe back to 'amazon_shipment'
amazon_shipment = df

# To validate your solution, convert your final pySpark df to a pandas df
amazon_shipment.toPandas()
