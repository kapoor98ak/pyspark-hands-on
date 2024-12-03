# Problem Link: https://platform.stratascratch.com/coding/2119-most-lucrative-products?code_type=6

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Start writing code
# Assuming 'online_orders' dataframe is already loaded

# Filter data for the year 2022 and the first half of the year (Jan to Jun)
df = online_orders
df = df.filter((F.year(df.date) == 2022) & (F.month(df.date).isin([1, 2, 3, 4, 5, 6])))

# Group by 'product_id' and calculate the total revenue by multiplying units_sold by cost_in_dollars, rounded to 0 decimal places
df = df.groupBy(df.product_id).agg(F.round(F.sum(df.units_sold * df.cost_in_dollars), 0).alias('total')) \
       .orderBy(F.col('total').desc()).limit(5)  # Get top 5 products by total revenue

df.show()  # Show the resulting dataframe

# Assign the result back to the 'online_orders' variable
online_orders = df

# To validate your solution, convert your final pySpark df to a pandas df
online_orders.toPandas()
