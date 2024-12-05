# Problem link: https://platform.stratascratch.com/coding/10318-new-products?code_type=6
# New Products

# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# Start writing code
df = car_launches

# Group data by company_name and pivot by year, counting the launches
df = car_launches.groupBy('company_name').pivot('year').count()

# Calculate the net products launched (difference between 2020 and 2019)
df = df.withColumn('net_products', F.col('2020') - F.col('2019')).select('company_name', 'net_products')

df.show()

car_launches = df

# To validate your solution, convert your final PySpark DataFrame to a Pandas DataFrame
car_launches.toPandas()
