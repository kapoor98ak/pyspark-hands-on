# Problem link: https://platform.stratascratch.com/coding/10322-finding-user-purchases?code_type=6
# Finding User Purchases

# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Start writing code
df = amazon_transactions

# Order data by user_id and created_at
df = df.orderBy(df.user_id, df.created_at)

# Define a window specification for each user ordered by the created_at column
windowSpec = Window.partitionBy(df.user_id).orderBy(df.created_at)

# Calculate the next purchase date and the difference in days
df = df.withColumn('next_purchase_date', F.lead(df.created_at).over(windowSpec)) \
       .withColumn('date_diff', F.datediff(F.col('next_purchase_date'), F.col('created_at')))

# Select relevant columns and filter for non-null next purchase dates
df = df.select('id', 'user_id', 'created_at', 'next_purchase_date', 'date_diff') \
       .filter(df.next_purchase_date.isNotNull())

# Filter users who made a purchase within 7 days and get distinct user IDs
df = df.filter(df.date_diff <= 7).select('user_id').distinct()

df.show()

amazon_transactions = df

# To validate your solution, convert your final PySpark DataFrame to a Pandas DataFrame
amazon_transactions.toPandas()
