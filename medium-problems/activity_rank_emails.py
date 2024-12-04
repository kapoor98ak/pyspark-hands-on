# Activity Rank
# https://platform.stratascratch.com/coding/10351-activity-rank?code_type=6

# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Start writing code

# Start with the google_gmail_emails dataset
df = google_gmail_emails

# Group by the sender ('from_user') and count the number of emails sent by each user
df = df.groupBy(df.from_user).agg(F.count(df.id).alias('emails_sent'))

# Create a ranking column ('rank') based on the number of emails sent, ordered in descending order
# If two users have the same count, they are ranked alphabetically by 'from_user'
df = df.withColumn(
    'rank', 
    F.row_number().over(Window.partitionBy(F.lit('1')).orderBy(F.col('emails_sent').desc(), F.col('from_user')))
)

# Show the resulting DataFrame with ranks
df.show()

# Assign the resulting DataFrame back to google_gmail_emails for validation
google_gmail_emails = df

# To validate your solution, convert your final PySpark DataFrame to a pandas DataFrame
google_gmail_emails.toPandas()
