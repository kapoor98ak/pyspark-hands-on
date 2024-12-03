# Problem Link: https://platform.stratascratch.com/coding/9845-find-the-number-of-employees-working-in-the-admin-department?code_type=6
# This code counts the number of employees in the "Admin" department who joined in April or later.

# Import necessary libraries
import pyspark.sql.functions as F
from pyspark.sql.types import DateType

# Assuming `worker` is the DataFrame containing worker data.

# Convert the 'joining_date' column to a date type
worker = worker.withColumn("joining_date", F.to_date(worker["joining_date"]))

# Extract the month from the 'joining_date' column
worker = worker.withColumn("month", F.month(worker["joining_date"]))

# Filter for workers who joined in April or later
april_df = worker.filter(worker["month"] >= 4)

# Filter further to select employees in the 'Admin' department and count them
result = april_df.filter(april_df["department"] == "Admin").count()

# Output the result
result
