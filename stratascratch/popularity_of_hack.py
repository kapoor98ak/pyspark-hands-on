# Problem Link: https://platform.stratascratch.com/coding/10061-popularity-of-hack?code_type=6
# Find the average popularity of Hack per office location based on the survey data.

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Assuming the DataFrames 'facebook_hack_survey' and 'facebook_employees' are pre-loaded with relevant schema:
# 'facebook_hack_survey' contains columns: 'employee_id', 'popularity', and other survey-related fields
# 'facebook_employees' contains columns: 'id', 'location', and other employee-related fields

# Joining 'facebook_hack_survey' with 'facebook_employees' on 'employee_id' and 'id' columns
df = facebook_hack_survey.join(
    facebook_employees,  # Join with the 'facebook_employees' DataFrame
    facebook_hack_survey['employee_id'] == facebook_employees['id']  # Joining on employee ID
).groupBy(
    F.col('location')  # Grouping by office location
).agg(
    F.round(F.avg(F.col('popularity')), 2).alias('popularity')  # Calculating the average popularity rounded to 2 decimal places
)

# Displaying the resultant DataFrame for validation
df.show()

# Overwriting 'facebook_employees' DataFrame with the result containing average popularity per location
facebook_employees = df

# Converting the PySpark DataFrame to a Pandas DataFrame for validation or further processing
facebook_employees.toPandas()
