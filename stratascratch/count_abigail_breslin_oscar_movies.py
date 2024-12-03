# Problem Link: https://platform.stratascratch.com/coding/10128-count-the-number-of-movies-that-abigail-breslin-nominated-for-oscar?code_type=6
# Count the number of distinct movies that Abigail Breslin was nominated for an Oscar.

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Assuming the DataFrame 'oscar_nominees' is pre-loaded with schema:
# Columns: 'nominee', 'movie', and other relevant fields

# Filtering the DataFrame for rows where the nominee is 'Abigail Breslin'
df = oscar_nominees.filter(
    oscar_nominees['nominee'] == 'Abigail Breslin'  # Keep rows for Abigail Breslin
)

# Selecting a count of distinct movies that she was nominated for
df = df.select(
    F.countDistinct('movie')  # Count distinct values in the 'movie' column
)

# Displaying the resultant DataFrame for validation
df.show()

# Overwriting 'oscar_nominees' DataFrame with the result
oscar_nominees = df

# Converting the PySpark DataFrame to a Pandas DataFrame for validation or further processing
oscar_nominees.toPandas()
