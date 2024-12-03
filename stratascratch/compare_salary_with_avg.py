# Question Link: https://platform.stratascratch.com/coding/9917-average-salaries?code_type=6

# Importing required PySpark libraries
import pyspark
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# DataFrame 'employee' is assumed to be pre-loaded with the following schema:
# Columns: 'department', 'first_name', 'salary'

# Creating a window specification to partition the data by department
# The window partition ensures calculations are done for each department group
windowSpec = Window.partitionBy(employee['department'])

# Adding a new column 'avg_salary' to calculate the average salary within each department
# Using the `over` function with the specified window to calculate the department-wise average
df = employee.withColumn(
    'avg_salary',  # Name of the new column
    F.avg(employee['salary']).over(windowSpec)  # Average salary calculated over the department
).select(
    'department', 'first_name', 'salary', 'avg_salary'  # Selecting required columns
)

# Displaying the resultant DataFrame for validation
df.show()

# Overwriting 'employee' DataFrame with the modified DataFrame containing 'avg_salary'
employee = df

# Converting the PySpark DataFrame to a Pandas DataFrame for validation or further processing
# This is typically used to validate small datasets or for use in environments that require Pandas
employee.toPandas()
