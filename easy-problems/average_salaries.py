# Problem Link: https://platform.stratascratch.com/coding/9917-average-salaries?code_type=6
# Calculate the average salary per department and display each employee's salary along with the department's average salary.

# Import necessary libraries
import pyspark
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Assuming `employee` is the DataFrame containing the employee data.

# Define a window specification to partition by department
windowSpec = Window.partitionBy(employee['department'])

# Add a new column 'avg_salary' to calculate the average salary for each department
df = employee.withColumn('avg_salary', F.avg(employee['salary']).over(windowSpec)) \
             .select('department', 'first_name', 'salary', 'avg_salary')  # Select the required columns

# Show the resulting DataFrame with department, employee name, salary, and average salary
df.show()

# Update the employee DataFrame with the new DataFrame containing average salary
employee = df

# Optionally, convert the resulting DataFrame to pandas for validation or further analysis
employee.toPandas()
