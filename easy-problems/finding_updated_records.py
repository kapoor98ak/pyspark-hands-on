# Problem Link: https://platform.stratascratch.com/coding/10299-finding-updated-records?code_type=6
# Find updated records for employee salaries based on maximum salary for each employee and department.

# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
# Grouping by 'id', 'department_id', 'first_name', and 'last_name' to find the most recent (maximum) salary for each employee.
df = ms_employee_salary.groupBy('id', 'department_id', 'first_name', 'last_name').agg(
    # Get the maximum salary for each employee in the same department
    F.max(ms_employee_salary.salary).alias('current_salary')  # Renaming the maximum salary column to 'current_salary'
).orderBy(ms_employee_salary.id.asc())  # Ordering by employee 'id' in ascending order

# Overwrite the original DataFrame with the updated result
ms_employee_salary = df

# Show the resulting DataFrame with the updated salary records
ms_employee_salary.show()

# Optionally, convert the resulting DataFrame to pandas for validation or further processing
ms_employee_salary.toPandas()
