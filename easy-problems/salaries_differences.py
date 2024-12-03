# Problem Link: https://platform.stratascratch.com/coding/10308-salaries-differences?code_type=6
# Calculate the absolute difference in maximum salaries between 'marketing' and 'engineering' departments.

from pyspark.sql import functions as F

# Join the employee and department dataframes on the department_id
joined_df = db_employee.join(db_dept, db_employee['department_id'] == db_dept['id'])

# Calculate the maximum salary for marketing and engineering departments
salary_diff = joined_df.agg(
    # Get the maximum salary for marketing department employees
    (F.max(F.when(joined_df['department'] == 'marketing', F.col('salary')))).alias('marketing_max'),
    # Get the maximum salary for engineering department employees
    (F.max(F.when(joined_df['department'] == 'engineering', F.col('salary')))).alias('engineering_max')
    ).withColumn(
        # Calculate the absolute difference between the maximum salaries of both departments
        'salary_difference', F.abs(F.col('marketing_max') - F.col('engineering_max'))
    ).select('salary_difference')  # Select only the salary difference column

# Show the resulting salary difference
salary_diff.show()

# Optionally, convert the resulting DataFrame to pandas for validation or further processing
salary_diff.toPandas()
