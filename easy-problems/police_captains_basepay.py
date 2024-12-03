# Problem Link: https://platform.stratascratch.com/coding/9972-find-the-base-pay-for-police-captains?code_type=6
# Find the base pay for Police Captains from the public salary dataset.

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Assuming 'sf_public_salaries' is already loaded DataFrame with columns: 'employeename', 'jobtitle', and 'basepay'

# Select distinct rows for employee name and their base pay, rounding the base pay to 2 decimal places
# Filter the data where job title contains 'captain' (case-insensitive)
df = sf_public_salaries.select(
    "employeename",  # Selecting the employee name column
    F.round(df.basepay, 2)  # Rounding the base pay to 2 decimal places
).distinct().filter(
    F.lower(df.jobtitle).like("%captain%")  # Filtering for job titles containing 'captain' (case-insensitive)
)
# Ensuring there are no duplicates in the result

# Display the resulting DataFrame with employee names and their rounded base pay for captains
df.show()

# Overwriting 'sf_public_salaries' with the new DataFrame containing filtered and rounded results
sf_public_salaries = df

# Optionally, convert the resulting DataFrame to pandas for validation or further processing
sf_public_salaries.toPandas()
