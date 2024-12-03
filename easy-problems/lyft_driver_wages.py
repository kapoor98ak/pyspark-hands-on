# Problem Link: https://platform.stratascratch.com/coding/10003-lyft-driver-wages?code_type=6
# Filter Lyft drivers' data based on yearly salary criteria (<= 30,000 or >= 70,000).

# Import necessary libraries
import pyspark

# Assuming `lyft_drivers` is already loaded DataFrame with columns including 'yearly_salary'

# Filter rows where 'yearly_salary' is either less than or equal to 30,000 or greater than or equal to 70,000
df = lyft_drivers.where(
    (lyft_drivers.yearly_salary <= 30000) |  # Condition for salary <= 30,000
    (lyft_drivers.yearly_salary >= 70000)    # Condition for salary >= 70,000
)

# Show the resulting DataFrame with the filtered records
df.show()

# Optionally, convert the resulting DataFrame to pandas for validation or further processing
df.toPandas()
