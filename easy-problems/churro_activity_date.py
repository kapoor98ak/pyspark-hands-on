# Problem Link: https://platform.stratascratch.com/coding/9688-churro-activity-date?code_type=6
# Task: Find the activity dates and descriptions for 'STREET CHURROS' when the score is less than 95.

# Import necessary libraries
import pyspark

# Start writing code
x = los_angeles_restaurant_health_inspections

# Filter the data to find inspections for 'STREET CHURROS' with a score less than 95
# We are selecting the activity date and description for these specific records
df = x.filter((x['score'] < 95) & (x['facility_name'] == 'STREET CHURROS')).select('activity_date', 'pe_description')

# Show the filtered DataFrame
df.show()

# Assign the filtered result back to the variable for further use
los_angeles_restaurant_health_inspections = df

# To validate your solution, convert the final PySpark DataFrame to a pandas DataFrame for easier inspection
los_angeles_restaurant_health_inspections.toPandas()

