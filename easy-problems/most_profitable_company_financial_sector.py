# Problem Link: https://platform.stratascratch.com/coding/9663-find-the-most-profitable-company-in-the-financial-sector-of-the-entire-world-along-with-its-continent?code_type=6

# Import your libraries
import pyspark

# Start writing code
# Filter the 'forbes_global_2010_2014' dataframe for companies in the 'Financials' sector
df = forbes_global_2010_2014.filter(forbes_global_2010_2014['sector'] == 'Financials') \
    .select('company', 'continent') \
    .orderBy(forbes_global_2010_2014['profits'].desc()) \
    .limit(1)  # Get the most profitable company by sorting profits in descending order and limiting to 1

df.show()  # Show the resulting dataframe with the most profitable company and its continent

# Assign the result to 'forbes_global_2010_2014' variable for further use
forbes_global_2010_2014 = df

# To validate your solution, convert your final PySpark df to a pandas df
forbes_global_2010_2014.toPandas()
