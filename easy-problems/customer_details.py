# Problem Link: https://platform.stratascratch.com/coding/9891-customer-details?code_type=6
# Join customers with their orders, handle missing order details, and display customer details.

# Import necessary libraries
import pyspark.sql.functions as F

# Assuming `customers` and `orders` are the DataFrames containing customer and order data.

# Perform a left join to get customer details along with their order details
df = customers.join(orders, customers.id == orders.cust_id, 'left')

# Select relevant columns: first name, last name, city, and order details (using coalesce to handle nulls)
df = df.select(
        df['first_name'], 
        df['last_name'], 
        df['city'], 
        F.coalesce(orders['order_details'], F.lit('')).alias('order_details')
    ).orderBy("first_name", "order_details")

# Store the result into a new DataFrame
result = df

# Optionally, convert the resulting DataFrame to pandas for validation or further analysis
result.toPandas()

