# Problem Link: https://platform.stratascratch.com/coding/9913-order-details?code_type=6
# Filter orders by customers with first names 'Jill' or 'Eva', join with orders, and display order details.

# Import necessary libraries
import pyspark
import pyspark.sql.functions as F

# Assuming `customers` and `orders` are the DataFrames containing customer and order data.

# Filter customers whose first name is either 'Jill' or 'Eva'
df = customers.filter(F.lower(customers['first_name']).isin(['jill', 'eva']))

# Join the filtered customers with orders DataFrame on customer ID, select relevant columns, and order by customer ID
df = orders.join(df, orders.cust_id == df.id).select('first_name', 'order_date', 'order_details', 'total_order_cost').orderBy(orders['cust_id'])

# Show the resulting DataFrame with order details for these customers
df.show()

# Update the customers DataFrame with the new DataFrame containing the order details
customers = df

# Optionally, convert the resulting DataFrame to pandas for validation or further analysis
customers.toPandas()

