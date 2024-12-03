# Problem Link: https://platform.stratascratch.com/coding/10166-reviews-of-hotel-arena?code_type=6
# Analyze the reviews for 'Hotel Arena' by grouping them based on reviewer scores.

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Assuming the DataFrame 'hotel_reviews' is pre-loaded with schema:
# Columns: 'hotel_name', 'reviewer_score', 'review_date'

# Filtering reviews for 'Hotel Arena' and grouping by reviewer score and hotel name
df = hotel_reviews.filter(
    hotel_reviews.hotel_name == 'Hotel Arena'  # Filter rows where hotel name is 'Hotel Arena'
).groupBy(
    hotel_reviews['reviewer_score'],  # Grouping by reviewer score
    hotel_reviews['hotel_name']       # Grouping by hotel name
).agg(
    F.count(hotel_reviews['review_date']).alias('n_reviews')  # Counting the number of reviews
)

# Displaying the resultant DataFrame for validation
df.show()

# Overwriting 'hotel_reviews' DataFrame with the aggregated results
hotel_reviews = df

# Converting the PySpark DataFrame to a Pandas DataFrame for validation or further processing
hotel_reviews.toPandas()
