# Problem Link: https://platform.stratascratch.com/coding/10087-find-all-posts-which-were-reacted-to-with-a-heart?code_type=6
# Find all posts which were reacted to with a heart.

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Assuming the DataFrames 'facebook_reactions' and 'facebook_posts' are pre-loaded with relevant schema:
# 'facebook_reactions' contains columns: 'reaction', 'post_id', and other fields
# 'facebook_posts' contains columns: 'post_id' and other fields

# Filter 'facebook_reactions' to keep only rows where the reaction is 'heart'
df = facebook_reactions.filter(
    facebook_reactions['reaction'] == 'heart'  # Keep reactions that are hearts
).select(
    'post_id'  # Selecting only the post_id of the reactions
).distinct()  # Removing duplicate post_ids

# Join 'facebook_posts' with 'df' (which contains post_ids with a heart reaction)
df2 = facebook_posts.join(
    df,  # Joining with the filtered DataFrame 'df' containing post_id
    df.post_id == facebook_posts.post_id  # Joining on 'post_id' column
).select(
    facebook_posts['*']  # Selecting all columns from 'facebook_posts'
)

# Displaying the resultant DataFrame for validation
df2.show()

# Overwriting 'facebook_reactions' DataFrame with the joined result
facebook_reactions = df2

# Converting the PySpark DataFrame to a Pandas DataFrame for validation or further processing
facebook_reactions.toPandas()
