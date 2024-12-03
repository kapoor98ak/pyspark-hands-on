# Problem Link: https://platform.stratascratch.com/coding/9992-find-artists-that-have-been-on-spotify-the-most-number-of-times?code_type=6
# Find how many times each artist appeared on the Spotify ranking list.

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Assuming 'spotify_worldwide_daily_song_ranking' is already loaded DataFrame
# 'artist' and 'id' columns are assumed to exist in the DataFrame, with 'artist' being the name of the artist and 'id' being a unique song identifier

# Grouping by 'artist' and counting how many times each artist's song appears (based on 'id') in the rankings
df = spotify_worldwide_daily_song_ranking.groupBy(
    df['artist']  # Grouping by the 'artist' column
).agg(
    F.count(df['id']).alias('n_occurences')  # Counting the number of occurrences of each artist's song
).orderBy(
    F.col('n_occurences').desc()  # Sorting the result in descending order of occurrences
)

# Overwriting the original DataFrame with the new result
spotify_worldwide_daily_song_ranking = df

# Optionally, convert the resulting DataFrame to pandas for validation or further processing
spotify_worldwide_daily_song_ranking.toPandas()
