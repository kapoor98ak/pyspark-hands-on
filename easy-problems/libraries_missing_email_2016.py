# Problem Link: https://platform.stratascratch.com/coding/9924-find-libraries-who-havent-provided-the-email-address-in-2016-but-their-notice-preference-definition-is-set-to-email?code_type=6
# Find libraries that haven't provided the email address in circulation year 2016 but their notice preference definition is set to email.

# Import your libraries
import pyspark

# Assuming 'library_usage' is already loaded DataFrame with columns: 'provided_email_address', 'notice_preference_definition', and 'home_library_code'

# Filter libraries where:
# 1. 'provided_email_address' is False (email not provided)
# 2. 'notice_preference_definition' is set to 'email'
df = library_usage.filter(
    (df['provided_email_address'] == False) &  # Filter for records where email address is not provided
    (df['notice_preference_definition'] == 'email')  # Filter for libraries with 'email' as notice preference
).select(
    df['home_library_code']  # Select the 'home_library_code' column
).distinct()  # Ensuring no duplicate home library codes in the result

# Show the resulting DataFrame with distinct home library codes
df.show()

# Overwrite the 'library_usage' DataFrame with the filtered result
library_usage = df

# Optionally, convert the resulting DataFrame to pandas for validation or further processing
library_usage.toPandas()
