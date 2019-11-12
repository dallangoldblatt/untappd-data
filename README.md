# untappd-data
AWS Lambda functions for monitoring data posted to Untappd, saving it to an Amazon S3, and matching it to Foursquare venue data

# Requirements

These Lambda functions require automatic triggering and access to an AWS S3 bucket.

See the function code docstrings for detailed descriptions of the required environment variables

# Adding new breweries

The breweries tracked by the Lambda functions are controlled through environment variables.

Go to `Services>Lambda>Functions` and choose `update-untappd-rss-feed-data` and `parse-untappd-rss-feed-data`.
Scroll down to `Environment variables` and edit the value for the key: `untappd-breweries`.
The new brewery's number (assigned by Untappd) is visible in its RSS feed URL.
Make sure that the numbers in both function's environment variables are comma-separated without spaces.

# Lambda Functions

* update-untappd-rss-feed-data - reads new posts from the Untappd website and saves them to the bucket, indexed by brewery number and post id. Uses last_update.json to determine which posts are new on the website and need to be saved
* parse-untappd-rss-feed-data - parses new posts in the bucket and appends the data to untappd_aggregate_data.csv. Uses last_parsed.json to determine which posts are new in the bucket and need to be parsed. Updates venue_list.json as it finds new venues in the posts that it parses.
* get-untappd-venue-locations - reads new venues from venue_list.json and searches Untappd and Foursquare for missing data to be saved in venue_locations.csv.
* clean-and-backup-untappd-data - tries to find data in venue_locations.csv marked as 'Missing' using premium Foursquare calls. After uploading the updated venue data, it saves a copy of all files listed above in a backup folder labelled with the date. This folder will be deleted after one week

*** See function code in AWS Lambda for more detailed documentation/comments ***

# Files

* last_parsed.json - a dictionary mapping brewery numbers to the latest post that has been parsed for that brewery
* last_update.json - the latest post that has been saved to the bucket from the Untappd website. This post may or may not be parsed
* untappd_aggregate_data.csv - csv of every post in the bucket parsed into labelled columns
* venue_list.csv - list of all unique venues that are mentioned in posts and a link to the first post they are mentioned in
* venue_locations.csv - csv of every unique venue, its Untappd and Foursquare venue urls, address, coordinates, categories, and a flag indicating if it is located in the United States. The columns for a venue will contain "Missing" if the get-untappd-venue-locations function failed to find data and they will contain "Unavailable" if the data is still unobtainable after clean-and-backup-untappd-data has run.

*** Do not modify or overwrite these files unless restoring from a backup. You can safely download or read these files ***


# API Keys

API Keys for each Lambda function are stored in their respective environment variables.

AWS keys are granted to a specific user.

Foursquare keys are granted to a free Sandbox account, register to get new keys at https://foursquare.com/developers/signup
