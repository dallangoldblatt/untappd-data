#!/usr/bin/env python
"""Collect user post data from Untappd

This function collects post data from the Untappd RSS feeds for target breweries
and saves it to the S3 bucket specified by an environment variable.
Example RSS feed url (Ballast Point): https://untappd.com/rss/brewery/68

Inputs:
    last_update.json - config file containing the latest post from the Untappd
        website that has been saved to the S3 bucket - do not manually modify

Outputs:
    last_update.json - config file containing the latest post from the Untappd
        website that has been saved to the S3 bucket - do not manually modify
    Post data organized by brewery number and post id in S3 bucket (json format)

Environment Variables:
    untappd_access_key_id - access key for AWS user
    untappd_secret_access_key - secret access key for AWS user
    untappd_breweries - comma-separated (without spaces) numbers that represent
        the breweries that posts will be saved for. Brewery numbers can be found
        in the brewery's RSS feed url
	untappd_bucket - name of the target S3 bucket where posts will be saved

Automatic Triggering:
    AWS CloudWatch automatically triggers this function once a minute
"""
__author__ = "Dallan Goldblatt"

import boto3
import feedparser
import json
import os

# Get environment variables
untappd_access_key_id = os.environ['untappd_access_key_id']
untappd_secret_access_key = os.environ['untappd_secret_access_key']
untappd_breweries = os.environ['untappd_breweries']
untappd_bucket = os.environ['untappd_bucket']

def create_client(access_key_id, secret_access_key):
    """Create S3 client for later use"""
    client = boto3.client('s3',
                          aws_access_key_id=access_key_id,
                          aws_secret_access_key=secret_access_key)
    return client

def get_posts(url):
    """Get 25 posts from the RSS feed for a specific brewery"""
    feed = feedparser.parse(url)
    return feed.entries

def get_all_posts(breweries):
    """Get all posts from the RSS feeds for all breweries"""
    posts = []
    for brewery in breweries:
        posts += get_posts(f'https://untappd.com/rss/brewery/{brewery}')
    return posts

def get_last_update_id(client):
    """Read last_update.json in the S3 database to get most recent post"""
    f = client.get_object(Bucket=,
                          Key='last_update.json')['Body']
    # f is a StreamingBody object in json, load to retrieve id number
    return json.load(f)['id']

def set_last_update_id(client, id):
    """Write most recent post id to last_update.json in S3 database"""
    # json format is used in case more key-value pairs need to be stored
    body = {'id': id}
    json_body = json.dumps(body)
    client.put_object(ACL='private',
                      Bucket=untappd_bucket,
                      Key='last_update.json',
                      Body=json_body)

def write_post_to_s3(client, post, post_id):
    """Write an entire post to the S3 database"""
    # Convert post to json
    json_body = json.dumps(post)
    # Get unique brewery id (assigned by Untappd) from post
    # Example: https://untappd.com/rss/brewery/68 has brewery id 68
    brewery_id = post['title_detail']['base'].rsplit('/', 1)[-1]
    # Construct key for object using brewery id and post id
    post_key = f"{brewery_id}/{brewery_id}-{post_id}"
    client.put_object(ACL='private',
                      Bucket=untappd_bucket,
                      Key=post_key,
                      Body=json_body)

def main():
    """Get and handle all new posts on Untappd"""
    # Create client for interfacing with S3
    s3 = create_client(untappd_access_key_id, untappd_secret_access_key)

    # Get id of the latest post that was handled in the previous function call
    last_update_id = get_last_update_id(s3)
    # Set inital value for lastest post handled by this function call
    most_recent_id = last_update_id

    # Get list of all posts from all breweries specified by env variable
    posts = get_all_posts(untappd_breweries.split(','))

    # Iterate over all posts and write new posts to the database
    for post in posts:
        # Get unique post id number from post
        # Example: 'https://untappd.com/user/Mckman007/checkin/756802330'
        # has post id number 756802330
        post_id = int(post['id'].rsplit('/', 1)[-1])
        # if post_id is greater than last_update_id, post is not yet in database
        if(post_id > last_update_id):
            most_recent_id = max(most_recent_id, post_id)
            write_post_to_s3(s3, post, post_id)

    # After all new posts have been written to database, set last_update_id
    # for next function call
    set_last_update_id(s3, most_recent_id)

def lambda_handler(event, context):
    main()

if __name__ == '__main__':
    main()
