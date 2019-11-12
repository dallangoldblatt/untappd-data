#!/usr/bin/env python
"""Parse and aggregate user post data from Untappd

This function parses Untappd post data in the S3 bucket specified by env var,
saves it to an aggregate data file in csv format, and keeps track of unique
venues that are mentioned in the posts

Inputs:
    last_parsed.json - config file containing a dictionary mapping brewery
        numbers to the latest post that has been parsed for that brewery - do
        not manually modify
    untappd_aggregate_data.csv - csv containing information from every post in
        the bucket parsed into labelled columns
    venue_list.csv - list of all unique venues that are mentioned in posts
    Post data organized by brewery number and post id in the S3 bucket

Outputs:
    last_parsed.json - config file containing a dictionary mapping brewery
        numbers to the latest post that has been parsed for that brewery - do
        not manually modify
    untappd_aggregate_data.csv - csv containing information from every post in
        the bucket parsed into labelled columns
    venue_list.csv - list of all unique venues that are mentioned in posts

Environment Variables:
    untappd_access_key_id - access key for AWS user
    untappd_secret_access_key - secret access key for AWS user
    untappd_breweries - comma-separated (without spaces) numbers that represent
        the breweries that posts will be saved for. Brewery numbers can be found
        in the brewery's RSS feed url
	untappd_bucket - name of the target S3 bucket where posts will be saved

Automatic Triggering:
    AWS CloudWatch automatically triggers this function once an hour
"""
__author__ = "Dallan Goldblatt"

import boto3
import csv
import json
import os

# Get environment variables
untappd_access_key_id = os.environ['untappd_access_key_id']
untappd_secret_access_key = os.environ['untappd_secret_access_key']
untappd_breweries = os.environ['untappd_breweries'].split(',')
untappd_bucket = os.environ['untappd_bucket']

def create_client(access_key_id, secret_access_key):
    """Create S3 client for later use"""
    client = boto3.client('s3',
                          aws_access_key_id=access_key_id,
                          aws_secret_access_key=secret_access_key)
    return client

def download_parsed_data(client, file):
    """Download exisiting parsed data"""
    # S3 does not support appending to objects in place
    client.download_file(Bucket=untappd_bucket,
                         Key='untappd_aggregate_data.csv',
                         Filename=file)

def upload_parsed_data(client, file):
    """Upload parsed data file to S3"""
    # S3 does not support appending to objects in place
    client.upload_file(Bucket=untappd_bucket,
                       Key='untappd_aggregate_data.csv',
                       Filename=file)

def download_venue_list(client, file):
    """Download existing csv containing venue names"""
    # S3 does not support appending to objects in place
    client.download_file(Bucket=untappd_bucket,
                         Key='venue_list.csv',
                         Filename=file)

def upload_venue_list(client, file):
    """Upload venue csv file to S3"""
    # S3 does not support appending to objects in place
    client.upload_file(Bucket=untappd_bucket,
                       Key='venue_list.csv',
                       Filename=file)

def get_last_parsed_ids(client, breweries):
    """Read S3 file containing most recently parsed post ids for each brewery"""
    f = client.get_object(Bucket=untappd_bucket,
                          Key='last_parsed.json')['Body']
    # f is a StreamingBody object in json, load to retrieve id number dictionary
    ids = json.load(f);
    # Handle missing keys resulting from when a new brewery is added
    for brewery in breweries:
        if brewery not in ids:
            ids[brewery] = ''
    return ids

def set_last_parsed_ids(client, ids):
    """Write most recently parsed post ids for each brewery to S3"""
    json_body = json.dumps(ids)
    client.put_object(ACL='private',
                      Bucket=untappd_bucket,
                      Key='last_parsed.json',
                      Body=json_body)

def get_next_post_ids(client, brewery, start):
    """Get object names in S3 of up to 1000 unparsed posts for a brewery"""
    # Construct object name of starting post (ordered by last modified FIFO)
    # Handle missing start id when a new brewery is added
    start = '' if not start else f'{brewery}/{brewery}-{start}'

    # Get list of up to 1000 unparsed posts' object names
    resp = client.list_objects_v2(Bucket=untappd_bucket,
                                  Prefix=f'{brewery}/{brewery}-',
                                  StartAfter=start,
                                  MaxKeys=1000)
    return resp['Contents'] if resp['KeyCount'] > 0 else []

def split(strng, sep, n):
    """Utility function for splitting a string by the nth occurance of sep"""
    strng = strng.split(sep)
    return sep.join(strng[:n]), sep.join(strng[n:])

def parse_post(client, post_id):
    """Parse passed post into list of attributes"""
    # Get post from S3
    f = client.get_object(Bucket=untappd_bucket, Key=post_id)['Body']
    # f is a StreamingBody object in json, load to retrieve post data
    post = json.load(f)
    # Construct list that will become a row in the csv
    row = [int(post_id.split('-')[-1])] # guid
    row.append(post['link'].split('/')[-3]) # username
    row.append(post_id.split('/')[0]) # brewery
    # Get title containing beer and location/venue name
    title = post['title']
    # Handle special case when beer name contains 'at'
    beer_ex_list = ['victory at sea', 'murder at schrute farm...death by fire']
    loc_idx = 2 if any(sbstr in title.lower() for sbstr in beer_ex_list) else 1
    s = split(title, ' at ', loc_idx) # extract location if it exists
    y = s[0].split(' is drinking ') # extract beer name
    row.append(y[1].split(' ', 1)[1]) # beer name
    if len(s) > 1:
        row.append(s[1]) # location
    else:
        row.append('') # location
    # Check if comment or rating or both exist in post
    summary_split = post['summary'].rsplit('(', 1)
    if len(summary_split) == 2:
        row.append(summary_split[0]) # comment
        try:
            rating = float(summary_split[1].split('/5 Stars')[0]) # rating
        except ValueError:
            rating = ''
        row.append(rating) # rating
    elif len(summary_split) == 1:
        row.append(summary_split[0]) # comment
        row.append('') # rating
    else:
        row.append('') # comment
        row.append('') # rating
    row.append(post['published']) # date
    row.append(post['link']) # url
    return row

def append_to_csv(file, rows):
    """Append list of rows to existing csv"""
    with open(file, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

def append_to_venues(venue_file, rows):
    """Append new venues to file containing existing list of venues"""
    # Read file containing list of existing venues
    with open(venue_file, "r", encoding='utf-8') as f:
        data = csv.reader(f)
        next(data) # skip headers
        venue_list = [row[0] for row in list(data)] # get list of venues only
    
    # Get list of new venues and urls from new rows, ignoring rows with no venue
    new_venues = [[row[4], row[8]] for row in rows if row[4] != '']
    # Compare lists, ignoring duplicates in both existing list and appended list
    new_rows = []
    new_venue_names = []
    for venue in new_venues:
        if venue[0] not in venue_list and venue[0] not in new_venue_names:
            new_rows.append(venue)
            new_venue_names.append(venue[0])

    # Append new venues to csv
    with open(venue_file, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(new_rows)

def main():
    """Get and handle all new posts in S3 bucket"""
    # Temp directory for csvs
    csv_file = '/tmp/data.csv'
    venue_file = '/tmp/venue_list.csv'

    # Create client for interfacing with S3
    s3 = create_client(untappd_access_key_id, untappd_secret_access_key)

    # Get ids of latest posts that have already been added to the csv
    last_parsed_ids = get_last_parsed_ids(s3, untappd_breweries)

    # Generate new rows to add to csv for each brewery
    rows = []
    for brewery in untappd_breweries:
        # Parse all new posts for brewery, up to 1000 posts at a time
        while True:
            next_post_ids = get_next_post_ids(s3,
                                              brewery,
                                              last_parsed_ids[brewery])
            # Jump to next brewery if no new posts remain
            if not next_post_ids:
                break;
            for obj in next_post_ids:
                rows.append(parse_post(s3, obj['Key']))
                last_parsed_ids[brewery] = obj['Key'].split('-')[-1]

    if rows:
        # Download csvs with existing data
        download_parsed_data(s3, csv_file)
        download_venue_list(s3, venue_file)

        # Append new rows to existing data files
        append_to_csv(csv_file, rows)
        append_to_venues(venue_file, rows)

        # Upload finished data
        upload_parsed_data(s3, csv_file)
        upload_venue_list(s3, venue_file)
        set_last_parsed_ids(s3, last_parsed_ids)

def lambda_handler(event, context):
    main()

if __name__ == '__main__':
    main()