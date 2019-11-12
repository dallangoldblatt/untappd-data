#!/usr/bin/env python
"""Fill in missing Untappd venue data and backup files for last 7 days

This function searches for missing data in venue_locations.csv by using premium
Foursquare API requests. After uploading the updated file, it copies all files
other than post data to a backup folder labelled with the date and deletes the
backups older than 7 days.

Inputs:
    venue_locations.csv - existing csv of every unique venue and its Untappd
        and Foursquare urls, adddress, coordinates, and categories if available

Outputs:
    venue_locations.csv - existing csv of every unique venue and its Untappd
        and Foursquare urls, adddress, coordinates, and categories if available
    Copy of all non-post data in the backup folder labelled with the date

Environment Variables:
    aws_access_key_id - access key for AWS user
    aws_secret_access_key - secret access key for AWS user
    foursquare_client_id - access key for free Sandbox Foursquare account that
        has been verified with a credit card
    foursquare_client_secret - secret access key for free Sandbox Foursquare 
        account that has been verified with a credit card
	untappd_bucket - name of the target S3 bucket where posts will be saved

Automatic Triggering:
    AWS CloudWatch automatically triggers this function once every day
"""
__author__ = "Dallan Goldblatt"

import boto3
import csv
import datetime
import json
import os
import requests
import time

aws_access_key_id = os.environ['aws_access_key_id']
aws_secret_access_key = os.environ['aws_secret_access_key']
foursquare_client_id = os.environ['foursquare_client_id']
foursquare_client_secret = os.environ['foursquare_client_secret']
untappd_bucket = os.environ['untappd_bucket']

def create_client(access_key_id, secret_access_key):
    """Create S3 client for later use"""
    client = boto3.client('s3',
                          aws_access_key_id=access_key_id,
                          aws_secret_access_key=secret_access_key)
    return client

def download_venue_locations(client, file):
    """Download existing csv containing venue location data"""
    client.download_file(Bucket=untappd_bucket,
                         Key='venue_locations.csv',
                         Filename=file)

def upload_venue_locations(client, file):
    """Upload venue location csv to S3"""
    client.upload_file(Bucket=untappd_bucket,
                       Key='venue_locations.csv',
                       Filename=file)

def read_csv_to_dict(file):
    """Read venue location csv into a dictionary indexed by venue name"""
    # Get 2D list of all rows from csv
    with open(file, "r", encoding='utf-8') as f:
        data = csv.reader(f)
        next(data) # skip headers
        venue_list = list(data)
    # Convert list to dictionary indexed by venue name
    venue_exists = venue_list and venue_list[0]
    return {venue:data for venue, *data in venue_list} if venue_exists else {}

def write_dict_to_csv(venue_dict, file):
    """Write venue location dictionary to the venue location csv"""
    # Convert dictionary to 2D list of rows
    venue_list = [[venue, *data] for venue, data in venue_dict.items()]
    headers = ['venue', 'untappd_url', 'foursquare_url', 'address',
               'lat', 'long', 'categories', 'in_united_states']
    # Write rows to csv, overwriting old data since order is not guaranteed
    with open(file, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Write headers
        writer.writerow(headers)
        # Write venue data
        writer.writerows(venue_list)

def safe_dict(dict_name, key):
    """Utility function for handling missing keys in dictionary"""
    try:
        return dict_name[key]
    except KeyError:
        return 'Unavailable'
        
def search_foursquare(foursquare_url, coords, foursquare_client_id, foursquare_client_secret):
    """Get address data list for a venue details request to Foursquare API"""
    # Unpack data from venue_info
    venue_id = foursquare_url.split('/')[-1]
    # Query Foursquare API for venue
    search_url = 'https://api.foursquare.com/v2/venues/' + venue_id
    params = dict(v=time.strftime('%Y%m%d'),
                  client_id=foursquare_client_id,
                  client_secret=foursquare_client_secret)
    with requests.get(url=search_url, params=params) as query:
        if query.status_code == 400:
            # Param Error indicates venue id no longer exists
            return ['Unavailable', *coords, 'Unavailable', 'Unavailable']
        elif query.status_code != 200:
            print('Foursquare request returned status code:',
                query.status_code, '\n\t', query.reason)
            return ['Missing', *coords, 'Missing', 'Missing'] # Try again later
        else:
            data = json.loads(query.text)
    try:
        # Extract missing data returned by requst
        venue = data['response']['venue']['location']
        categories = data['response']['venue']['categories']
    except KeyError: # KeyError if address is set to private or doesn't extist
        return ['Unavailable', *coords, 'Unavailable', 'Unavailable']
    row = []
    # Get address as list before joining to save in one column
    address_list = safe_dict(venue, 'formattedAddress')
    address_list = address_list if isinstance(address_list, list) else [address_list]
    row.append(', '.join(str(a) for a in address_list ))
    # Get coordinates
    row.append(safe_dict(venue, 'lat'))
    row.append(safe_dict(venue, 'lng'))
    # Extract all categories to save in one column
    if categories:
        category_list = [safe_dict(category, 'name') for category in categories]
        row.append(', '.join(str(c) for c in category_list))
    else: # Indicate there are no categories
        row.append('Uncategorized')
    # Check if location is in United States
    country = safe_dict(venue, 'country')
    in_us = 'Unavailable' if country == 'Unavailable' else country == 'United States'
    row.append(in_us)
    return row

def create_backup(client):
    """Create backup of all files needed for Lambda function execution other than post data"""
    today = datetime.date.today().strftime("%Y-%m-%d")
    last_week = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    last_week2 = (datetime.date.today() - datetime.timedelta(days=8)).strftime("%Y-%m-%d")
    file_list = ['last_parsed.json', 'last_update.json', 'untappd_aggregate_data.csv',
                 'venue_list.csv', 'venue_locations.csv']
    
    # Copy current files to new backup folder
    for file in file_list:
        client.copy_object(Bucket=untappd_bucket,
                           CopySource=f'untappd-rss-feed-data/{file}',
                           Key=f'Backups/{today}/{file}')
    
    # Delete backup from one week ago (and 8 days ago if it exists)
    keys_to_delete = {'Objects': [{'Key': f'Backups/{last_week}/{file}'} for file in file_list]}
    client.delete_objects(Bucket=untappd_bucket, Delete=keys_to_delete)
    keys_to_delete2 = {'Objects': [{'Key': f'Backups/{last_week2}/{file}'} for file in file_list]}
    client.delete_objects(Bucket=untappd_bucket, Delete=keys_to_delete2)

def main():
    """Use premium Foursquare API calls to fix missing data and backup non-post files in S3"""
    # Temp directory for csv
    venue_locations_file = '/tmp/venue_locations.csv'

    # Create client for interfacing with S3
    s3 = create_client(aws_access_key_id, aws_secret_access_key)

    # Download existing venue location data csv
    download_venue_locations(s3, venue_locations_file)

    # Convert venue location data to a searchable dictionary
    venue_dict = read_csv_to_dict(venue_locations_file)
    
    # Find rows with missing data and attempt to fill in
    for venue, venue_data in venue_dict.items():
        foursquare_url = venue_data[1]
        coords = [venue_data[3], venue_data[4]]
        # Skip rows where all data is present or if Foursquare url is missing
        if 'Missing' not in venue_data or foursquare_url == 'Missing':
            continue
        # Use premium Foursquare API calls to get details of venue by ID
        venue_dict[venue][-5:] = search_foursquare(foursquare_url,
                                                   coords,
                                                   foursquare_client_id,
                                                   foursquare_client_secret)
        if venue_dict[venue][3] == 'Missing':
            # Foursquare rejected request, stop searching Foursquare
            break
    
    # Write venue location dictionary back to csv
    write_dict_to_csv(venue_dict, venue_locations_file)
    
    # Upload new venue location data
    upload_venue_locations(s3, venue_locations_file)
    
    # Create backup of all non-post files so operation can be restored from an 
    # earlier date if Lambda functions produce erroneous data 
    create_backup(s3)
    
def lambda_handler(event, context):
    main()

if __name__ == '__main__':
    main()
