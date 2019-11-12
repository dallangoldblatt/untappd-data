#!/usr/bin/env python
"""Get location data for Untappd venues

This function searches for address, coodinate, and category data for all unique 
venues (by name) that are mentioned in post data saved in the untappd-rss-feed-
data S3 bucket

Inputs:
    venue_list.csv - list of all unique venues that are mentioned in posts
    venue_locations.csv - existing csv of every unique venue and its Untappd
        and Foursquare urls, adddress, coordinates, and categories if available

Outputs:
    venue_locations.csv - existing csv of every unique venue and its Untappd
        and Foursquare urls, adddress, coordinates, and categories if available

Environment Variables:
    aws_access_key_id - access key for AWS user
    aws_secret_access_key - secret access key for AWS user
    foursquare_client_id - access key for free Sandbox Foursquare account that
        has been verified with a credit card
    foursquare_client_secret - secret access key for free Sandbox Foursquare 
        account that has been verified with a credit card
	untappd_bucket - name of the target S3 bucket where posts will be saved

Automatic Triggering:
    AWS CloudWatch automatically triggers this function once every 6 hours
"""
__author__ = "Dallan Goldblatt"

import boto3
import csv
import json
import os
import random
import requests
import time
from html.parser import HTMLParser

# Get environment variables
aws_access_key_id = os.environ['aws_access_key_id']
aws_secret_access_key = os.environ['aws_secret_access_key']
foursquare_client_id = os.environ['foursquare_client_id']
foursquare_client_secret = os.environ['foursquare_client_secret']
untappd_bucket = os.environ['untappd_bucket']

user_agents = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
                   'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
                   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36',
                   'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
                   'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
                   'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)',
                   'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko']

class CheckinHTMLParser(HTMLParser):
    """HTMLParser for reading the venue link from the Untappd checkin page"""
    def __init__(self):
        super().__init__()
        self.found_p = False
        self.url = []
    def handle_starttag(self, tag, attributes):
        # The venue link is always found in an 'a' tag nested in a 'p' tag with
        # 'location' as its class
        if tag != 'a' and tag != 'p':
            return
        if not self.found_p and tag == 'p':
            for name, value in attributes:
                if name == 'class' and value == 'location':
                    self.found_p = True
                    return
        if self.found_p and tag == 'a':
            for name, value in attributes:
                if name == 'href':
                    self.url.append(value)
                    return
    def handle_endtag(self, tag):
        if tag == 'p':
            self.found_p = False
    def handle_data(self, data):
        pass

class VenueHTMLParser(HTMLParser):
    """HTMLParser for reading the Foursquare link and venue coordinates from the
    Untappd venue page"""
    def __init__(self):
        super().__init__()
        self.found_div = False
        self.urls = []
        self.coords = []
    def handle_starttag(self, tag, attributes):
        # The Foursquare link is always found in an 'a' tag nested in a 'div' 
        # tag with 'venue-social' as its class
        # The venue coordinates are found in 'meta' tags with 'property' set
        # to 'place:location:latitude' and 'place:location:longitude'
        if tag not in ['div', 'a', 'meta']:
            return
        attributes_dict = dict(attributes)
        if tag == 'meta' and 'property' in attributes_dict:
            if attributes_dict['property'] in ['place:location:latitude',
                                               'place:location:longitude']:
                self.coords.append(attributes_dict['content'])
            return
        if 'class' not in attributes_dict:
            return
        if (not self.found_div and tag == 'div' 
            and attributes_dict['class'] == 'venue-social'):
            self.found_div = True
            return
        if (self.found_div and tag == 'a' 
            and attributes_dict['class'] == 'fs track-click'):
            self.urls.append(attributes_dict['href'].split('?')[0])
            return
    def handle_endtag(self, tag):
        if tag == 'div':
            self.found_div = False
    def handle_data(self, data):
        pass

def create_client(access_key_id, secret_access_key):
    """Create S3 client for later use"""
    client = boto3.client('s3',
                          aws_access_key_id=access_key_id,
                          aws_secret_access_key=secret_access_key)
    return client

def get_venue_list(client):
    """Get list of venue names from existing csv in S3"""
    sb = client.get_object(Bucket=untappd_bucket,
                           Key='venue_list.csv')['Body']
    # sb is a StreamingBody which needs to be decoded into a csv format
    f = sb.read().decode('utf-8').splitlines(True)
    # Read list of all venues from csv
    data = csv.reader(f)
    next(data) # skip header
    # Convert data to list of rows
    return list(data)

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

def search_untappd(checkin_url):
    """Get Foursquare venue url and venue coordinates from from following venue 
    link on Untappd checkin"""
    # Set User-Agent header to simulate requests coming from different browsers
    headers = {'User-Agent': random.choice(user_agents),
               'Content-Type': 'text/html'}
    # Get html for checkin page using Untappd checkin url
    with requests.get(checkin_url, headers=headers) as response:
        if response.status_code == 404:
            # Checkin was deleted by user
            return ['Missing'] * 5 # Do not try again later
        if response.status_code != 200:
            # Handle unexpected errors
            print('Untappd request returned status code:', response.status_code,
                  '\n\t', response.reason)
            return [''] * 5 # Try again later
        htmlstring = response.text
    # Feed html as text to parser to get Untappd venue url
    parser = CheckinHTMLParser()
    parser.feed(htmlstring)
    # Extract Untappd venue url from parser if it exists
    if not parser.url:
        # Checkin location tag was deleted by user
         return ['Missing'] * 5 # Do not try again later
    untappd_venue_url = 'https://untappd.com' + parser.url[0]

    # Get html for venue page using Untappd venue url
    with requests.get(untappd_venue_url, headers=headers) as response:
        if response.status_code == 404:
            # Venue was deleted or merged with a different venue url
             return ['Missing'] * 5 # Do not try again later
        if response.status_code != 200:
            print('Untappd request returned status code:', response.status_code,
                  '\n\t', response.reason)
            return [''] * 5 # Try again later
        htmlstring = response.text
    # Feed html as text to parser to get Foursquare venue url
    parser = VenueHTMLParser()
    parser.feed(htmlstring)
    # Extract data from parser, may have more than one link (desktop and mobile)
    foursquare_venue_urls = parser.urls
    coords = parser.coords
    if foursquare_venue_urls and coords:
        return [untappd_venue_url, foursquare_venue_urls[0], '',
                coords[0], coords[1]] # Return data for searching Foursquare
    else:
        # Do not try again later since Untappd was missing data
        return ['Missing'] * 5

def search_foursquare(venue, venue_info, foursquare_client_id, foursquare_client_secret):
    """Get address data list for a venue search from Foursquare"""
    # Unpack data from venue_info
    venue_url = venue_info[1]
    coords = [venue_info[3], venue_info[4]]
    # Handle when Untappd could not find a location
    if venue_url == 'Missing':
        return ['Missing'] * 5
    # Pull venue id from url
    venue_id = venue_url.split('/')[-1]
    # Query Foursquare API for venue using data from Untappd
    search_url = 'https://api.foursquare.com/v2/venues/search'
    params = dict(intent='browse',
                  query=venue,
                  ll=','.join(coords),
                  radius=25000,
                  limit=10,
                  v=time.strftime('%Y%m%d'),
                  client_id=foursquare_client_id,
                  client_secret=foursquare_client_secret)
    with requests.get(url=search_url, params=params) as query:
        if query.status_code != 200:
            print('Foursquare request returned status code:',
                query.status_code, '\n\t', query.reason)
            return ['', *coords, '', ''] # Try again later
        else:
            data = json.loads(query.text)
    # Get list of venues returned by search (up to 10)
    venue_list = data['response']['venues']
    for venue_item in venue_list:
        # Compare venue_item's id to the one in the Foursquare url on Untappd
        if (venue_item['id'] != venue_id):
            continue # See if next venue from venue_list matches origial
        # Successful match, extract data
        venue = venue_item['location']
        categories = venue_item['categories']
        row = []
        try:
            # Add columns to row
            # Join address list to save in one column
            row.append(', '.join(str(a) for a in venue['formattedAddress']))
            # Include lat and long from Foursquare
            row.append(venue['lat'])
            row.append (venue['lng'])
            # Join all categories to save in one column
            if categories:
                category_list = [category['name'] for category in categories]
                row.append(', '.join(str(c) for c in category_list))
            else: # Indicate there are no categories
                row.append('Uncategorized')
            # Check if location is in United States
            row.append(venue['country'] == 'United States')
            return row
        except KeyError: # KeyError if address is set to private, return Missing
            pass
    # No matching venues were returned from search, do not try again
    return ['Missing', *coords, 'Missing', 'Missing']

def backup_data(s3, venue_dict, venue_locations_file):
    """Write venue locations dictionary to the csv file and upload it to S3"""
    # Write venue location dictionary to csv file
    write_dict_to_csv(venue_dict, venue_locations_file)
    # Upload new venue location data
    upload_venue_locations(s3, venue_locations_file)

def main():
    """Try to find missing location data for all venues in S3 venue list"""
    # Temp directory for csv
    venue_locations_file = '/tmp/venue_locations.csv'

    # Create client for interfacing with S3
    s3 = create_client(aws_access_key_id, aws_secret_access_key)

    # Get list of all unique venues from S3
    venue_list = get_venue_list(s3)

    # Download existing venue location data csv
    download_venue_locations(s3, venue_locations_file)

    # Convert venue location data to a dictionary indexed by venue name
    venue_dict = read_csv_to_dict(venue_locations_file)
    
    foursquare_available = True
    time_at_last_backup = time.time()
    try: # Get data for each venue
        for venue in venue_list:
            already_slept = False
            # venue[0] = venue name
            # venue[1] = Untappd checkin link mentioning venue
            # Check if new venue or venue is missing Untappd data
            if venue[0] not in venue_dict or not venue_dict[venue[0]][0]:
                # Get Untappd and Foursquare venue urls and venue coordinates
                venue_dict[venue[0]] = [*search_untappd(venue[1]), '', '']
                if not venue_dict[venue[0]][0]: # Untappd rejected request
                    break # Stop script
                # Sleep about 4 seconds to prevent Untappd rate limiting
                time.sleep(4 + random.uniform(-1, 1))
                already_slept = True # Set flag for Foursquare request
            # Check if venue has Untappd data but is missing Foursquare data
            if (foursquare_available and venue_dict[venue[0]][1]
                and '' in venue_dict[venue[0]]):
                # Get venue address, coordinates, categories, and in_us flag
                venue_dict[venue[0]][-5:] = search_foursquare(venue[0],
                                                venue_dict[venue[0]],
                                                foursquare_client_id,
                                                foursquare_client_secret)
                if '' in venue_dict[venue[0]][-5:]:
                    # Foursquare rejected request, stop searching Foursquare
                    foursquare_available = False
                if not already_slept: # Don't sleep if already slept for Untappd
                    time.sleep(0.75) # Sleep to stay under hourly API call limit
            # Occasionally write data to database for large batches
            # Lambda limits execution time to 15 minutes, backup every 14.75 min
            if time.time() - time_at_last_backup >= 14 * 60 + 45:
                backup_data(s3, venue_dict, venue_locations_file)
                time_at_last_backup = time.time()
    except KeyboardInterrupt:
        pass # Allow manually stopping script without losing data

    # Write updated data back to S3
    backup_data(s3, venue_dict, venue_locations_file)

def lambda_handler(event, context):
    main()

if __name__ == '__main__':
    main()
