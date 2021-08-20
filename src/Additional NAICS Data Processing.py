#!/usr/bin/env python
# coding: utf-8

# # Additional NAICS Data Processing

# **By: Calvin Chen and Irene Tang**

# This file is for adding additionally features to the generated NAICS data from the `NAICS Code Data Generation` notebook. Feel free to add on here if there are other features you'd like to analyze in junction with previously generated NAICS Salesforce dataset.

import numpy as np
import pandas as pd
import re
import json
import time
import requests


# ---

# ## Add Lat/Long Information to Data

# This script is to generate lat/long information to the collected Salesforce information via `ArcGIS`'s `batch_geocode` functionality.

# ### Credentials Helpers


def get_arcgis_access_token(client_id: str, client_secret: str) -> str:
    """
    Gets a new ArcGIS access token.
    """
    token = requests.get("https://www.arcgis.com/sharing/oauth2/token?client_id={}&grant_type=client_credentials&client_secret={}&f=pjson".format(client_id, client_secret))
    return token.json()['access_token']

def get_credentials(filename):
    """
    Getting credentials from given file.
    """
    with open(filename, 'r') as f:
        credentials = json.load(f)
        return credentials

def refresh_access_token(filename) -> None:
    """
    Writes the new passed in access token into the credentials file.
    """
    # Getting a new access token and writing it into `credentials.txt`
    credentials = get_credentials(filename)
    
    if filename == '../credentials/sf_credentials.txt':
        new_token = get_sf_access_token(credentials['client_id'], credentials['client_secret'], credentials['refresh_token'])
    elif filename == '../credentials/arcgis_credentials.txt':
        new_token = get_arcgis_access_token(credentials['client_id'], credentials['client_secret'])
    credentials['access_token'] = new_token
    
    # Writing new credentials back into `credentials.txt`
    with open(filename, 'r+') as f:
        json.dump(credentials, f, ensure_ascii=False)
    return credentials


# ### Data Extraction

# Read in generated data from previous notebook
all_data = pd.read_csv('../data/all_data.csv')

def format_data(all_data: pd.DataFrame) -> list:
    """
    Formatting the data from the `all_data` dataframe into the correct format for the request payload.
    
    End payload to look like:
    [{'attributes': {'OBJECTID': 1,
       'Address': '25000 Avenue Stanford Suite 117',
       'City': 'Valencia',
       'Region': 'CA'}},
     {'attributes': {'OBJECTID': 2,
       'Address': '1 World Way',
       'City': 'Los Angeles',
       'Region': 'CA'}}]
    """
    # Making temp dataframe
    reduced_and_relabeled = (
        all_data[[
            'Account__r.BillingStreet', 
            'Account__r.BillingCity', 
            'Account__r.BillingState'
        ]]
        .rename(columns={
            'Account__r.BillingStreet': 'Address', 
            'Account__r.BillingCity': 'City', 
            'Account__r.BillingState': 'Region'
        })
        .drop_duplicates()
        # Getting an index column to link different objects before and after request is made
        .reset_index(drop=True)
        .reset_index()
        .rename(columns={
            'index': 'OBJECTID'
        })
    )
    
    # Finalizing dataframe attributes
    spatial_data = [
        {'attributes': val} 
            for val in list(
                reduced_and_relabeled
                # Only including certain characters in addresses (removing most special characters)
                .assign(Address=reduced_and_relabeled.Address.apply(lambda address: re.sub(r"[^a-zA-Z0-9. ]", "", address)))
                .T
                .to_dict()
                .values()
    )]

    return reduced_and_relabeled, spatial_data


def chunks(l, n):
    """
    Splits list into n-sized chunks and each chunk into the following format:
    
    {'records': [{'attributes': {'Address': '25000 Avenue Stanford Suite 117',
            'City': 'Valencia',
            'Region': 'CA'}},
        {'attributes': {'Address': '1 World Way',
            'City': 'Los Angeles',
            'Region': 'CA'}}]}
    """
    n = max(1, n)
    return [{'records': l[i:i+n]} for i in range(0, len(l), n)]


# Getting geospatial data
reduced_df, spatial_data = format_data(all_data)

# Geocoding addresses in batches of 100 (limit by URI length)
spatial_data_chunks = chunks(spatial_data, 100)


def generate_spatial_data(df_chunks, to_json=False):
    """
    Adds spatial data information to the `all_data` dataframe generated from earlier.
    """
    def check_error(data):
        """
        Check if the current data payload has an error.
        """
        # Check if the `access_token` worked. If not, refresh this call with a new access token.
        if 'error' in data:
            # Refresh the access token
            print("HTTP Request Access Token {} Error:".format(data['error']['code'], data['error']['message']))
            print("Trying to resolve issue by refreshing access token...")

            # Refreshing access token in credentials
            refresh_access_token('../credentials/arcgis_credentials.txt')
            return True
        
        return False
    
    # Getting credentials
    credentials = get_credentials('../credentials/arcgis_credentials.txt')
    
    # Making requests and appending locations to overall location list
    all_locations = []
    for chunk_idx in range(len(df_chunks)):
        start = time.time()
        print("Currently on chunk:", chunk_idx)
        res = requests.get("https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/geocodeAddresses?addresses={}&token={}&f=pjson".format(spatial_data_chunks[chunk_idx], credentials['access_token']))
        data = res.json()
        
        # Check if the data has an error
        if check_error(data):
            # Trying to make request again with new access token if there is an error
            return generate_spatial_data(df_chunks, to_json)
            
        all_locations.extend(data['locations'])
        finish = time.time()
        print("Request finished. Chunk took", finish - start, 'seconds.')
        
    # Exporting result object into JSON if desired.
    if to_json:
        full_json = json.dumps(all_locations)
        json_file = open('../data/arcgis_latlong_data.json', 'w')
        json_file.write(full_json)
        json_file.close()
        
    return all_locations


# Getting location data
locations = generate_spatial_data(spatial_data_chunks, to_json=True)


def add_loc_data(reduced_df, all_data, to_csv=False):
    """
    Adds location data generated by `generate_spatial_data` to the `all_data` dataframe in a new `final_df`.
    """
    # Load in the data
    file = open('../data/arcgis_latlong_data.json')
    data = json.load(file)
    
    # Creating a mapping between 'OBJECTID' and 'Latitude' and 'Longitude' coordinates.
    temp_mapping = {}
    for obj in data:
        object_id, status = obj['attributes']['ResultID'], obj['attributes']['Status']
        temp_mapping[object_id] = obj['location'] if status != 'U' else {'x': np.nan, 'y': np.nan}

    # Sorting the keys by ID so inserting into the dataframe will be easy.
    coords_mapping = dict(sorted(temp_mapping.items()))
    
    # Creating location mapping dataframe
    loc_mapping = reduced_df.copy(deep=True)
    loc_mapping['Latitude'] = [coord['y'] for coord in list(coords_mapping.values())]
    loc_mapping['Longitude'] = [coord['x'] for coord in list(coords_mapping.values())]
    loc_mapping['Full Address'] = loc_mapping['Address'] + ',' + loc_mapping['City'] + ',' + loc_mapping['Region']
    loc_mapping = loc_mapping[['Full Address', 'Latitude', 'Longitude']]
    
    # Creating `full_data` dataframe with `Full Address` w/o NaN values
    full_data = all_data.copy(deep=True)
    full_data['Full Address'] = full_data['Account__r.BillingStreet'] + ',' + full_data['Account__r.BillingCity'] + ',' + full_data['Account__r.BillingState']
    full_data = full_data[full_data['Full Address'].notnull()]
    
    # Merging data and removing NaN's in lat/long
    full_spatial_data = full_data.merge(loc_mapping, how='inner', left_on='Full Address', right_on='Full Address')
    final_df = full_spatial_data[full_spatial_data.Latitude.notnull() & full_spatial_data.Longitude.notnull()]
    
    # Adding country column (just U.S. for now)
    final_df['Country'] = 'United States'
    
    # Exporting dataframe to CSV if desired
    if to_csv:
        final_df.to_csv('../data/data_with_latlong.csv', index=False)
        
    return final_df


# Generating final dataframe with location data (lat, long) attached to each awarded opportunity.
final = add_loc_data(reduced_df, all_data, to_csv=True)


# ---

# ## **Adding a category column instead of separate `DBE`, `MBE`, and `WBE` columns (to overlay plots at the same time on Carto).**


# Read in data
arcgis_df = pd.read_csv('../data/data_with_latlong.csv')


# Join columns together
arcgis_df['Category'] = arcgis_df[['DBE__c', 'MBE__c', 'WBE__c']].apply(
    lambda x: ','.join(x.dropna().astype(str)),
    axis=1
)


def find_category_name(booleans):
    """
    Find the category name for the list of booleans.
    """
    vals = list(map(lambda val: val == 'True', booleans.split(',')))
    if all(vals):
        return 'DBE, MBE, and WBE'
    elif vals[0] and vals[1]:
        return 'DBE and MBE'
    elif vals[1] and vals[2]:
        return 'MBE and WBE'
    elif vals[0] and vals[2]:
        return 'DBE and WBE'
    elif vals[0]:
        return 'DBE'
    elif vals[1]:
        return 'MBE'
    elif vals[2]:
        return 'WBE'
    elif (not vals[0] and not vals[1] and not vals[2]):
        return 'Not DBE, MBE, WBE'
    

# Find category name for each row
arcgis_df['Category'] = arcgis_df['Category'].apply(find_category_name)


# Export dataframe
arcgis_df.to_csv('../data/data_with_latlong_and_cat.csv', index=False)

