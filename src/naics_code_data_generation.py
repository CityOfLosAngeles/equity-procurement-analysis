#!/usr/bin/env python
# coding: utf-8

# # NAICS Code Data Generation

# This notebook includes the complete code of how to re-generate the `Getting NAICS Codes w/o Proprietary Departments` NAICS codes analysis sheet. Determines NAICS codes used by the city, number of opportunities, DBE/MBE/WBE breakdowns, and category breakdowns all by each NAICS code used by # of opp. frequency.

# **By: Calvin Chen and Irene Tang**

import numpy as np
import pandas as pd
import requests
import collections
import json
import itertools
import re
import pygsheets
import warnings
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)


def get_http(ep):
    """
    Gets the initial http domain for HTTP requests.
    """
    return 'https://lacity.my.salesforce.com' + ep


def soql_to_python(soql_statement):
    """
    Converts a SOQL query into its Python-form for making a request.
    """
    return '+'.join(soql_statement.split(' '))


def python_to_soql(python_statement):
    """
    Converts a Python SOQL query back into its SOQL form.
    """
    return ' '.join(python_statement.split('+'))


def get_endpoint(query):
    """
    Generates the endpoint for Salesforce given the passed in SOQL query.
    """
    return '/services/data/v51.0/query/?q=' + query


def make_simple_request(endpoint, full_url=True):
    """
    Makes the request to Salesforce for the given endpoint.
    """
    payload={}
    headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ***REMOVED***',
      'Cookie': 'BrowserId=IoTFrthNEeuXEXtWwwGMWA; CookieConsentPolicy=0:0'
    }

    # Making the request
    url = endpoint if full_url else get_http(endpoint)
    res = requests.request("GET", url, headers=headers, data=payload)
    return res.json()


def remove_a_key(d: dict, remove_key: str) -> dict:
    """
    Removes a key passed in as `remove_key` from a dictionary `d`.
    Reference: https://stackoverflow.com/questions/58938576/remove-key-and-its-value-in-nested-dictionary-using-python
    """
    if isinstance(d, dict):
        for key in list(d.keys()):
            if key == remove_key:
                del d[key]
            else:
                remove_a_key(d[key], remove_key)
    return d


def flatten(d, parent_key='', sep='.'):
    """
    Flattens a dictionary.
    Source: https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


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
    # Getting a new access token and writing it into credentials file.
    credentials = get_credentials(filename)
    
    if 'sf_credentials.txt' in filename:
        new_token = get_sf_access_token(credentials['client_id'], credentials['client_secret'], credentials['refresh_token'])
    elif 'arcgis_credentials.txt' in filename:
        new_token = get_arcgis_access_token(credentials['client_id'], credentials['client_secret'])
    credentials['access_token'] = new_token
    
    # Writing new credentials back into `credentials.txt`
    with open(filename, 'r+') as f:
        json.dump(credentials, f, ensure_ascii=False)
    return credentials


def get_sf_access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    """
    Gets a new Salesforce access token.
    """
    token_url = get_http('/services/oauth2/token')
    payload='client_id={}&client_secret={}&redirect_uri=https%3A%2F%2Fjwt.ms&grant_type=refresh_token&refresh_token={}'.format(client_id, client_secret, refresh_token)
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Cookie': 'BrowserId=IoTFrthNEeuXEXtWwwGMWA; CookieConsentPolicy=0:0'
    }
    response = requests.request("POST", token_url, headers=headers, data=payload)
    return response.json()['access_token']


def create_df_from_req(data) -> pd.DataFrame:
    """
    Creates a dataframe from the passed in data from a former API request.
    """
    records = []
    for r in data['records']:
        # Converts multi-level dict into a single-level and removes 'attributes' keys.
        converted = flatten(remove_a_key(r, 'attributes'))
        records.append(converted)
    return pd.DataFrame(records)


def make_request_and_transform(endpoint, full_url=True, full_data=False):
    """
    Makes the request to Salesforce for the given endpoint.
    Can return all the objects or a subset given from Salesforce.
    """
    # Getting credentials
    credentials = get_credentials('../credentials/sf_credentials.txt')
    
    payload={}
    headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer {}'.format(credentials['access_token']),
      'Cookie': 'BrowserId=IoTFrthNEeuXEXtWwwGMWA; CookieConsentPolicy=0:0'
    }

    # Making the request
    url = endpoint if full_url else get_http(endpoint)
    res = requests.request("GET", url, headers=headers, data=payload)
    data = res.json()
    
    # Checking for error messages
    if isinstance(data, list):
        if 'errorCode' in data[0].keys():
            print("HTTP Request Access Token Error:", data[0]['errorCode'])
            print("Trying to refresh access token...")
            
            # Refreshing access token in credentials
            refresh_access_token('../credentials/sf_credentials.txt')
            
            print("Refreshed access token. Re-trying request.")
            
            # Trying request again with new access token.
            return make_request_and_transform(endpoint, full_url, full_data)
        
    # Formatting data
    df = create_df_from_req(data)

    # Pulling all the data from Salesforce if requested.
    if full_data:
        while not data['done']:
            url = get_http(data['nextRecordsUrl'])
            res = requests.request("GET", url, headers=headers, data=payload)
            data = res.json()
            new_df = create_df_from_req(data)
            df = pd.concat([df, new_df], ignore_index=True)
            try:
                print("Finished batch request! Currently on row {}".format(data['nextRecordsUrl'].split('-')[-1]))
            except KeyError:
                continue
    return df


def get_data_from_soql(soql_query: str, full_data: bool=False) -> pd.DataFrame:
    """
    Gets the data into a Pandas DataFrame from the given SOQL query.
    'full_data' determines if all of the objects are retrieved or not.
    """
    return make_request_and_transform(get_http(get_endpoint(soql_to_python(soql_query))), full_data=full_data)


### Extracting SOQL statements from Salesforce workbench
awards_soql = 'SELECT Account__r.BillingStreet,Account__r.BillingPostalCode,Account__r.BillingCity,Account__r.BillingState,Account__r.Name,Award_Amount__c,Contract_Award_ID__c,DBE__c,MBE__c,WBE__c,Opportunity__r.Id,Opportunity__r.Name FROM Award__c WHERE Opportunity__r.Active__c = true'
opp_naics_soql = 'SELECT Opportunity__r.Id,Opportunity__r.Account.Name,NAICS_Code__r.Name,NAICS_Code__r.NAICS_Description__c,Opportunity__r.Category__c,Opportunity__r.Bid_Due__c,Opportunity__r.Bid_Post__c FROM NAICS_Opportunity__c WHERE Active_YN__c = 1 AND Opportunity__r.Bid_Due__c > 2016-07-01T00:00:00.000Z'
# acc_naics_soql = 'SELECT Account__c,NAICS_Code__r.Name FROM NAICS_Account__c WHERE Active__c = true'


# ## Joining tables
awards = get_data_from_soql(awards_soql, full_data=True)
opp_naics = get_data_from_soql(opp_naics_soql, full_data=True)
# acc_naics = get_data_from_soql(acc_naics_soql, full_data=True)


merged_data = (
    awards
    .merge(opp_naics, left_on='Opportunity__r.Id', right_on='Opportunity__r.Id')
    .rename(columns={'NAICS_Code__r.Name': 'Opportunity_NAICS'})
#   These two columns are to check the account's NAICS codes as well-- may be useful for the future?
#   Currently, this overmerges data and does not follow the Salesforce report.
#     .merge(acc_naics, left_on='Account__c', right_on='Account__c')
#     .rename(columns={'NAICS_Code__r.Name': 'Account_NAICS'})
)

# Removing rows with DWP, LAWA, and POLA data to unskew data
all_data = merged_data[~merged_data['Opportunity__r.Account.Name'].isin(['Water & Power', 'Airports, Los Angeles World', 'Harbor Department, Port of Los Angeles'])]
all_data.drop(columns=['Opportunity__r.Account'], inplace=True)
all_data.head()

# Exporting `all_data` to CSV to be used in a different script
all_data.to_csv('../data/all_naics_data.csv', index=False)

# Removing repeated rows split up by NAICS code by removing NAICS code and dropping duplicates
all_amount_data = all_data.drop(columns=['Opportunity_NAICS', 'NAICS_Code__r.NAICS_Description__c']).drop_duplicates()
all_amount_data.to_csv('../data/all_data.csv', index=False)


def data_to_business_enterprise(data):
    """
    Extracts the desired DBE, MBE, WBE information from the dataframe to a CSV to be uploaded to a Google Sheet.
    """
    types = ['DBE__c', 'MBE__c', 'WBE__c']
    functions = [
        # Getting % of contracts award to DBE, MBE, WBEs (by dollar amount) by group
        lambda group_type: lambda group: (group['Award_Amount__c'] * group[group_type]).sum() / group['Award_Amount__c'].sum() * 100,
        # Getting % of awarded contracts given to DBE, MBE, WBEs (by number of contracts) by group
        lambda group_type: lambda group: group[group_type].sum() / len(group)  * 100,
        # Getting the total number of awards given to DBE, MBE, WBE contractors by group
        lambda group_type: lambda group: group[group_type].sum(),
    ]
    
    # Creating all the functions to aggregate the data with
    all_funcs = [pair[1](pair[0]) for pair in list(itertools.product(types, functions))]
    
    # Constructing the entire dataframe
    total_data = []
    for func in all_funcs:
        total_data.append(all_data.groupby(['Opportunity_NAICS', 'NAICS_Code__r.NAICS_Description__c']).apply(func))
    total_data = pd.concat(total_data, axis=1)
    
    # Renaming + re-ordering columns
    total_data.columns = [
        '% Dollars awarded to DBEs',
        '% Contracts awarded to DBEs',
        'Total Number of Awards Given to DBE Contractors', 
        '% Dollars awarded to MBEs',
        '% Contracts awarded to MBEs',
        'Total Number of Awards Given to MBE Contractors', 
        '% Dollars awarded to WBEs',
        '% Contracts awarded to WBEs',
        'Total Number of Awards Given to WBE Contractors', 
    ]
    total_data = total_data[[
        '% Dollars awarded to DBEs',
        '% Dollars awarded to MBEs',
        '% Dollars awarded to WBEs',
        '% Contracts awarded to DBEs',
        '% Contracts awarded to MBEs',
        '% Contracts awarded to WBEs',
        'Total Number of Awards Given to DBE Contractors', 
        'Total Number of Awards Given to MBE Contractors', 
        'Total Number of Awards Given to WBE Contractors',
    ]]
    
    # Formatting data + filling null values.
    total_data = (
        total_data
        .reset_index(drop=False)
        .rename(columns={
            'NAICS_Code__r.NAICS_Description__c': 'NAICS Industry Name (6-digit)'
        })
        .fillna('No award amount for this NAICS code.')
    )
    
    return total_data


def data_to_category_counts(df):
    """
    Extracts opportunity category counts for each NAICS code.
    """
    return (
        df.groupby(['Opportunity_NAICS', 'Opportunity__r.Category__c'])
        .count()
        .iloc[:,0]
        .to_frame()
        .rename(columns={df.columns[0]: 'Count'})
        .reset_index()
        .pivot(index='Opportunity_NAICS', columns='Opportunity__r.Category__c', values='Count')
        .fillna(0)
        .rename(columns={
            'Commodity': 'Commodity Count', 
            'Construction': 'Construction Count', 
            'Personal Services': 'Personal Services Count'
        })
        .reset_index()
    )


def data_to_naics_opp_counts(opp_naics_df):
    """
    Takes the NAICS_Opportunity dataframe and gets the total opportunity count by NAICS code.
    """
    filtered_df = opp_naics_df[~opp_naics_df['Opportunity__r.Account.Name'].isin([
        'Water & Power', 
        'Airports, Los Angeles World', 
        'Harbor Department, Port of Los Angeles'
    ])]
    return (
        filtered_df
        .groupby(['NAICS_Code__r.Name'])
        .count()
        .sort_values(by='Opportunity__r.Id', ascending=False)
        .reset_index()
        [['NAICS_Code__r.Name', 'Opportunity__r.Id']]
        .rename(columns={
            'NAICS_Code__r.Name': 'Opportunity_NAICS',
            'Opportunity__r.Id': 'Number of Opportunities'
        })
    )


def data_to_sheet(all_data_df, opp_naics_df, to_csv=False, to_sheet=False):
    """
    Converts inputted dataframe into final Google Sheet.
    """
    # Converting and merging dataframes
    counts_df = data_to_naics_opp_counts(opp_naics_df)
    bus_enterprise_df = data_to_business_enterprise(all_data)
    cat_counts_df = data_to_category_counts(all_data)
    temp = bus_enterprise_df.merge(cat_counts_df, left_on='Opportunity_NAICS', right_on='Opportunity_NAICS')
    final_df = (
        temp
        .merge(counts_df, left_on='Opportunity_NAICS', right_on='Opportunity_NAICS', how='outer')
        .sort_values(by='Number of Opportunities', ascending=False)
        .fillna('No award info for NAICS code.')
        .reset_index(drop=True)
    )
    
    # Adding two columns and organization NAICS columns
    final_df['Opportunity_NAICS_2'] = final_df['Opportunity_NAICS'].apply(lambda row: row[:2])
    final_df.rename(columns={'Opportunity_NAICS': 'Opportunity_NAICS_6'}, inplace=True)
    
    # Doing final ordering of columns
    begin_col = ['Opportunity_NAICS_2', 'Opportunity_NAICS_6', 'Number of Opportunities']
    column_order = begin_col + [col for col in final_df.columns if col not in begin_col]
    final_df = final_df[column_order]
    
    # Removing unwanted rows and only listing rows with 100 opportunities or above
    legacy_naics = final_df[final_df['Opportunity_NAICS_6'] == '999999']
    if legacy_naics.shape[0] == 1:
        final_df.drop(index=legacy_naics.index, inplace=True)
    
    final_df = final_df[final_df['Number of Opportunities'] >= 100]
    
    # Creating CSV if `to_csv` is true
    if to_csv:
        final_df.to_csv('../data/naics_code_analysis.csv', index=False)
    
    # Export dataframe to sheet if `to_sheet` is true
    if to_sheet:
        
        # Get authorization from `google_credentials.json` file.
        gc = pygsheets.authorize(client_secret='../credentials/google_credentials.json')

        # Open the sheet (replace 'Testing Stuff' with correct sheet name.)
        sh = gc.open('Procurement Data')

        # Select first sheet
        wks = sh[0]

        # Update the sheet with the dataframe
        wks.set_dataframe(final_df,(1,1))
    
    # Returning final dataset
    return final_df


# Adding generated Salesforce data to Google Sheet
data = data_to_sheet(all_data, opp_naics, to_csv=False, to_sheet=False)

