# -*- coding: utf-8 -*-
"""business_in_city_or_county.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1vGKCoaKNpE68vS39AMzAVNhunTHyuvzS
"""

import pandas as pd
import numpy as np
!pip install sodapy
from sodapy import Socrata

from google.colab import drive
drive.mount('/content/drive')

# set up Socrata client
client = Socrata("data.lacounty.gov", None)

"""# Fetch zip code dataset"""

# fetch zip : city
zips = pd.DataFrame.from_records(client.get("c3xr-3jw2", limit=999999999))
zips.head()

# preprocess city names
zips.postal_city_1 = zips.postal_city_1.apply(lambda x: x.strip().lower().replace(' ', ''))
zips.postal_city_1

# preprocess zip code data
zips.rename(columns={"zip_code" : "ZIP5"}, inplace=True)

# Separate zip codes: those belonging to LA City
city_zips = zips[zips.postal_city_1=='losangeles'].ZIP5
city_zips = city_zips.astype(str)
city_zips = city_zips.unique()
city_zips

# Separate zip codes: those belonging to Greater LA County
county_zips = zips[zips.postal_city_1!='losangeles'].ZIP5
county_zips = county_zips.astype(str)
county_zips = county_zips.unique()
county_zips

county_names = zips[zips.postal_city_1!='losangeles'].postal_city_1
county_names = county_names.unique()
county_names

for x in city_zips:
    if x in county_zips:
        print(x)

"""# Fetch business data"""

# fetch List of Active Businesses
# biz = pd.DataFrame.from_records(client.get("6rrh-rzua", limit=999999999))

all_biz = pd.read_csv('/content/drive/MyDrive/Diversity and Procurement Analysis/Scripts and Data/Listing_of_Active_Businesses.csv')
all_biz.columns

# preprocess business data

# if business has no NAICS reported, replace it with 999999
all_biz.NAICS.fillna('999999', inplace=True)

# convert NAICS type to int
all_biz.NAICS = all_biz.NAICS.astype(str)

# rename some columns
all_biz.rename(columns={"STREET ADDRESS" : "STREET",
                    'ZIP CODE' : "ZIP9",
                   }, inplace=True)


# preprocess the city name
all_biz.CITY = all_biz.CITY.apply(lambda x: str(x).strip().lower().replace(' ', ''))

# add a column for the 5-digit zip code
all_biz['ZIP5'] = all_biz.ZIP9.apply(lambda x: x[:5])
all_biz.ZIP5 = all_biz.ZIP5.replace('-', 000000)
#all_biz.ZIP5.to_csv('/content/drive/MyDrive/Getting_NAICS_counts/test.csv')


all_biz.ZIP5 = all_biz.ZIP5.astype(str, errors='ignore')

# if a NAICS is fewer than 6 digits, extend it to 6 digits by appending 0s
all_biz.NAICS = all_biz.NAICS.apply(lambda x: str(x).split('.')[0])
# for x in all_biz.NAICS:
#     print(x)
all_biz.NAICS = all_biz.NAICS.apply(lambda x: str(x) + '0'*(6-len(str(x))))
all_biz.NAICS = all_biz.NAICS.astype(str)
# all_biz.head()

# print some naics info
all_biz_counts = all_biz.NAICS.value_counts().to_frame().reset_index()
all_biz_counts.columns = ['NAICS', 'all_biz_count']
all_biz_counts.tail()

"""# Separate businesses into LA city vs. Greater LA County vs. Other"""

# df of LA city businesses

# TODO: accept an accuracy level of 75%

# based on city name
# city_biz = all_biz.loc[all_biz.CITY == 'losangeles']

# based on zip code
city_biz = all_biz.loc[(all_biz.ZIP5.astype(str).isin(city_zips.astype(str))) | (all_biz.CITY == 'losangeles')]
city_biz.CITY.unique()

# df of LA county businesses (excluding LA City)

# based on city name
# county_biz = all_biz.loc[all_biz.CITY.isin(county_names)]

# based on zip code
county_biz = all_biz.loc[all_biz.ZIP5.astype(str).isin(county_zips.astype(str)) & ( (all_biz.CITY != 'losangeles')) ]


# county_biz1.size - county_biz.size
county_biz.CITY.unique()
# NOTE: 5-digit zip codes are less helpful because some span multiple cities -- businesses may get (in)correctly classified as county/city
# FIXME: do we want to just base it on city name instead since the data is already there?

# based on city name
# other_biz = all_biz.loc[(all_biz.CITY!='losangeles') & (all_biz.CITY!='losangeles')]

# based on zip
other_biz = all_biz.loc[(~all_biz.ZIP5.isin(city_zips)) & (~all_biz.ZIP5.isin(county_zips)) & (all_biz.CITY != 'losangeles') & (~all_biz.CITY.isin(county_names))]

# all_biz.size - city_biz.size - county_biz.size - other_biz.size
other_cities = other_biz.CITY.unique()
other_cities.sort()
# for x in other_cities:
#     print(x)

# sanity check
pd.concat([city_biz,county_biz,other_biz, all_biz]).drop_duplicates(keep=False)

# sanity check
all_biz.size - city_biz.size - county_biz.size - other_biz.size

# print some naics info
city_biz_counts = city_biz.NAICS.value_counts().to_frame().reset_index()
city_biz_counts.columns = ['NAICS', 'city_biz_count']
city_biz_counts.head(7)

# print some naics info
county_biz_counts = county_biz.NAICS.value_counts().to_frame().reset_index()
county_biz_counts.columns = ['NAICS', 'county_biz_count']
county_biz_counts.head(7)

# print some naics info
other_biz_counts = other_biz.NAICS.value_counts().to_frame().reset_index()
other_biz_counts.columns = ['NAICS', 'other_biz_count']
other_biz_counts.head()

"""# Read in opportunities
This report was generated in Salesforce. Bid due date 7/1/2011 - 7/15/2021
"""

all_opp = pd.read_csv('/content/drive/MyDrive/Diversity and Procurement Analysis/Scripts and Data/opportunities.csv')
all_opp.columns

# preprocessing

# rename some columns
all_opp.rename(columns={"NAICS Code: NAICS Code" : "NAICS"}, inplace=True)

# if business has no NAICS reported, replace it with 999999
all_opp.NAICS.fillna('999999', inplace=True)

# convert NAICS type to int
all_opp.NAICS = all_opp.NAICS.astype(str)

# if a NAICS is fewer than 6 digits, extend it to 6 digits by appending 0s
all_opp.NAICS = all_opp.NAICS.apply(lambda x: str(x).split('.')[0])
all_opp.NAICS = all_opp.NAICS.apply(lambda x: str(x) + '0'*(6-len(str(x))))
all_opp.NAICS = all_opp.NAICS.astype(str)

# all_opp.head()
# all_opp.NAICS.unique().size

# print some naics info
all_opp_counts = all_opp.NAICS.value_counts().to_frame().reset_index()
all_opp_counts.columns = ['NAICS', 'all_opp_count']
all_opp_counts.head()

"""# Merge opportunities and business counts"""

merged = all_opp_counts.merge(city_biz_counts, how='outer', on='NAICS')
merged = merged.merge(county_biz_counts, how='outer', on='NAICS')
merged = merged.merge(other_biz_counts, how='outer', on='NAICS')

merged.replace(np.nan, 0, inplace=True)
merged.all_opp_count = merged.all_opp_count.astype(int)
merged.city_biz_count = merged.city_biz_count.astype(int)
merged.county_biz_count = merged.county_biz_count.astype(int)
merged.other_biz_count = merged.other_biz_count.astype(int)

merged.sort_values(by='all_opp_count', ascending=False, inplace=True)

merged.head()

# more columns
# merged['opp/city_biz'] = merged.all_opp_count/merged.city_biz_count
# merged['opp/(city_biz + county_biz)'] = merged.all_opp_count/(merged.city_biz_count + merged.county_biz_count)

# merged.query("city_biz_count > all_opp_count")

"""# Sandbox: Read in awards"""

# lower = all_biz.STREET.str.lower()
# lower = lower.str.strip(' .')
# all_biz.STREET = lower
# all_biz.STREET.head()

# awards = pd.read_csv('/content/drive/MyDrive/Getting_NAICS_counts/awards.csv')
# awards.columns

# awards.rename(columns={
#     "Contractor: Billing Street" : "STREET",
#     "Contractor: Billing City" : "CITY",
#     "Contractor: Billing Zip/Postal Code" : "ZIP5"
# }, inplace=True)

# lower = awards.STREET.str.lower()
# lower = lower.str.strip(' .')
# awards.STREET = lower
# awards.STREET

# street_naics_mapping = dict(all_biz[['STREET', 'NAICS']].values)
# print(type(street_naics_mapping))
# for x in list(street_naics_mapping)[0:10]:
#     print (x, street_naics_mapping[x])

# awards['NAICS'] = awards['STREET'].map(street_naics_mapping)
# awards.NAICS = awards['NAICS'].fillna(999999)
# awards.NAICS.unique()

# awards.NAICS.value_counts()

# city_awards = awards.loc[awards.CITY == 'Los Angeles']
# county_awards = awards.loc[awards.CITY != 'Los Angeles']

# city_awards = awards.loc[awards.ZIP5.isin(city_zips)]
# county_awards = awards.loc[awards.ZIP5.isin(county_zips)]
# awarded_to_unknown = awards.loc[~awards.ZIP5.isin(city_zips) & ~awards.ZIP5.isin(county_zips)]

# city_awards.shape

# county_awards.shape

# awarded_to_unknown.shape

# city_awards_mapping = city_awards.NAICS.value_counts().to_dict()
# county_awards_mapping = county_awards.NAICS.value_counts().to_dict()
# awarded_to_unknown_mapping = awarded_to_unknown.NAICS.value_counts().to_dict()

# merged['awarded to city'] = merged.NAICS.map(city_awards_mapping)
# merged['awarded to county'] = merged.NAICS.map(county_awards_mapping)
# merged['award unknown'] = merged.NAICS.map(awarded_to_unknown_mapping)

# merged

"""# Read in awards

"""

awards = pd.read_csv('/content/drive/MyDrive/Diversity and Procurement Analysis/Scripts and Data/data_with_latlong.csv')
awards.columns

# rename some columns
awards.rename(columns={"Account__r.BillingStreet" : "STREET",
                    'Account__r.BillingPostalCode' : "ZIP5",
                    'Account__r.BillingCity' : "CITY",
                    'Account__r.BillingState' : "STATE",
                   }, inplace=True)

awards.columns

# preprocess city names
awards.CITY = awards.CITY.apply(lambda x: x.strip().lower().replace(' ', ''))
# awards.CITY.unique()

zero_award_amount = awards.loc[(awards.STATE!='CA')]

# awarded in county
awards_in_county = awards.loc[awards.ZIP5.astype(str).isin(county_zips.astype(str)) & ( (awards.CITY != 'losangeles')) ]
awards_in_city = awards.loc[(awards.ZIP5.astype(str).isin(city_zips.astype(str))) | (awards.CITY == 'losangeles')]
awards_in_state = awards.loc[(awards.STATE=='CA') & (~awards.ZIP5.astype(str).isin(county_zips.astype(str))) & (~awards.ZIP5.astype(str).isin(city_zips.astype(str)))]
awards_out_of_state = awards.loc[(awards.STATE!='CA')]

print(len(awards_in_city), len(awards_in_county), len(awards_in_state), len(awards_out_of_state))
awards_out_of_state.head()

# sanity check
awards_in_state_all = awards.loc[(awards.STATE=='CA')]

len(awards_in_state_all) - len(awards_in_state) - len(awards_in_city) - len(awards_in_county)

# sanity check
len(awards) - len(awards_in_city) - len(awards_in_county) - len(awards_in_state) - len(awards_out_of_state)

"""# Save file"""

merged.to_csv('/content/drive/MyDrive/Diversity and Procurement Analysis/Scripts and Data/business_counts.csv', index=False)