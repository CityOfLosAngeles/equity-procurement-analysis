import pandas as pd
import numpy as np
from sodapy import Socrata
import save_files

# set up Socrata client
COUNTY_CLIENT = Socrata("data.lacounty.gov", None)
CITY_CLIENT = Socrata("data.lacity.org", None)

def get_zip_codes():
    """
    Fetches zip code and city names data from LA County Data Portal
    Returns a list of zip codes making up LA City
    Returns a list of zip codes making up LA County (excluding LA City)
    Returns a list of names for cities in LA County (excluding LA City)
    """

    # Fetch information from the LA County data portal
    # Headcount of cities in LA County with their corresponding zip codes
    # !!make sure this is fetching all of them with no limit
    zips = pd.DataFrame.from_records(COUNTY_CLIENT.get("c3xr-3jw2"))

    # preprocess: rename some columns
    zips.rename(columns={"zip_code": "ZIP5"}, inplace=True)

    # preprocess: make all city names lowercase and remove spaces
    zips.postal_city_1 = zips.postal_city_1.apply(
        lambda x: x.strip().lower().replace(' ', ''))

    # Get a list of zip codes belonging to LA City
    city_zips = zips[zips.postal_city_1 == 'losangeles'].ZIP5
    city_zips = city_zips.astype(str)
    city_zips = city_zips.unique()

    # Get a list of zip codes belonging to Greater LA County (excluding LA City)
    county_zips = zips[zips.postal_city_1 != 'losangeles'].ZIP5
    county_zips = county_zips.astype(str)
    county_zips = county_zips.unique()

    # Get a list of names for cities in LA County (excluding LA City)
    county_names = zips[zips.postal_city_1 != 'losangeles'].postal_city_1
    county_names = county_names.astype(str)
    county_names = county_names.unique()

    # return city_zips, county_zips, county_names
    return city_zips, county_zips, county_names


def get_all_business_data():
    """
    Fetches a listing of registered active businesses from LA City Data Portal
    Returns a dataframe of all the businesses
    """
    # fetch List of Active Businesses
    all_biz = pd.DataFrame.from_records(CITY_CLIENT.get("6rrh-rzua"))

    # preprocess: rename some columns
    all_biz.rename(columns={"street_address": "STREET",
                            "city": "CITY",
                            'zip_code': "ZIP9",
                            'naics': "NAICS"
                            }, inplace=True)
    
    # preprocess: if business has no NAICS reported, replace it with 999999 (aka the "legacy" code)
    all_biz.NAICS.fillna('999999', inplace=True)

    # preprocess: make all city names lowercase and remove spaces
    all_biz.CITY = all_biz.CITY.apply(
        lambda x: str(x).strip().lower().replace(' ', ''))

    # add a column for the 5-digit zip code; use 00000 if none is given
    all_biz['ZIP5'] = all_biz.ZIP9.apply(lambda x: x[:5])
    all_biz.ZIP5 = all_biz.ZIP5.replace('-', 000000)

    # preprocess: convert some data types
    all_biz.NAICS = all_biz.NAICS.astype(str)
    all_biz.ZIP5 = all_biz.ZIP5.astype(str, errors='ignore')

    # if a NAICS is fewer than 6 digits, extend it to 6 digits by appending 0s
    all_biz.NAICS = all_biz.NAICS.apply(lambda x: str(x).split('.')[0])
    all_biz.NAICS = all_biz.NAICS.apply(lambda x: str(x) + '0'*(6-len(str(x))))
    all_biz.NAICS = all_biz.NAICS.astype(str)

    # # print some naics info
    # all_biz_counts = all_biz.NAICS.value_counts().to_frame().reset_index()
    # all_biz_counts.columns = ['NAICS', 'all_biz_count']
    # print(all_biz_counts.tail())

    return all_biz


def separate_businesses(all_biz: pd.DataFrame, city_zips, county_zips, county_names):
    """
    Returns a dataframe of registered active businesses in LA City
    Returns a dataframe of registered active businesses in LA County (excluding LA City)
    Returns a dataframe of registered active businesses outside LA County
    """
    # note that there are a lot of typos in the city names
    city_biz = all_biz.loc[(all_biz.ZIP5.astype(str).isin(city_zips) & (~all_biz.CITY.isin(county_names))) | (all_biz.CITY == 'losangeles')]

    county_biz = all_biz.loc[(all_biz.ZIP5.astype(str).isin(county_zips)) | (all_biz.CITY.isin(county_names)) & (all_biz.CITY != 'losangeles')]

    other_biz = all_biz.loc[(~all_biz.ZIP5.isin(city_zips)) & (~all_biz.ZIP5.isin(county_zips)) & (all_biz.CITY != 'losangeles') & (~all_biz.CITY.isin(county_names))]
    # other_biz = pd.concat([all_biz, city_biz, county_biz]).drop_duplicates(keep=False)

    # sanity check
    print(all_biz.size - city_biz.size - county_biz.size - other_biz.size == 0)
    return city_biz, county_biz, other_biz


def get_business_naics_info(city_biz, county_biz, other_biz):
    """
    Returns number of city businesses in each NAICS sector
    Returns number of county businesses in each NAICS sector
    Returns number of other businesses in each NAICS sector
    """
    city_biz_counts = city_biz.NAICS.value_counts().to_frame().reset_index()
    city_biz_counts.columns = ['NAICS', 'city_biz_count']

    county_biz_counts = county_biz.NAICS.value_counts().to_frame().reset_index()
    county_biz_counts.columns = ['NAICS', 'county_biz_count']

    other_biz_counts = other_biz.NAICS.value_counts().to_frame().reset_index()
    other_biz_counts.columns = ['NAICS', 'other_biz_count']

    return city_biz_counts, county_biz_counts, other_biz_counts

def count_opportunities_by_naics(data):
    print(data.columns)

# all_opp = pd.read_csv('/content/drive/MyDrive/Diversity and Procurement Analysis/Scripts and Data/opportunities.csv')
# all_opp.columns

# # preprocessing

# # rename some columns
# all_opp.rename(columns={"NAICS Code: NAICS Code" : "NAICS"}, inplace=True)

# # if business has no NAICS reported, replace it with 999999
# all_opp.NAICS.fillna('999999', inplace=True)

# # convert NAICS type to int
# all_opp.NAICS = all_opp.NAICS.astype(str)

# # if a NAICS is fewer than 6 digits, extend it to 6 digits by appending 0s
# all_opp.NAICS = all_opp.NAICS.apply(lambda x: str(x).split('.')[0])
# all_opp.NAICS = all_opp.NAICS.apply(lambda x: str(x) + '0'*(6-len(str(x))))
# all_opp.NAICS = all_opp.NAICS.astype(str)

# # all_opp.head()
# # all_opp.NAICS.unique().size

# # print some naics info
# all_opp_counts = all_opp.NAICS.value_counts().to_frame().reset_index()
# all_opp_counts.columns = ['NAICS', 'all_opp_count']
# all_opp_counts.head()

# """# Merge opportunities and business counts"""

# merged = all_opp_counts.merge(city_biz_counts, how='outer', on='NAICS')
# merged = merged.merge(county_biz_counts, how='outer', on='NAICS')
# merged = merged.merge(other_biz_counts, how='outer', on='NAICS')

# merged.replace(np.nan, 0, inplace=True)
# merged.all_opp_count = merged.all_opp_count.astype(int)
# merged.city_biz_count = merged.city_biz_count.astype(int)
# merged.county_biz_count = merged.county_biz_count.astype(int)
# merged.other_biz_count = merged.other_biz_count.astype(int)

# merged.sort_values(by='all_opp_count', ascending=False, inplace=True)

# merged.head()

# # more columns
# # merged['opp/city_biz'] = merged.all_opp_count/merged.city_biz_count
# # merged['opp/(city_biz + county_biz)'] = merged.all_opp_count/(merged.city_biz_count + merged.county_biz_count)

# # merged.query("city_biz_count > all_opp_count")


def get_awards_by_location(data, city_zips, county_zips, county_names):
    """
    Returns awards broken down by geographical location 
    """
    awards = data 

    # preprocess: rename some columns
    awards.rename(columns={"Account__r.BillingStreet" : "STREET",
                        'Account__r.BillingPostalCode' : "ZIP5",
                        'Account__r.BillingCity' : "CITY",
                        'Account__r.BillingState' : "STATE",
                    }, inplace=True)

    # preprocess: make all city names lowercase and remove spaces
    awards.CITY = awards.CITY.apply(lambda x: x.strip().lower().replace(' ', ''))


    # awarded in county
    awards_in_city = awards.loc[(awards.ZIP5.astype(str).isin(city_zips) & (~awards.CITY.isin(county_names))) | (awards.CITY == 'losangeles')]
    awards_in_county = awards.loc[(awards.ZIP5.astype(str).isin(county_zips) | (awards.CITY.isin(county_names))) & (awards.CITY != 'losangeles')]
    awards_in_state = awards.loc[(awards.STATE == 'CA') & (~awards.ZIP5.astype(str).isin(county_zips)) & (~awards.ZIP5.astype(str).isin(city_zips)) & (~awards.CITY.isin(county_names)) & (awards.CITY != 'losangeles')]
    awards_out_of_state = awards.loc[(awards.STATE!='CA')]

    # sanity check
    print(len(awards) - len(awards_in_city) - len(awards_in_county) - len(awards_in_state) - len(awards_out_of_state))

    return awards_in_city, awards_in_county, awards_in_state, awards_out_of_state


def count_awards_by_location(awards_in_city, awards_in_county, awards_in_state, awards_out_of_state):
    """
    Returns a tally of how many awards in each region
    """
    df = pd.DataFrame({
        'awards_in_city': awards_in_city.size,
        'awards_in_county': awards_in_county.size,
        'awards_in_state': awards_in_state.size,
        'awards_out_of_state': awards_out_of_state.size
    }, index=[0])

    return df


def count_opportunities_vs_businesses():
    pass
# TODO



def main():
    # Andrew said that counts within 75% accuracy is okay.
    # So the distribution by location is slightly off due to some instances of double-counting
    # See sanity checks throughout this code

    data = pd.read_csv('../data/all_data.csv')

    city_zips, county_zips, county_names = get_zip_codes()
    all_biz = get_all_business_data()
    city_biz, county_biz, other_biz = separate_businesses(all_biz, city_zips, county_zips, county_names)
    city_biz_counts, county_biz_counts, other_biz_counts = get_business_naics_info(city_biz, county_biz, other_biz)

    awards_in_city, awards_in_county, awards_in_state, awards_out_of_state = get_awards_by_location(data, city_zips, county_zips, county_names)
    awards_by_location = count_awards_by_location(awards_in_city, awards_in_county, awards_in_state, awards_out_of_state)

    count_opportunities_by_naics(data)
    count_opportunities_vs_businesses()

    # save awards by location to gsheet
    # save opportunities vs businesses to gsheet




if __name__ == "__main__":
    main()
