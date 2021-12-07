import pandas as pd
import pygsheets
import os
from google.oauth2 import service_account
import json

from civis_aqueduct_utils.github import upload_file_to_github

# Constants for loading the file to GitHub
#TOKEN = os.environ["GITHUB_TOKEN_PASSWORD"]
REPO = "CityOfLosAngeles/equity-procurement-analysis"
BRANCH = "main"

DEFAULT_COMMITTER = {
    "name": "Los Angeles ITA data team",
    "email": "ITAData@lacity.org",
}


# Export dataframe to sheet if `to_sheet` is true
def save_to_gsheet(df: pd.DataFrame, file_name: str, sheet_index: int):
    #print(file_name)

    # Get authorization from `google_credentials.json` file.
    #gc = pygsheets.authorize(client_secret='../credentials/google_credentials.json')
    gc = pygsheets.authorize(service_file='../credentials/sa_credentials.json')

    # Open the sheet (replace 'Testing Stuff' with correct file name.)
    sh = gc.open(file_name)
    #print(sh)

    # Select first sheet
    wks = sh[sheet_index]

    # Update the sheet with the dataframe
    wks.set_dataframe(df, (1, 1))
    
    # Upload to GitHub
    '''upload_file_to_github(
        TOKEN,
        REPO,
        BRANCH,
        f"data/{file_name}",
        f"data/{file_name}",
        f"Update {file_name}",
        DEFAULT_COMMITTER,
    )'''