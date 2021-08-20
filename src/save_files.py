import pandas as pd
import pygsheets

# Export dataframe to sheet if `to_sheet` is true
def save_to_gsheet(df: pd.DataFrame, file_name: str, sheet_index: int):

    # Get authorization from `google_credentials.json` file.
    gc = pygsheets.authorize(client_secret='../credentials/google_credentials.json')

    # Open the sheet (replace 'Testing Stuff' with correct file name.)
    sh = gc.open(file_name)

    # Select first sheet
    wks = sh[sheet_index]

    # Update the sheet with the dataframe
    wks.set_dataframe(df, (1, 1))
