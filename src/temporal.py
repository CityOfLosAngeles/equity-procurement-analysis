import pandas as pd
import save_files

# Read in generated data from previous notebook
all_data = pd.read_csv("../data/all_data.csv")
print(list(all_data.columns))

def get_percents(timeframes: list, timeframe_column: str = "bid_due_year"):
    """
    Returns the % of contracts and % of dollars awarded to DBE, MBE, WBEs
    """
    df = pd.DataFrame({"timeframe": timeframes})

    # looks compact but has a divide by zero issue
    # df["percent_dbe_contracts"] = [100 * (all_data[(all_data.DBE__c == True) & (all_data[timeframe_column] == t)].shape[0])/(all_data[(all_data[timeframe_column] == t)].shape[0]) for t in timeframes]
    # df["percent_mbe_contracts"] = [100 * (all_data[(all_data.MBE__c == True) & (all_data[timeframe_column] == t)].shape[0])/(all_data[(all_data[timeframe_column] == t)].shape[0]) for t in timeframes]
    # df["percent_wbe_contracts"] = [100 * (all_data[(all_data.WBE__c == True) & (all_data[timeframe_column] == t)].shape[0])/(all_data[(all_data[timeframe_column] == t)].shape[0]) for t in timeframes]

    # df["percent_dbe_dollars"] = [100 * (all_data[(all_data.DBE__c == True) & (all_data[timeframe_column] == t)].Award_Amount__c.sum())/(all_data[(all_data[timeframe_column] == t)].Award_Amount__c.sum()) for t in timeframes]
    # df["percent_mbe_dollars"] = [100 * (all_data[(all_data.MBE__c == True) & (all_data[timeframe_column] == t)].Award_Amount__c.sum())/(all_data[(all_data[timeframe_column] == t)].Award_Amount__c.sum()) for t in timeframes]
    # df["percent_wbe_dollars"] = [100 * (all_data[(all_data.WBE__c == True) & (all_data[timeframe_column] == t)].Award_Amount__c.sum())/(all_data[(all_data[timeframe_column] == t)].Award_Amount__c.sum()) for t in timeframes]


    # less-compact but addresses divide by zero issue
    df_contract_columns = ["percent_dbe_contracts", "percent_mbe_contracts", "percent_wbe_contracts"]
    df_dollars_columns = ["percent_dbe_dollars", "percent_mbe_dollars", "percent_wbe_dollars"]
    all_data_columns = ["DBE__c", "MBE__c", "WBE__c"]

    # get percents for number of contracts
    for i, x in enumerate(all_data_columns):
        value = []
        for t in timeframes:
            a = all_data[(all_data[x] == True) & (all_data[timeframe_column] == t)].shape[0]
            b = all_data[(all_data[timeframe_column] == t)].shape[0]
            percent = 100*a/b if b != 0 else 0
            value.append(percent)
        df[df_contract_columns[i]] = value

    # get percents for dollar value of contracts
    for i, x in enumerate(all_data_columns):
        value = []
        for t in timeframes:
            a = all_data[(all_data[x] == True) & (all_data[timeframe_column] == t)].Award_Amount__c.sum()
            b = all_data[(all_data[timeframe_column] == t)].Award_Amount__c.sum()
            percent = 100*a/b if b != 0 else 0
            value.append(percent)
        df[df_dollars_columns[i]] = value

    return df


def main():
    # Generate google sheet for yearly analysis
    years = sorted(all_data.bid_due_year.unique())
    percents_by_year = get_percents(years, "bid_due_year")
    print(percents_by_year)

    # write to google sheet
    save_files.save_to_gsheet(percents_by_year, "Procurement Data New", 3)


    # Generate google sheet for monthly analysis
    months = sorted(all_data.bid_due_year_month.unique())
    percents_by_month = get_percents(months, "bid_due_year_month")
    print(percents_by_month)

    # write to google sheet
    save_files.save_to_gsheet(percents_by_month, "Procurement Data new", 4)


if __name__ == "__main__":
    main()
