import urllib.request

import numpy as np
import pandas as pd
import sqlalchemy as sq
import tabula


def retrieve_pdf(url, output_filename):
    """
    Function to go to a website, extract a document(e.g. PDF) and save it onto the machine for future use
    :param url: URL of the document to fetch
    :param output_filename: the name of the file to dump onto the machine
    :return: An excel file of the data retrieved
    """
    user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
    headers = {'User-Agent': user_agent, }
    request = urllib.request.Request(url, None, headers)  # The assembled request
    response = urllib.request.urlopen(request)
    file = open(output_filename, 'wb')
    file.write(response.read())
    file.close()


def generate_zurich_flight_numbers(pdf_input_filename):
    """
    Based on pdf, extracts the tables from the document and then extracts the data from the document.
    :param pdf_input_filename: The location of where the pdf file has been placed
    :return: a dataframe with the numbers of dpeartures and arrivals per day
    """
    # Begins by reading the pdf document with some help of where the tables are "template"
    df = tabula.read_pdf_with_template(input_path=pdf_input_filename, template_path="../reference/ZH_flights_template.json", pages='all', stream=True)
    flights = pd.DataFrame()
    for dfs in df:  # Loops through all the tables found from the PDF
        dfs = dfs.filter(['Unnamed: 0', 'Anflüge', 'Anflüge Abflüge', 'Abflüge'])  # We're only interested in the departures/ arrivals
        for x in range(0, 2):
            for i in dfs.columns[1:]:
                try:
                    dfs[[i, i + ".1"]] = dfs[i].str.split(' ', 1, expand=True)  # DQ Resolution - Sometimes the departures & arrivals are merged into 1 column, this splits them
                except:
                    pass  # Somtimes not, so then here it just ignores it
        dfs.columns = ["date", "Departures", "Arrivals"]
        flights = flights.append(dfs)  # Adds the months data to the dataframe
    return flights


def flights_dirtify(flights):
    """
    Function to make the data evener dirtier
    :param flights: a "sem-clean" dataframe
    :return: a "dirty" dataframe"
    """
    flights = flights[flights.index % 10 != 0]  # Removes every 10th record
    #flights.iloc[::50, :].loc[:, "Departures"] = flights.iloc[::50, :].loc[:, "Departures"].astype(int) * 5  # Changes every 50th record's departures number to be multiplied by 5
    #flights.iloc[::45, :].loc[:, "Arrivals"] = flights.iloc[::45, :].loc[:, "Arrivals"].astype(int) * -1  # Changes every 50th records's arrivals number to be multiplied by 5
    flights = flights.reset_index(drop=True)
    flights['Departures'] = np.where(flights.index % 60 == 0, flights["Departures"].astype(int) * 10, flights["Departures"])
    flights['Arrivals'] = np.where(flights.index % 55 == 0, flights["Arrivals"].astype(int) * -5, flights["Arrivals"])
    flights.loc[::65, "Departures"] = np.NaN
    flights.to_csv("../data/output/StudentA_SourceB_ZH_Airport_Flights_dirty.csv", index=False)
    return flights


def flights_cleanse(flights):
    """
    Function to cleanse the data after artificially making dirty
    :param flights: a "dirty" dataframe
    :return: a "clean" dataframe"
    """
    flights = flights[flights.columns[:3]]  # DQ Resolution - We only want the first 3 columns
    flights.columns = ["date", "Departures", "Arrivals"]  # DQ Resolution - Gives the headings the correct name
    # DQ Resolution - makes sure all the dataytpes are correct
    flights["date"] = pd.to_datetime(flights["date"], format="%d.%m.%y")
    flights = clean_monthly_average(flights)  # Adds the monthly mean for missing records
    flights["company_name"] = "Flughafen Zürich AG"  # DQ Resolution - ensure the key is available
    flights["Departures"] = flights["Departures"].astype(int)
    flights["Arrivals"] = flights["Arrivals"].astype(int)
    flights["flights"] = flights["Departures"].astype(int) + flights["Arrivals"].astype(int)  # Merges to give the total number

    return flights


def remove_extreme_values(df):
    """
    Removes any values that are negative, greater than 1 stand dev from the mean, or is NanN
    :param df: a df with dirty records
    :return: a cleaer df with records removed
    """
    df = df[~df["Departures"].isna()] # Removes any NaN's, we'll calculate them again later
    df.loc[:, "Departures"] = df.loc[:, "Departures"].astype(int)
    df = df[df["Departures"] > 0]
    df = df[df["Departures"] < df["Departures"].std()*2 + df["Departures"].mean()]  # We only want values that are less than 1 standard deviation above the mean
     # We can't have negative flights, so remove them. We'll calculate them again later

    df = df[~df["Arrivals"].isna()]  # Removes any NaN's, we'll calculate them again later
    df["Arrivals"] = df["Arrivals"].astype(int)
    df = df[df["Arrivals"] > 0]  # We can't have negative flights, so remove them. we'll calculate them again later
    df = df[df["Arrivals"] < df["Arrivals"].std()*2 + df["Arrivals"].mean()]  # We only want values that are less than 1 standard deviation above the mean

    return df


def clean_monthly_average(df):
    """
    Takes a df with every 10th record missing, finds these records (could be more) and then adds a record back with the monthly mean
    :param df: a dirty df with every 10th record removed
    :return: a cleaer df with every 10th record added back in with the monthly mean
    """
    df = remove_extreme_values(df)
    df["date"] = pd.to_datetime(df["date"], infer_datetime_format=True)
    # Identifies any missing date records, by taking the min and max and then using the difference to find any daily records
    missing = pd.date_range(start=df["date"].min(), end=df["date"].max()).difference(df["date"])
    missing = pd.DataFrame({"date": missing})
    missing["missing_month"] = missing["date"].dt.year.astype(str) + "-" + +missing["date"].dt.month.astype(str).apply(lambda x: '{0:0>2}'.format(x))

    # generates a monthly average, that we can use to fill in the missing values
    monthly = df.groupby(pd.PeriodIndex(df['date'], freq="M")).mean()  # Generates monthly means
    monthly = monthly.round(0).astype(int)  # rounds as converts to int
    monthly = monthly.reset_index()
    monthly["date"] = monthly["date"].astype(str)

    missing = missing.merge(monthly, how="left", left_on="missing_month", right_on="date")  # Merges the missing values with the monthly means
    missing = missing.rename(columns={"date_x": "date"})
    missing = missing[["date", "Departures", "Arrivals"]]  # Removes other date columns we no longer need

    df = df.append(missing, ignore_index=True)  # Appends the imputed records with the mean to the main DF
    df["date"] = pd.to_datetime(df["date"],infer_datetime_format=True)  # Ensures that the datetime format is correct
    #df = df.sort_values(by="date", ascending=True)
    return df


def main():
    url = "https://www.flughafen-zuerich.ch/-/jssmedia/airport/portal/dokumente/das-unternehmen/politics-and-responsibility/noise-and-sound-insulation/monatliche-flugbewegungen_2108.pdf?vs=1"
    output_filename = "../data/output/StudentA_SourceB_ZH_Airport_Flights_src.pdf"
    retrieve_pdf(url, output_filename)  # First get the pdf file from the website
    flights = generate_zurich_flight_numbers(output_filename)
    flights = flights_dirtify(flights)  # Runs procedure to make the data even dirtier
    flights = flights_cleanse(flights)  # Runs the procedure to cleanse the data
    # Outputs the data to files/ database
    flights.to_csv("../data/output/StudentA_SourceB_ZH_Airport_Flights_stage.csv", index=False)
    engine = sq.create_engine("mysql+mysqlconnector://mark:password@localhost:3306/CIP")
    flights.to_sql(con=engine, name="zurich_flights", if_exists="replace", index=False)


if __name__ == "__main__":
    main()
