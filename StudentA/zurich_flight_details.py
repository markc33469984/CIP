import urllib.request

import pandas as pd
import tabula
import sqlalchemy as sq


def retrieve_pdf(url, output_filename):
    user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
    headers = {'User-Agent': user_agent, }
    request = urllib.request.Request(url, None, headers)  # The assembled request
    response = urllib.request.urlopen(request)
    file = open(output_filename, 'wb')
    file.write(response.read())
    file.close()


def generate_zh_arrivals():
    url = "https://www.flughafen-zuerich.ch/-/jssmedia/airport/portal/dokumente/das-unternehmen/politics-and-responsibility/noise-and-sound-insulation/monatliche-flugbewegungen_2108.pdf?vs=1"
    output_filename = "../data/input/Zurich_Airpot_Flight_numbers.pdf"
    retrieve_pdf(url, output_filename)
    df3 = tabula.read_pdf_with_template(input_path=output_filename, template_path="../reference/ZH_flights_template.json", pages='all', stream=True)
    flights = pd.DataFrame()
    for dfs in df3:
        dfs = dfs.filter(['Unnamed: 0', 'Anflüge', 'Anflüge Abflüge', 'Abflüge'])
        for x in range(0, 2):
            for i in dfs.columns[1:]:
                try:
                    dfs[[i, i + ".1"]] = dfs[i].str.split(' ', 1, expand=True)
                except:
                    pass
        dfs.columns = ["date", "Departures", "Arrivals"]
        flights = flights.append(dfs)
    flights["company_name"] = "Flughafen Zürich AG"
    flights["flights"] = flights["Departures"] + flights["Arrivals"]
    flights.to_csv("../data/output/StudentA_Source_B1.1_clean_ZH_flight_details.csv", index=False)
    engine = sq.create_engine("mysql+mysqlconnector://mark:password@localhost:3306/CIP")
    flights.to_sql(con=engine, name="zurich_flights", if_exists="replace", index=False)


def extract_departures_df(df):
    """

    :param df:
    :return:
    """
    departures = pd.DataFrame()
    for dfs in df:  # Loops through each table extracted from the PDF
        if dfs.columns[0] == "Abflüge":
            ind = False
            dfs = dfs.dropna(axis="columns", how="all")
            if dfs.columns[2] != "Piste 10":
                dfs.columns = dfs.iloc[0]
                dfs = dfs.iloc[2:].reset_index(drop=True)
            else:
                ind = True
            for x in range(0, 2):
                for i in dfs.columns[1:]:
                    try:
                        dfs[[i, i + ".1"]] = dfs[i].str.split(' ', 1, expand=True)
                    except:
                        pass

            dfs = dfs.dropna(axis="columns", how="all")
            if ind:
                dfs.columns = dfs.iloc[1]
                dfs = dfs.iloc[2:].reset_index(drop=True)
            else:
                dfs.columns = dfs.iloc[0]
                dfs = dfs.iloc[1:].reset_index(drop=True)

            dfs.columns.values[0] = "Date"  # Set's the first column to be the date
            dfs = dfs[:dfs.index[dfs["Date"].str.contains('Total', na=False)][0]]  # In some cases, two tables are merged as one. Here we only take until the total
            dfs["Date"] = pd.to_datetime(dfs["Date"].str.split(' ', 1, expand=True)[0], format="%d.%m.%y")  # Removes the day and converts to a datetime objects
            dfs = dfs.set_index("Date")
            dfs = dfs.drop(columns=["Total", "Abflüge"], errors="ignore")

            dfs = dfs.dropna(axis="columns", how="all")
            suff = '_1'
            dfs.columns = [name if duplicated == False else name + suff for duplicated, name in zip(dfs.columns.duplicated(), dfs.columns)]  # Columns N and O are duplicated, here we add a suffix "_1" to these columns
            mapper = {"A": "Piste 10", "B": "Piste 10", "C": "Piste 10", "D": "Piste 10", "E": "Piste 16", "F": "Piste 16", "G": "Piste 16", "I": "Piste 28", "K": "Piste 28", "L": "Piste 28", "N": "Piste 32",
                      "N_1": "Piste 34", "O": "Piste 32", "O_1": "Piste 34"}
            dfs.columns = dfs.columns.map(mapper)  # We remap the column names from the Letter (e.g. A) to the piste number
            dfs = dfs.replace("’", "", regex=True)
            dfs = dfs.apply(pd.to_numeric)  # Make the columns of a numeric datatype
            dfs = dfs.groupby(lambda x: x, axis=1).sum()  # Group the pistes together
            departures = departures.append(dfs)
    departures["Total"] = departures.sum(axis=1) # Adds a total column

    return departures


def generate_zh_departures():
    url = "https://www.flughafen-zuerich.ch/-/jssmedia/airport/portal/dokumente/das-unternehmen/politics-and-responsibility/noise-and-sound-insulation/monatliche-flugbewegungen_2108.pdf?vs=1"
    output_filename = "../data/input/Zurich_Airpot_Flight_numbers.pdf"
    retrieve_pdf(url, output_filename)
    data = tabula.read_pdf(output_filename, pages='all', stream=True)
    departures = extract_departures_df(data)
    departures["company_name"] = "Flughafen Zürich AG"
    departures.to_csv("../data/output/StudentA_Source_B1_clean_ZH_departures.csv", index=False)
    engine = sq.create_engine("mysql+mysqlconnector://mark:password@localhost:3306/CIP")
    departures.to_sql(con=engine, name="zurich_departures", if_exists="replace")


def main():
    generate_zh_arrivals()
    generate_zh_departures()


if __name__ == "__main__":
    main()
