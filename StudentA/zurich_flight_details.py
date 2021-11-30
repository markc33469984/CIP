import urllib.request

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
        dfs = dfs.filter(['Unnamed: 0', 'Anflüge', 'Anflüge Abflüge', 'Abflüge'])  #We're only interested in the departures/ arrivals
        for x in range(0, 2):
            for i in dfs.columns[1:]:
                try:
                    dfs[[i, i + ".1"]] = dfs[i].str.split(' ', 1, expand=True) # DQ Resolution - Sometimes the departures & arrivals are merged into 1 column, this splits them
                except:
                    pass # Somtimes not, so then here it just ignores it
        dfs.columns = ["date", "Departures", "Arrivals"]
        flights = flights.append(dfs) # Adds the months data to the dataframe
    return flights

def flights_dirtify(flights):
    """
    Function to make the data evener dirtier
    :param df: a "sem-clean" dataframe
    :return: a "dirty" dataframe"
    """
    flights.to_csv("../data/output/StudentA_SourceB_ZH_Airport_Flights_dirty.csv", index=False)
    return flights


def flights_cleanse(flights):
    """
    Function to cleanse the data after artificially making dirty
    :param flights: a "dirty" dataframe
    :return: a "clean" dataframe"
    """
    flights = flights[flights.columns[:3]] # DQ Resolution - We only want the first 3 columns
    flights.columns = ["date", "Departures", "Arrivals"]  # DQ Resolution - Gives the headings the correct name
    flights["company_name"] = "Flughafen Zürich AG" # DQ Resolution - ensure the key is available
    flights["flights"] = flights["Departures"].astype(int) + flights["Arrivals"].astype(int) # Merges to give the total number
    return flights


def main():
    url = "https://www.flughafen-zuerich.ch/-/jssmedia/airport/portal/dokumente/das-unternehmen/politics-and-responsibility/noise-and-sound-insulation/monatliche-flugbewegungen_2108.pdf?vs=1"
    output_filename = "../data/output/StudentA_SourceB_ZH_Airport_Flights_src.pdf"
    retrieve_pdf(url, output_filename)  # First get the pdf file from the website
    flights = generate_zurich_flight_numbers(output_filename)  # Then get the flight numbers from the pdf doc
    flights = flights_dirtify(flights)  # Runs procedure to make the data even dirtier
    flights = flights_cleanse(flights)  # Runs the procedure to cleanse the data
    # Outputs the data to files/ database
    flights.to_csv("../data/output/StudentA_SourceB_ZH_Airport_Flights_stage.csv", index=False)
    engine = sq.create_engine("mysql+mysqlconnector://mark:password@localhost:3306/CIP")
    flights.to_sql(con=engine, name="zurich_flights", if_exists="replace", index=False)

if __name__ == "__main__":
    main()
