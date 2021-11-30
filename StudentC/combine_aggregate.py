#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import sqlalchemy as sq


def retrieve_tables_to_df(engine, file_tables):
    """
    Connects to DB, then loops through list of tables and saves them to an object in the global namespace
    :param file_tables: a dictionary of tables to load from the database
    :param engine: a database connection object
    """
    for i in file_tables:
        table_name = file_tables[i]
        globals()[table_name] = pd.read_sql_table(table_name, engine)


def load_files_to_database(engine, file_tables):
    """
    Loads the csv files from all students into the database
    :param engine: a database connection object
    :param file_tables: the dictionary of file paths & table names to load the data into
    """
    for i in file_tables:  # Loops through each item, loads the csv file and then pushes to database
        # print("Loading file: " + i + " into table: "+file_tables[i])
        df = pd.read_csv("data/" + i)
        df.to_sql(con=engine, name=file_tables[i], if_exists="replace")


def generate_esg_summary(esg_ratings):
    """
    Does any cleansing/ filtering to only return the ESG data we need for the main df
    :param esg_ratings: df from database
    :return: filtered dataset
    """
    return esg_ratings[["Airport", "ESG_score"]]


def generate_total_flights(zh_flights, vi_flights):
    zh_flights["date"] = pd.to_datetime(zh_flights["date"], format="%d.%m.%y") # Ensure that the date is in the correct format
    zh_flights = zh_flights.drop(columns=["Departures", "Arrivals"])  # We're only interested in the total flight numbers, so can drop this
    zh_flights["flights"] = zh_flights["flights"].astype(int) # Ensure that the flight number is in the correct format
    zh_flights = zh_flights.groupby(["company_name", pd.Grouper(key='date', freq='M')]).sum().reset_index() # We group the flights by month. As the Vienna data is only provided on a monthly basis, we need to aggregate

    vi_flights["date"] = pd.to_datetime(vi_flights["date"], format="%d.%m.%Y") # Ensure that the date is in the correct format
    vi_flights = vi_flights[vi_flights["Category"] == "Flugbewegungen (an + ab)"] # Filter the data to only get the flight volumnes
    vi_flights = vi_flights[vi_flights["Airport"] == "Flughafen Wien Gruppe (VIE, MLA, KSC)"] # Although we get the data for all airports, we're interested in the company volumes
    vi_flights["company_name"] = "Flughafen Wien AG"  # Setting a column column name for joinng later
    vi_flights = vi_flights.groupby(["company_name", pd.Grouper(key='date', freq='M')]).sum().reset_index() # We group the flights by month
    vi_flights = vi_flights.rename(columns={"amount": "flights"}) # Ensure we have consistant naming conventions

    flights = zh_flights.append(vi_flights) # Append the two dataframes into one
    flights = flights[flights["flights"].astype(int) > 0] # We're not interested in months with 0 flights
    flights = flights[flights["date"] >= pd.to_datetime("2019-01-01")] # To compare, we only want common dates

    # Yearly aggregation - flights summed per year
    flights_sum = flights.groupby(["company_name", pd.Grouper(key='date', freq='Y')]).sum().reset_index().pivot(index="company_name", columns="date") # Here we group by year and airport
    flights_sum.columns = flights_sum.columns.get_level_values(1).year # Clean up the columns, as pandas creates a multi-level column. Therefore "flattens"
    flights_sum = flights_sum.add_prefix("Annual flights ") # Add a prefix to the columns e.g. Zurich 2020 -> Annual flights Zurich 2020

    flights = flights.pivot(index='date', columns='company_name', values='flights') # Finally pivots to columns, for the graphs later
    return flights, flights_sum


def generate_covid_cases_summary(cov):
    cov["date"] = pd.to_datetime(cov["date"], format="%Y.%m.%d")
    cov = cov[cov["country"].isin(["Switzerland", "Austria"])]
    cov = cov[["country", "date", "new_cases"]]

    cov_sum = cov.groupby(["country", pd.Grouper(key='date', freq='Y')]).sum().reset_index()
    cov_sum = cov_sum.pivot(index='country', columns='date', values='new_cases')
    cov_sum.columns = cov_sum.columns.year
    cov_sum = cov_sum.add_prefix("COVID Cases ")

    cov = cov.pivot(index='date', columns='country', values='new_cases').reset_index()
    cov = cov.rename(columns={"Austria": "COVID-19 Cases - Austria", "Switzerland": "COVID-19 Cases - Switzerland"})
    cov = cov.set_index("date")
    # cov = cov.groupby(["location",pd.Grouper(key='date', freq='M')]).sum().reset_index()
    return cov, cov_sum


def generate_share_price_summary(share_vi, share_zh):
    share_vi["ticker"] = "VIEV.VI"
    share_zh["ticker"] = "FHZN.S"
    share_price = share_zh.append(share_vi)

    share_price["Date"] = pd.to_datetime(share_price["Date"], infer_datetime_format=True)
    share_price = share_price[["ticker", "Date", "Close"]]
    share_price = share_price.rename(columns={"Date": "date", "Close": "share_price"})

    share_price_sum = share_price.groupby("ticker").agg({"share_price": [np.mean, np.max, np.min]}).reset_index()
    share_price_sum.columns = share_price_sum.columns.get_level_values(1)
    share_price_sum = share_price_sum.rename(columns={"": "ticker", "mean": "mean_share_price", "amax": "max_share_price", "amin": "min_share_price"})
    share_price_sum

    share_price = share_price.groupby(["ticker", pd.Grouper(key='date', freq='M')]).mean().reset_index()
    share_price = share_price.pivot(index='date', columns='ticker').reset_index()  # .set_index("share_price")
    share_price.columns = share_price.columns.get_level_values(1)
    share_price = share_price.rename(columns={"": "date"})
    share_price = share_price.set_index("date")
    return share_price, share_price_sum


def main():
    file_tables = {"ESG_ratings_stage.csv": "esg_ratings",
                   "share_price_Vienna_stage.csv": "shareprice_vienna",
                   "share_price_Zurich_stage.csv": "shareprice_zurich",
                   "StudentA_SourceB_ZH_Airport_Flights_stage.csv": "zurich_flights",
                   "StudentA_SourceA_airport_details_stage.csv": "airport",
                   "StudentA_SourceB2_Vienna_Airport_stage.csv": "vienna_flights",
                   "covid_cleaned.csv": "covid",
                   "Combined_ariports_environment _data.csv": "airport_env_ind"}

    engine = sq.create_engine("mysql+mysqlconnector://student:password@localhost:3306/CIP") # Creates a database connection object

    retrieve_tables_to_df(engine, file_tables)
    esg = generate_esg_summary(esg_ratings)  # ESG
    flights, flights_sum = generate_total_flights(zurich_flights, vienna_flights)  # Total Flights
    covid_cases, covid_cases_sum = generate_covid_cases_summary(covid)
    share_price, share_price_sum = generate_share_price_summary(shareprice_vienna, shareprice_zurich)

    # Based on the aggregated/ further refined data, we then merge the data into one "summary/ master" dataframe
    summary = airport  # We start off with the airport details
    summary = summary.merge(esg, how="left", left_on="company_name", right_on="Airport")
    summary = summary.merge(share_price_sum, how="inner", left_on="ticker", right_on="ticker")
    summary = summary.merge(covid_cases_sum, how="left", left_on="country", right_on="country")
    summary = summary.merge(flights_sum, how="left", left_on="company_name", right_on="company_name")

    # Then finally, we insert the aggregated and joined data back into the database

    share_price.to_sql(con=engine, name="agg_share_price", if_exists="replace")
    covid_cases.to_sql(con=engine, name="agg_covid", if_exists="replace")
    flights.to_sql(con=engine, name="agg_flights", if_exists="replace")


if __name__ == "__main__":
    main()

