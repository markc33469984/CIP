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


def generate_co2_summary(airport_env_ind):
    """

    :param airport_env_ind:
    :return:
    """
    airport_env_ind = airport_env_ind[airport_env_ind["Enviromental Key performance indicators"].isin(["CO2 emissions, scope 1","CO2 emissions, scope 2","CO2 emissions, scope 3"])]
    airport_env_ind = airport_env_ind[["Airport","2018","2019","2020"]]
    airport_env_ind["2018"] = airport_env_ind["2018"].astype(float)
    airport_env_ind["2019"] = airport_env_ind["2019"].astype(float)
    airport_env_ind["2020"] = airport_env_ind["2020"].astype(float)
    airport_env_ind["Airport"] = np.where(airport_env_ind["Airport"]=="Vienna","Flughafen Wien AG","Flughafen Zürich AG")
    airport_env_ind = airport_env_ind.rename(columns={"Airport":"company_name"})
    airport_env_ind = airport_env_ind.groupby("company_name").sum()
    airport_env_ind = airport_env_ind.add_prefix("Total CO2 Emissions ")
    return airport_env_ind


def generate_total_flights(zh_flights, vi_flights):
    """
    Combines & aggregates the flight data from two dataframes. It then pivots and aggregates on a monthly/ yearly basis
    :param zh_flights: dataframe of zurich airport volumes
    :param vi_flights:  dataframe of vienna airport flight volumes
    :return: flights: aggregated monthly flight volumes, flights_sum, summarised yearly flight volumes
    """
    zh_flights["date"] = pd.to_datetime(zh_flights["date"], format="%d.%m.%y")  # Ensure that the date is in the correct format
    zh_flights = zh_flights.drop(columns=["Departures", "Arrivals"])  # We're only interested in the total flight numbers, so can drop this
    zh_flights["flights"] = zh_flights["flights"].astype(int)  # Ensure that the flight number is in the correct format
    zh_flights = zh_flights.groupby(["company_name", pd.Grouper(key='date', freq='M')]).sum().reset_index()  # We group the flights by month. As the Vienna data is only provided on a monthly basis, we need to aggregate

    vi_flights["date"] = pd.to_datetime(vi_flights["date"], format="%d.%m.%Y")  # Ensure that the date is in the correct format
    vi_flights = vi_flights[vi_flights["Category"] == "Flugbewegungen (an + ab)"]  # Filter the data to only get the flight volumes
    vi_flights = vi_flights[vi_flights["Airport"] == "Flughafen Wien Gruppe (VIE, MLA, KSC)"]  # Although we get the data for all airports, we're interested in the company volumes
    vi_flights["company_name"] = "Flughafen Wien AG"  # Setting a column column name for joining later
    vi_flights = vi_flights.groupby(["company_name", pd.Grouper(key='date', freq='M')]).sum().reset_index()  # We group the flights by month
    vi_flights = vi_flights.rename(columns={"amount": "flights"})  # Ensure we have consistent naming conventions

    flights = zh_flights.append(vi_flights)  # Append the two dataframes into one
    flights = flights[flights["flights"].astype(int) > 0]  # We're not interested in months with 0 flights
    flights = flights[flights["date"] >= pd.to_datetime("2019-01-01")]  # To compare, we only want common dates

    # Yearly aggregation - flights summed per year
    flights_sum = flights.groupby(["company_name", pd.Grouper(key='date', freq='Y')]).sum().reset_index().pivot(index="company_name", columns="date")  # Here we group by year and airport
    flights_sum.columns = flights_sum.columns.get_level_values(1).year  # Clean up the columns, as pandas creates a multi-level column. Therefore "flattens"
    flights_sum = flights_sum.add_prefix("Annual flights ")  # Add a prefix to the columns e.g. Zurich 2020 -> Annual flights Zurich 2020

    flights = flights.pivot(index='date', columns='company_name', values='flights')  # Finally pivots to columns, for the graphs later
    return flights, flights_sum


def generate_covid_cases_summary(cov):
    """
    Filters, groups and aggregates the COVID data
    :param cov: dataframe with covid data on a daily basis for all countries
    :return: cov: filtered data for Switzerland and Austria, cov_sum, yearly aggregated data
    """
    cov["date"] = pd.to_datetime(cov["date"], format="%Y.%m.%d")  # Ensure that the date is in the correct format
    cov = cov[cov["country"].isin(["Switzerland", "Austria"])]  # Filters to only the two countries that we're interested in
    cov = cov[["country", "date", "new_cases"]]  # And only the columns that we want to use in the charts

    # Yearly aggregated data
    cov_sum = cov.groupby(["country", pd.Grouper(key='date', freq='Y')]).sum().reset_index()  # Groups on the year, for yearly data
    cov_sum = cov_sum.pivot(index='country', columns='date', values='new_cases')  # Pivots to columns
    cov_sum.columns = cov_sum.columns.year  # Rename the columns, so that it is the year
    cov_sum = cov_sum.add_prefix("COVID Cases ")  # Add a prefix to the columns e.g. Zurich 2020 -> COVID Cases flights Zurich 2020

    cov = cov.pivot(index='date', columns='country', values='new_cases').reset_index()  # Pivots the monthly data, so each column is a country.
    cov = cov.rename(columns={"Austria": "COVID-19 Cases - Austria", "Switzerland": "COVID-19 Cases - Switzerland"})  # Renames the columss to be more meaningful
    cov = cov.set_index("date")
    # cov = cov.groupby(["location",pd.Grouper(key='date', freq='M')]).sum().reset_index()
    return cov, cov_sum


def generate_share_price_summary(share_vi, share_zh):
    """
    Procedure that aggregates the shareprices of  each company and then creates a yearly summary
    :param share_vi: daily share prices for vienna airport
    :param share_zh: daily share prices for Zurich airport
    :return: share_price: monthly average share price values for each company, share_price_sum: yearly aggregate data (mean, min, max)
    """
    share_vi["ticker"] = "VIEV.VI" # First, adds the ticker column, so they can be joined later
    share_zh["ticker"] = "FHZN.S"
    share_price = share_zh.append(share_vi) # Aggregates the two dataframes into 1

    share_price["Date"] = pd.to_datetime(share_price["Date"], infer_datetime_format=True) #Ensures that we have correct datatype
    share_price = share_price[["ticker", "Date", "Close"]] # We're only interested in 3 columns
    share_price = share_price.rename(columns={"Date": "date", "Close": "share_price"}) # Renames the columns to be consistent

    # Summarised data
    share_price_sum = share_price.groupby("ticker").agg({"share_price": [np.mean, np.max, np.min]}).reset_index() # We aggegate the data to get the mean, min and max of the data
    share_price_sum.columns = share_price_sum.columns.get_level_values(1) # Because of the multi-level columns, we want to flatten to get the aggregate function column names
    share_price_sum = share_price_sum.rename(columns={"": "ticker", "mean": "mean_share_price", "amax": "max_share_price", "amin": "min_share_price"}) #Give them more meaningful names

    share_price = share_price.groupby(["ticker", pd.Grouper(key='date', freq='M')]).mean().reset_index() # Aggegate the data on a monthly basis. This "flattens" the line in subsequent charts (we have many years of data)
    share_price = share_price.pivot(index='date', columns='ticker').reset_index()  # Pivots the data to get the columns as each company's ticker
    share_price.columns = share_price.columns.get_level_values(1) # Because of the multi-level columns, we want to flatten to get the comapany's name
    share_price = share_price.rename(columns={"": "date"}) # give it a common data column
    share_price = share_price.set_index("date") # And sets it to the index, to join easier later
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

    engine = sq.create_engine("mysql+mysqlconnector://student:password@localhost:3306/CIP")  # Creates a database connection object

    retrieve_tables_to_df(engine, file_tables) # Extracts the data from the database
    esg = generate_esg_summary(esg_ratings)  # gets the filtered esg data
    flights, flights_sum = generate_total_flights(zurich_flights, vienna_flights)  # aggregates the flight data on a monthly/ yearly basis
    covid_cases, covid_cases_sum = generate_covid_cases_summary(covid) # Gets the COVID data
    share_price, share_price_sum = generate_share_price_summary(shareprice_vienna, shareprice_zurich) # Generates the share price, on a monthly/ yearly basis
    co2 = generate_co2_summary(airport_env_ind) #CO2 Environmental data

    # Based on the aggregated/ further refined data, we then merge the data into one "summary/ master" dataframe
    summary = airport  # We start off with the airport details
    summary = summary.merge(esg, how="left", left_on="company_name", right_on="Airport") # Merge it with the esg data
    summary = summary.merge(share_price_sum, how="inner", left_on="ticker", right_on="ticker") # then with the share price
    summary = summary.merge(covid_cases_sum, how="left", left_on="country", right_on="country") # then with the yeatly covid data
    summary = summary.merge(flights_sum, how="left", left_on="company_name", right_on="company_name") # then with the yearly flight data
    summary = summary.merge(co2, how="left", left_on="company_name", right_on="company_name")

    # Then finally, we insert the aggregated and joined data back into the database
    summary.to_sql(con=engine, name="agg_airport_summary", if_exists="replace")
    share_price.to_sql(con=engine, name="agg_share_price", if_exists="replace")
    covid_cases.to_sql(con=engine, name="agg_covid", if_exists="replace")
    flights.to_sql(con=engine, name="agg_flights", if_exists="replace")


if __name__ == "__main__":
    main()