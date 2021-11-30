#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import requests
import sqlalchemy as sq
from bs4 import BeautifulSoup

# Set some variables, including the reuters base urls, company website & headers
company_search_url = ["https://www.reuters.com/finance/stocks/lookup?search=Aerop&searchType=any&sortBy=&dateRange=&comSortBy=marketcap",
                      "https://www.reuters.com/finance/stocks/lookup?search=Flughafen&searchType=any&sortBy=&dateRange=&comSortBy=marketcap",
                      "https://www.reuters.com/finance/stocks/lookup?search=Airport&searchType=any&sortBy=&dateRange=&comSortBy=marketcap"]
base_company_url = "https://www.reuters.com/companies/"
myheaders = {'User-Agent': 'Mozilla/5.0              (Macintosh; Intel Mac OS X 10_15_7)              AppleWebKit/605.1.15 (KHTML, like Gecko)             Version/15.1 Safari/605.1.15'}


def extract_airport_list(company_search_url):
    """
    Uses beautiful soup to search for a list of airports that are public on the stock exchange. Needed to search different pages within different urls
    :param company_search_url: the url for reuters to use to seatch
    :return: a dataframe of the aiports and their codes for further searching
    """
    html_page = requests.get(company_search_url, headers=myheaders)
    soup = BeautifulSoup(html_page.content, 'lxml')
    tab = soup.find("table", {"class": "search-table-data"})
    df = pd.read_html(tab.prettify(), flavor='bs4')[0]
    return df


def extract_airport_multiple_search():
    """
    Aggregates the list of data returned from each page of the airport search. This combined list is used for further searches
    :return: a complete dataframes for each of the n pages previously searched
    """
    airport_list = pd.DataFrame()
    for i in company_search_url:
        airport_list = airport_list.append(extract_airport_list(i))  # uses the method to append the returned airport lists
    return airport_list


def extract_airport_details(base_company_url, symbol, myheaders):
    """
    Visits a reuters webpage, based on a company code and then scrapes company information from that website, including the name, address, website etc.
    :param base_company_url: the url to request further information for for that symbol
    :param symbol: the unique company code (ticker/ symbol)
    :param myheaders: use a specific "realistic" header code, to reduce the chance of being spotted scrapign
    :return: a dataframe that has details of multiple companies (airports), including their address details, website etc.
    """
    html_page = requests.get(base_company_url + symbol, headers=myheaders)
    soup = BeautifulSoup(html_page.content, 'lxml')
    airport_company = pd.DataFrame()
    try:
        company_name = str(soup.find_all("h1", class_="TextLabel__text-label___3oCVw TextLabel__black___2FN-Z TextLabel__medium___t9PWg QuoteRibbon-name-3x_XE")[0].string)
        ticker = str(soup.find_all("p", class_="TextLabel__text-label___3oCVw TextLabel__gray___1V4fk TextLabel__medium___t9PWg QuoteRibbon-ric-2pHzH")[0].string)
        profile = str(soup.find_all("p", class_="TextLabel__text-label___3oCVw TextLabel__black___2FN-Z TextLabel__serif___3lOpX Profile-body-2Aarn")[0].string)
        industry = str(soup.find_all("p", class_="TextLabel__text-label___3oCVw TextLabel__black___2FN-Z TextLabel__regular___2X0ym About-value-3oDGk")[0].string)
        addressl1 = str(soup.find_all("p", class_="TextLabel__text-label___3oCVw TextLabel__black___2FN-Z TextLabel__regular___2X0ym About-value-3oDGk")[1].string)
        addressl2 = str(soup.find_all("p", class_="TextLabel__text-label___3oCVw TextLabel__black___2FN-Z TextLabel__regular___2X0ym About-value-3oDGk")[2].string)
        country = str(soup.find_all("p", class_="TextLabel__text-label___3oCVw TextLabel__black___2FN-Z TextLabel__regular___2X0ym About-value-3oDGk")[3].string)
        phone = str(soup.find_all("p", class_="TextLabel__text-label___3oCVw TextLabel__black___2FN-Z TextLabel__regular___2X0ym About-value-3oDGk About-phone-2No5Q")[0].string)
        website = str(soup.find_all("a", class_="TextLabel__text-label___3oCVw TextLabel__blue-to-orange___1SFN2 TextLabel__regular___2X0ym About-value-3oDGk website")[0].string)
        airport_company = airport_company.append(pd.DataFrame(data=[[company_name, ticker, profile, industry, addressl1, addressl2, country, phone, website]],
                                                              columns=["company_name", "ticker", "profile", "industry", "addressl1", "addressl2", "country", "phone", "website"]))
        return airport_company
    except Exception:
        pass


def airport_details_dirty(df):
    """
    Function to make the data evener dirtier
    :param df: a "sem-clean" dataframe
    :return: a "dirty" dataframe"
    """
    df = df.append(df[df["company_name"] == "Airport City Ltd."])  # Make dirty by duplicate
    df["ticker"] = np.where(df["ticker"] == "AIRPORT FACILITIES Co., LTD.", np.NaN, df["ticker"])  # DQ Resolution Issue 1
    return df


def airport_details_cleanse(df):
    """
    Function to cleanse the data after artificially making dirty
    :param df: a "dirty" dataframe
    :return: a "clean" dataframe"
    """
    df["company_name"] = np.where(df["company_name"] == "Flughafen Zuerich AG", "Flughafen Zürich AG", df["company_name"])  # DQ Resolution Issue 1
    df[["phone-prefix", "phone"]] = df["phone"].str.split('.', 1, expand=True)  # DQ Resolution Issue 2
    df["phone"] = df["phone"].str.replace(".", "", regex=True)  # DQ Resolution Issue 3
    df = df.drop_duplicates()  # DQ Resolution Issue 4
    df = df[df["company_name"] != "AIRPORT FACILITIES Co., LTD."]  # DQ Issue 5

    airport_country_map = {"Airports of Thailand PCL": "Thailand", "AIRPORT FACILITIES Co., LTD.": "Japan", "Malaysia Airports Holdings Berhad": "Malaysia", "Shanghai International Airport Co., Ltd.": "China"}
    for x in airport_country_map:
        df.loc[df["company_name"] == x, "addressl3"] = df[df["company_name"] == x]["country"]  # DQ Resoltion 6
        df.loc[df["company_name"] == x, "country"] = airport_country_map[x]  # DQ Resoltion 6
    return df


def main():
    """
    Main function that a) searches for a list of comapnies with "airport", "flughafen" etc from Reuters website b) scrapes data from these websites c) loads into DB/ CSV file
    """
    airport_list = extract_airport_multiple_search()  # First gets the list of companies
    airport_company = pd.DataFrame()
    for i in airport_list["Symbol"].unique():  # Then for each company, scrapes the website for information
        airport_company = airport_company.append(extract_airport_details(base_company_url, i, myheaders))
        # time.sleep(randint(3, 6))  # Sleeps for a random time. Useful to reduce likelihood of website figuring its scraping and then blocking
    airport_company.to_csv("data/output/StudentA_SourceA_airport_details_src.csv", index=False)  # Saves to CSV
    airport_company = airport_details_dirty(airport_company)
    airport_company.to_csv("data/output/StudentA_SourceA_airport_details_dirty.csv", index=False)  # Saves to CSV
    airport_company = airport_details_cleanse(airport_company)
    airport_company.to_csv("data/output/StudentA_SourceA_airport_details_stage.csv", index=False)  # Saves to CSV
    engine = sq.create_engine("mysql+mysqlconnector://mark:password@localhost:3306/CIP")
    airport_company.to_sql(con=engine, name="airport", if_exists="replace", index=False)  # Inserts into the database


if __name__ == "__main__":
    main()
