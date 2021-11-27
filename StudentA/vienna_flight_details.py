#!/usr/bin/env python
# coding: utf-8
import urllib.request

import pandas as pd
import sqlalchemy as sq


def retrieve_pdf(url, output_filename):
    """

    :param url:
    :param output_filename:
    """
    user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
    headers = {'User-Agent': user_agent, }
    request = urllib.request.Request(url, None, headers)  # The assembled request
    response = urllib.request.urlopen(request)
    import os
    print(os.getcwd())
    file = open(output_filename, 'wb')
    file.write(response.read())
    file.close()


def month_to_num(shortmonth, year):
    """

    :param shortmonth:
    :param year:
    :return:
    """
    return {
        'Jänner': "01.01." + str(year),
        'Februar': "01.02." + str(year),
        'März': "01.03." + str(year),
        'April': "01.04." + str(year),
        'Mai': "01.05." + str(year),
        'Juni': "01.06." + str(year),
        'Juli': "01.07." + str(year),
        'August': "01.08." + str(year),
        'September': "01.09." + str(year),
        'Oktober': "01.10." + str(year),
        'November': "01.11." + str(year),
        'Dezember': "01.12." + str(year),
        'Unnamed: 0': 'Category',
        'Year': 'Year'
    }[shortmonth]


def main():
    """

    """
    url = "https://www.viennaairport.com/jart/prj3/va/uploads/data-uploads/IR/2021/10_Excel_Traffic_results_October_2021.xlsx"
    output_filename = "data/input/vienna.xlsx"
    retrieve_pdf(url, output_filename)

    year_start_point = {2021: 5, 2020: 34, 2019: 63, 2018: 92}  # in the Excel file, there are different  starting rows for each year
    df = pd.DataFrame()  # A main dataframe for the finalised data
    df_airport = pd.DataFrame()  # As there are multiple airports in the file, we use this df as an intermediary store
    for year in year_start_point:
        tmp_df = pd.read_excel(output_filename, skiprows=year_start_point[year], nrows=24, usecols="A:M")  # Depending on year, start at specific row
        tmp_df["Year"] = year
        tmp_df = tmp_df.rename(columns=lambda s: month_to_num(s, tmp_df["Year"].unique()[0]))  # Depending on the month column name, map it to a full date

        for x in range(0, 4):  # This splits the sub-tables into seperate chunks for each airport
            airport_name = tmp_df[["Category"]].iloc[x * 6][0]  # Extracts the airport name from the 1 row
            tmp_df_airport = tmp_df.iloc[x * 6 + 1:(x + 1) * 6]  # defines which rows to take for that 1 specific airport
            tmp_df_airport["Airport"] = airport_name
            df_airport = df_airport.append(tmp_df_airport)

        df2 = pd.melt(df_airport, id_vars=["Airport", "Category"], value_vars=tmp_df.columns[1:-1], value_name="amount", var_name="date")  # Changes the columns to 1 row for a better processing format
        df = df.append(df2)
    df.to_csv("data/output/StudentA_Source_B2_clean_Vienna_Aiport.csv", index=False)  # exports the data
    df["company_name"] = "Flughafen Wien AG"
    engine = sq.create_engine("mysql+mysqlconnector://mark:password@localhost:3306/CIP")
    df.to_sql(con=engine, name="vienna_flights", if_exists="replace", index=False)


if __name__ == "__main__":
    main()
