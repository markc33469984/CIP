#!/usr/bin/env python
# coding: utf-8

# The following code is to clean up the covid data

import pandas as pd
import numpy as np


# Read the .csv file
covid_data = pd.read_csv("/home/student/Project/StudentC_SourceF_covid19_dirty.csv")
# Get column names from the table
print(covid_data.columns)
# Change the column name 'location' to 'country'
covid_data = covid_data.rename(columns={"location": "country"})
print("The size of the dataframe is:",covid_data.shape)


## NULL VALUES

# Check for which countries that do not have corresponding continents filled
continent_missing = covid_data["continent"].isnull().groupby(covid_data['country']).sum()
# Enlist the countries where there is no corresponding continent data
print(continent_missing[continent_missing > 0])
# Since the listed 'countries' names of continents or groups of countries, These are summed data and redundant 
# and can be removed from the dataframe
countries_continent_null = ['Africa','Asia','Europe','European Union','High income','International','Low income',
'Lower middle income','North America','Oceania','South America','Upper middle income','World']
# Remove the above countries from covid_data
covid_data = covid_data[~covid_data.country.isin(countries_continent_null)]


## MISSING VALUES

# Check for the count of total_case data for countries 
count_total_cases = covid_data["total_cases"].groupby(covid_data['country']).count()
print("Maximum number of total_cases data available for a country is", max(count_total_cases))
# Based on the maximum number of data available for a country, it is sensible to remove any country 
# with missing values more than 200 or has less than 400 non-missing values. 
# These countries can be listed as follows:
countries_total_missing = count_total_cases[count_total_cases< 400]
# Remove these countries 
covid_data = covid_data[~covid_data.country.isin(list(countries_total_missing.index))]
# The remaining countries with missing total cases will be filled later


## DUPLICATE DATA

# Check for duplicates in the dataframe
duplicates = covid_data[covid_data.duplicated()]
duplicates_index = duplicates.index
# Remove the duplicated records from the dataframe
covid_data = covid_data.drop(list(duplicates_index))


## FORWARD FILLING FOR MISSING VALUES

# Countries with NULL values in stringency_index and total_cases
countries_missing_stringency = covid_data["stringency_index"].isnull().groupby(covid_data['country']).sum() 
countries_missing_total = covid_data["total_cases"].isnull().groupby(covid_data['country']).sum() 
print("Number of countries with stringency_index missings:", len(countries_missing_stringency))
print("Number of countries with total_cases missing:", len(countries_missing_total))
# Forward filling for stringency_index and total_cases
covid_data["stringency_index"].fillna(method = 'ffill', inplace=True)
covid_data["total_cases"].fillna(method = 'ffill', inplace = True)
# Check for missing values in new_cases
countries_missing_newcase = covid_data["new_cases"].isnull().groupby(covid_data['country']).sum() 
print("Number of countires with new_cases missing", len(countries_missing_newcase[countries_missing_newcase > 0]))
# Fill new_cases missing values with 0
covid_data["new_cases"].fillna(0, inplace = True)


## FIXING DATA TYPES

# Check for data type of data in all columns in the dataframe
print(covid_data.dtypes)
# Since the data type of the date column is not in the correct date type, change this to the datetime type
covid_data["date"] = pd.to_datetime(covid_data["date"])


## TEXT - NUMBER MISMATCH

# The 'population' column has a data type 'object'. Since it is meant to be numeric,
# examine if the column can be converted to a numeric type. First check for NULL values
print(covid_data.population.isnull().sum())
# All values are filled. Convert all non-numeric values to NULL
covid_data.population[covid_data.population.str.isnumeric()==False] = np.NaN
# Fowrard fill the above generated NULL values
covid_data["population"].fillna(method = 'ffill', inplace = True)
# Convert the population data type to float64
covid_data.population = pd.to_numeric(covid_data.population)


## ENRICHING
# Introducing a new column for population in millions
covid_data['population_mio'] = covid_data['population']/1000000

# Converting to .csv file
covid_data.to_csv("StudentC_SourceF_covid19_stage.csv")



