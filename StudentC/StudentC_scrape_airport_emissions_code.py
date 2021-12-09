#!/usr/bin/env python
# coding: utf-8

# In[34]:


#!/usr/bin/env python
# coding: utf-8

import pandas as pd

def extract_table(url,match,index):
    '''
    Scrape table given a url and the matching text. The required table is at the 
    given index from the returned matched tables result.
    :param url: URL of the source of data for scraping
    :param match: the text that should be used to search for the tables
    :param index: index of the required table from the return of the match search 
    :returns: Table 
    '''
    table_extract = pd.read_html(url,match)
    table = table_extract[index]
    table.drop(0, inplace = True)
    return table
    
def set_column_name_vienna(df):
    '''
    Change column names so that all extracted tables from Vienna webpage have the same column names
    :param df: dataframe of which column names to be changed
    '''
    column_names = ['Enviromental Key performance indicators','unit','2017','2018','2019','2020']
    df.columns = column_names
    return df

def df_vienna_env(url):
    '''
    Concats all the environmental tables from the webpage to one data frame. 
    Tables created include environmental data, CO2 emissions data, airborne emissions data, 
    waste production data, water consumption data
    Index of general environmental table = 0
    Index of CO2 emissions table = 0
    Index of Airborne emissions table = 0
    Index of Waste table = 1
    Index of Water consumption table = 1
    :param url: url to be scraped
    :return: Vienna airport environmental indicators dataframe
    '''
    # Extract table enlisting general environmental indicators:
    table_env = extract_table(url,'Environmental aspect', 0)
    set_column_name_vienna(table_env)
    # Extract table with CO2 emissions:
    table_co2 = extract_table(url,'Emissions',0)
    table_co2.insert(1,"Unit",["tonne","tonne","tonne","tonne"]) # insert 'unit' column
    set_column_name_vienna(table_co2)
    # Extract table for airborne emissions
    table_airborne = extract_table(url,'Airborne',0)
    table_airborne.insert(1,"Unit",['kg','g/TU','kg','g/TU','kg','g/TU','kg','g/TU']) #insert 'unit' column
    set_column_name_vienna(table_airborne)
    # Extract table for annual waste production
    table_waste = extract_table(url,"Waste",1)
    set_column_name_vienna(table_waste)
    # Extract table for annual water consumption 
    table_water = extract_table(url, 'Water', 1)
    set_column_name_vienna(table_water)
    # concantenate all tables and return the final environmental data frame
    return pd.concat([table_env,table_co2, table_airborne,table_waste,table_water], ignore_index = True)

def df_zurich_env(url):
    '''
    Extracts the environmental data from Zurihc airport webpage.
    Index of Zurich table = 0
    :param url: URL to be scraped
    :return: Zurich airport environmental indicators 
    '''
    df_env_zurich = extract_table(url, 'emissions', 0)
    df_env_zurich.drop([1,3,5], axis = 1, inplace = True) # remove all columns with only NULL values
    df_env_zurich.insert(1,'unit',['tonne','tonne','tonne','tonne','tonne',
                                   'tonne','tonne','tonne','tonne','tonne','tonne','MWh',
                                   'm3','tonne','%','tonne']) # insert the unit column with corresponding values
    df_env_zurich.columns = ['Enviromental Key performance indicators','unit','2018','2019','2020'] 
    return(df_env_zurich)

def concat_dfs(df1,df2):
    '''
    Join two dataframes to give one final one
    :param df1: dataframe 1 to be concatenated
    :param df2: dataframe 2 to be concatenated
    :return: concatenated Vienna and Zurich airport environmental indicators dataframe
    '''
    return (pd.concat([df1,df2],ignore_index = True))

def main():
    url_vienna = "https://www.viennaairport.com/en/company__jobs/investor_relations/publications_and_reports/sustainability_report/key_data_of_flughafen_wien_group"
    url_zurich = "https://report.flughafen-zuerich.ch/2020/ar/en/key-environmental-data/"
    df_vienna = df_vienna_env(url_vienna)
    df_zurich = df_zurich_env(url_zurich)
    df_env = concat_dfs(df_vienna,df_zurich)
    df_env.to_csv("StudentC_SourceC_combined_airports_environment_src.csv") 

if __name__ == '__main__':
    main()


# In[35]:


pd.read_csv("StudentC_SourceC_combined_airports_environment_src.csv")


# In[ ]:




