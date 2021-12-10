#!/usr/bin/env python
# coding: utf-8


import numpy as np
import pandas as pd
import sqlalchemy as sq

def load_files_to_database(engine, file_tables):
    """
    Loads the csv files from all students into the database
    :param engine: a database connection object
    :param file_tables: the dictionary of file paths & table names to load the data into
    """
    for i in file_tables:  # Loops through each item, loads the csv file and then pushes to database
        df = pd.read_csv('~/Project/' + i,encoding= 'unicode_escape')
        df.to_sql(con=engine, name=file_tables[i], if_exists="replace")
        
def main():
    file_tables = {"ESG_ratings_stage.csv": "esg_ratings",
                   "share_price_Vienna_stage.csv": "shareprice_vienna",
                   "share_price_Zurich_stage.csv": "shareprice_zurich",
                   "StudentA_SourceB_ZH_Airport_Flights_stage.csv": "zurich_flights",
                   "StudentA_SourceA_airport_details_stage.csv": "airport",
                   "StudentA_SourceB2_Vienna_Airport_stage.csv": "vienna_flights",
                   "StudentC_SourceF_covid19_stage.csv": "covid",
                   "StudentC_SourceC_combined_airports_environment_stage.csv": "airport_env_ind"}
    engine = sq.create_engine("mysql+pymysql://admin:FARhslu123@localhost:3306/CIP")
    load_files_to_database(engine,file_tables)

if __name__ == "__main__":
    main()



