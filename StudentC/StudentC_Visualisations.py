#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import sqlalchemy as sq
import matplotlib.pyplot as plt
pd.set_option('display.max_rows', 500)
from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))
pd.options.display.float_format = '{:.2f}'.format
pd.options.mode.chained_assignment = None


# Extracts data from the database into dataframes
engine = sq.create_engine("mysql+mysqlconnector://student:password@localhost:3306/CIP")

# Answering Research Questions: loading aggregated tables from the database
agg_airport_summary = pd.read_sql_table("agg_airport_summary", engine)
agg_covid = pd.read_sql_table("agg_covid", engine)
agg_flights = pd.read_sql_table("agg_flights", engine)
agg_share_price = pd.read_sql_table("agg_share_price", engine)
q1 = agg_flights.merge(agg_covid, how="left", left_on="date", right_on="date")
q2 = agg_share_price.merge(agg_covid, how="left", left_on="date",
                               right_on="date")  # .plot(logy=False,figsize=(10, 5))


# Providing the answers to Q1 Flights Vs. Covid
q1 = agg_flights.merge(agg_covid,how="left",left_on="date",right_on="date")
ax = q1[["COVID-19 Cases - Austria","COVID-19 Cases - Switzerland"]].plot(figsize=(10, 5))
ax2 = q1[["Flughafen Wien AG","Flughafen Zürich AG"]].plot(secondary_y=True, ax=ax)
ax.set_ylabel('Number of New COVID Cases')
ax2.set_ylabel('Number of Flights')


# Providing the answers to Q2
q2 = agg_share_price.merge(agg_covid,how="left",left_on="date",right_on="date")#.plot(logy=False,figsize=(10, 5))
ax = q2[["COVID-19 Cases - Austria","COVID-19 Cases - Switzerland"]].plot(figsize=(10, 5))
ax2 = q2[["FHZN.S","VIEV.VI"]].plot(secondary_y=True, ax=ax)
ax.set_ylabel('Number of New COVID Cases')
ax2.set_ylabel('Share Price')


