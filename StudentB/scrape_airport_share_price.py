#!/usr/bin/env python
# coding: utf-8

# # Scraping Zurich Airport Daily Share Price

import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager


def extract_share_price(url, airport) :
    # Setting up the web driver
    s = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=s)

    # fetching the url
    driver.get(url)

    # Pointing Selenium where to go
    body = driver.find_element_by_tag_name("body")  # the whole webpage

    # scroll down 60 times to get all the required data
    no_of_pagedowns = 60
    while no_of_pagedowns:
        body.send_keys(Keys.PAGE_DOWN)
        time.sleep(0.2)
        no_of_pagedowns -= 1

    # find the table
    data = driver.find_element_by_xpath("/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[2]/table").get_attribute('outerHTML')  # to get the HTML code of the selected tag

    # fetching the html table and save it as a dataframe
    df = pd.read_html(data)[0]  # pd.read_html returns a list, so we access its first (and only) element

    # export dataframe
    df.to_csv('../data/output/StudentB/share_price_'+airport+'.csv', index=False)
    print("Outputted")
    return df


def main():
    extract_share_price("https://finance.yahoo.com/quote/FHZN.SW/history?period1=1546300800&period2=1635552000&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true","Zurich")
    extract_share_price("https://finance.yahoo.com/quote/FLW1.BE/history?period1=1546300800&period2=1635552000&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true", "Vienna")

if __name__ == "__main__":
    main()