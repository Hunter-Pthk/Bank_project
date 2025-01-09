# Code for ETL operations on Country-GDP data
# Importing the required libraries

import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import sqlite3
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name","MC_USD_Billion"]
db = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt","a") as f:
        f.write(timestamp + ' : ' + message + '\n') 
        
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    data = []
    for row in rows[1:]:  # Skip the header row
        columns = row.find_all('td')
        if len(columns) >= 3:  # Ensure row has enough columns
            Name = columns[1].get_text(strip=True)  # Extract name
            MC_USD_Billion = columns[2].get_text(strip=True).replace('\n', '')  # Clean Market Cap
            try:
                MC_USD_Billion = float(MC_USD_Billion)  # Convert to float
            except ValueError:
                MC_USD_Billion = None  # Handle invalid conversion
            data.append({"Name": Name, "MC_USD_Billion": MC_USD_Billion})
    
    # Create a DataFrame
    df = pd.DataFrame(data, columns=table_attribs)
    return df

def transform(df1):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    file_path = 'exchange_rate.csv'
    df = pd.read_csv(file_path)
    exc_dict = dict(zip(df['Currency'],df['Rate']))
    df1['MC_GBP_Billion'] = [ np.round((x * exc_dict['GBP']),2)  for x in df1['MC_USD_Billion']]
    df1['MC_EUR_Billion'] = [ np.round((x * exc_dict['EUR']),2)  for x in df1['MC_USD_Billion']]
    df1['MC_INR_Billion'] = [ np.round((x * exc_dict['INR']),2)  for x in df1['MC_USD_Billion']]
    return df1

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace',index = False)


log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
# print(df)
log_progress('Data extraction complete. Initiating Transformation process')

df_t = transform(df)
# print(df_t)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df_t, csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated')

load_to_db(df_t, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')