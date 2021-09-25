# Author: Saeesh Mangwani
# Date: 2021-06-02

# Description: A script that scrapes and organizes observation well data from
# Environment Canada, stored at
# https://www.env.gov.bc.ca/wsd/data_searches/obswell/map/data/

# %% ==== Loading libraries ====
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from io import StringIO
from sqlalchemy import create_engine
from json import load

# %% ==== Initializing global variables ====

# Reading credentials from file
creds = load(open('credentials.json',))

# Setting the default schema to 'obswell' unless another was specified in the file
if 'schema' not in creds.keys():
    creds['schema'] = 'obswell'

# Database connection
db = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}?options=-csearch_path%3D{}'.format(
    creds['user'],
    creds['password'],
    creds['host'],
    creds['port'],
    creds['dbname'],
    creds['schema'],
))
conn = db.raw_connection()
cursor = conn.cursor()

# The path to the directory where the the update report from each run should be
# stored. Defaults to the working directory
path_to_report = 'update_report.txt'

# A dictionary that stores the datatype of all 4 columns in each data
dtype_dict = {
    'Time': 'datetime64',
    'Value': 'float64',
    'Approval': 'str',
    'myLocation': 'str'
}

# Todays date as a string
thisDate = str(datetime.today().date())

# %% ==== Preparing URL names ====

# Website containing all data
page = requests.get(
    'https://www.env.gov.bc.ca/wsd/data_searches/obswell/map/data')
# Parsing with beautiful soup
soup = BeautifulSoup(page.content, 'html.parser')
# Using beautiful soup to find the data table element by class and id
dat_tab = soup.find(name='table')
# Converting it to a pandas dataframe
df = pd.read_html(str(dat_tab), na_values=' ', keep_default_na=False)[0]
# Getting only the filename column as a list
nameList = df.Name.dropna().to_list()
# Filtering only those names where the '-recent.csv' tag is present
datNames = [name for name in nameList if re.search('-recent\\.csv$', name)]

# ==== Creating dictionaries to store request status + error messages ====
datStatus = {name: '' for name in datNames}
datError = {name: '' for name in datNames}

# %% ==== Iterating over urls to download and format data ====

# For each name in the list of names
for name in datNames:
    try:
        # Initialize an empty string buffer
        sio = StringIO()
        # Creating the url for this dataset
        url = 'https://www.env.gov.bc.ca/wsd/data_searches/obswell/map/data/' + name
        # Getting the data
        page = requests.get(url)
        # Storing response status
        datStatus[name] = page.status_code
        # Splitting the csv text by lines
        lines = page.text.splitlines()
        # Since each line is stored as a single string, further splitting each line into a list of values
        text_list = [line.replace('"', '').split(',') for line in lines]
        # Converting the list of lists to a dataframe
        df = pd.DataFrame(text_list[1:], columns=text_list[0])
        # Ensuring type consistency
        df = df.astype(dtype_dict)

        # Querying only "recent" data already present in the table
        cursor.execute(
            """select * from obswell.hourly 
            where "myLocation" = '{}'
            and "Time" >= '{}'
            order by "Time" """.format(
                name.replace("-recent.csv", ""), 
                min(df.Time).strftime("%Y-%m-%d")
            )
        )
        # Reading the result
        curr_data = pd.DataFrame(cursor.fetchall(), columns=df.columns.array)

        # Set differencing to only get timestamps that are new
        index = list(set(df.Time).difference(set(curr_data.Time)))
        # Filtering the dataframe with it to only get the rows that need to be added
        update = df[df.Time.isin(index)]
        
        # Writing data to buffer
        update.to_csv(sio, sep = ',', index=False, header=None, columns = df.columns.array)
        sio.seek(0)
        # Appending to database from from buffer
        cursor.copy_from(sio, "hourly", sep = ',')
        conn.commit()
        # Status print
        print('Completed download for ' + name)
    except Exception as e:
        # Printing a message in case of an error
        print('Download failed for station', name)
        print("Error message:", str(e))
        # Saving the error message in the success dictionary
        datError[name] = str(e)

# Status print when the iteration completes
print('All downloads completed')

# %% ==== Writing a status report ====

# Checking whether all links were valid
all_valid = all(
    [status == 200 for status in datStatus.values()]
)

# Checking whether all downloads were error free
all_errorFree = all(
    [error == '' for error in datError.values()]
)

# Opening a report file
with open(path_to_report, "w") as f:
    # Printing a header and description
    print('===== BC Observation Wells Data Scraper =====', file=f)
    print('', file=f)
    print('This file gives a summary of the most BC\nObservation well dataset update, run via\nthe script "obswell_scraping.py"', file=f)
    print('', file=f)
    # Date of scraping attempt
    print('Last scrape: ', str(datetime.now()), file=f)
    print('', file=f)
    # Whether all links were valid
    print('All data links valid:', all_valid, file=f)
    print('', file=f)
    if(not all_valid):
        print('Invalid links were:', file=f)
        # Getting the invalid links (i.e those that weren't 200)
        invalid = {key: value for key, value in datStatus.items()
                   if value != 200}
        # Printing them as a dataframe
        print(pd.DataFrame(data=list(invalid.items()),
              columns=['station', 'response_code']))
    # Whether there were download or other errors
    print('All downloads and data formatting was error free:', all_errorFree, file=f)
    if(not all_errorFree):
        print('Links that thew errors were:',  file=f)
        # Getting the invalid links (i.e those that weren't 200)
        wError = {key: value for key, value in datError.items()
                  if value != ''}
        # Printing them as a dataframe
        print(pd.DataFrame(data=list(wError),
              columns=['station', 'error_message']))
    print('', file=f)
