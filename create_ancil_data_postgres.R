# Author: Saeesh Mangwani
# Date: 2021-06-02

# Description: A script that generates the ancilliary data files from the
# hourly observation well data (scraped monthly by the obswell_scraping.py
# script)

# ==== Loading libraries ====
library(DBI)
library(RPostgreSQL)
library(lubridate)

# ==== Reading data ====

# Opening database connection
conn <- dbConnect("PostgreSQL", 
                  host = 'localhost', dbname = 'gws', 
                  user = 'saeesh', password = 'admin')

# Dropping any temp tables if they exist
dbExecute(conn, 'drop table if exists obswell.temp')

# Creating a temp table as a copy of the hourly dataset with time converted to date
dbExecute(conn, 
          'create table obswell.temp as (
            select "Time","Time"::date as "Date", "Value", "Approval", "myLocation" 
            from obswell.hourly
          )')

# ==== Daily mean dataset ====

# Dropping the table if it exists
dbExecute(conn, 'drop table if exists obswell.daily_mean')

# Create the daily mean table from the temp table
dbExecute(conn, 
          '
          create table obswell.daily_mean as (
        	select "myLocation", 
        			"Date", 
        			avg("Value") as "Value", 
        			count("Date") as "numObservations"
        	from obswell.temp
        		group by "myLocation", "Date"
        		order by "myLocation", "Date")
          ')

# ==== Past 1-year dataset ====

# Dropping table if it exists
dbExecute(conn, 'drop table if exists obswell.hourly_recent')

# Specifying the timestamp for 1-year ago
date_filter <- ymd((Sys.Date() - 366))

# Creating a past 1 year dataset from the temptable
dbExecute(conn, 
          paste0('create table obswell.hourly_recent as(', 
          'select "Time", "Value", "Approval", "myLocation"
          from obswell.temp where "Date" >= ',
          "'", date_filter, "'",
          ')'))

# Dropping the temp table
dbExecute(conn, 'drop table obswell.temp')
