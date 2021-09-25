# Author: Saeesh Mangwani
# Date: 2021-06-02

# Description: A script that generates the ancilliary data files from the
# hourly observation well data (scraped monthly by the obswell_scraping.py
# script)

# ==== Loading libraries ====
library(DBI)
library(RPostgres)
library(lubridate)
library(rjson)

# ==== Reading data ====

# Credentials files
creds <- fromJSON(file = 'credentials.json')

# Setting default schema unless pre-specified
if (is.null(creds$schema)) creds$schema <- 'obswell'

# Opening database connection
conn <- dbConnect(RPostgres::Postgres(), 
                  host = creds$host, dbname = creds$dbname, 
                  user = creds$user, password = creds$password)

# Dropping any temp tables if they exist
dbExecute(conn, paste0('drop table if exists ', creds$schema, '.temp'))

# Creating a temp table as a copy of the hourly dataset with time converted to date
dbExecute(conn, 
          paste0('create table ', creds$schema, '.temp as (
            select "Time","Time"::date as "Date", "Value", "Approval", "myLocation" 
            from ', creds$schema, '.hourly
          )'))

# ==== Daily mean dataset ====

# Dropping the table if it exists
dbExecute(conn, paste0('drop table if exists ', creds$schema, '.daily_mean'))

# Create the daily mean table from the temp table
dbExecute(conn, 
          paste0('
          create table ', creds$schema, '.daily_mean as (
        	select "myLocation", 
        			"Date", 
        			avg("Value") as "Value", 
        			count("Date") as "numObservations"
        	from ', creds$schema, '.temp
        		group by "myLocation", "Date"
        		order by "myLocation", "Date")
          '))

# ==== Past 1-year dataset ====

# Dropping table if it exists
dbExecute(conn, paste0('drop table if exists ', creds$schema, '.hourly_recent'))

# Specifying the timestamp for 1-year ago
date_filter <- ymd((Sys.Date() - 366))

# Creating a past 1 year dataset from the temptable
dbExecute(conn, 
          paste0('create table ', creds$schema, '.hourly_recent as(', 
          'select "Time", "Value", "Approval", "myLocation"
          from ', creds$schema, '.temp where "Date" >= ',
          "'", date_filter, "'",
          ')'))

# Dropping the temp table
dbExecute(conn, paste0('drop table ', creds$schema, '.temp'))
