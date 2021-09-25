# Author: Saeesh Mangwani
# Date: 2021-06-02

# Description: A script that generates the ancilliary data files from the
# hourly observation well data (scraped monthly by the obswell_scraping.py
# script)

# ==== Loading libraries ====
library(dplyr)
library(readr)
library(stringr)
library(lubridate)

# ==== Reading data ====

# Path to the data (since the data is huge, we can't read it and just keep it
# in memory. It is read selectively for each step and then thrown out)
path_to_dat <- 'ObsWellHourly.csv'

# Path to the directory where data archives are stored (defaults to the data
# folder in the working directory)
path_to_archive = 'archive'

# ==== Creating a daily mean dataset ====

# Reading the hourly dataset and picking only relevant vars
obswell <- read_csv(path_to_dat, 
                    # Selecting only relevant columns and types
                    col_types = 'Td_c') %>%
  # Keeping only the dates from the datetime string
  mutate(Time = as.Date(x = Time, format = '%Y-%m-%d'))

# Removing

# Since the dataset is massive, it needs to be processed in chunks. Setting a
# chunksize to 10000
chunksize <- 100000
n <- 0
i <- 0
while (n < nrow(obswell)) {
  i <- n + 1
  # If n is bigger than the number of rows, setting it equal to nrow to
  # prevent an out of bounds error. Otherwise just keep incrementing n by
  # 100000
  n <- ifelse(n > nrow(obswell), nrow(obswell), n + chunksize)
  # For each chunk getting a mean grouped by date and time
  obswell[i:n,] %>% 
    group_by(myLocation, Time) %>% 
    summarise(Value = mean(Value), ssize = n()) %>% 
    # Writing to disk
    write_csv('ObsWellDailyMean-temp.csv', 
              # For the first set of rows, overwriting the current file (since
              # we're updating it fresh). Otherwise appending
              append = !(i == 1), 
              # For the first set of rows, adding column names. Otherwise just
              # appending data
              col_names = (i == 1))
  print(paste('Processed', n, 'rows'))
}

# Removing the object
rm(obswell)
gc()

# Reading the mean dataset back and further re-meaning any duplicate rows (it
# is possible these are created since the 100000 chunksize may not fall
# exactly on the boundary between days)
read_csv('ObsWellDailyMean-temp.csv',
         col_types = 'cDdd') %>% 
  group_by(myLocation, Time) %>% 
  summarize(Value = weighted.mean(Value, ssize)) %>% 
  write_csv('ObsWellDailyMean.csv', append = F)

# removing the temp file
file.remove('ObsWellDailyMean-temp.csv')

# ==== Creating a past 1-year dataset ====

# Specifying the timestamp
last_year <- ymd((Sys.Date() - 366))

# Reading dataset
read_csv(path_to_dat, col_types = 'Tdcc') %>% 
  # Filtering only values since the last year
  filter(Time > last_year) %>% 
  # Writing to disk
  write_csv('ObsWellHourly1Year.csv', append = F)

# ==== Copying all datasets to the archive ====

# Hourly
file.copy('ObsWellHourly.csv', 
          paste0(path_to_archive, '/ObsWellHourly_', Sys.Date(), '.csv'), 
          overwrite = T)
# Daily means
file.copy('ObsWellDailyMean.csv', 
          paste0(path_to_archive, '/ObsWellDailyMean_', Sys.Date(), '.csv'),
          overwrite = T)
# 1-year
file.copy('ObsWellHourly1Year.csv', 
          paste0(path_to_archive, '/ObsWellHourly1Year_', Sys.Date(), '.csv'),
          overwrite = T)
# Update report
file.copy('update_report.txt', 
          paste0(path_to_archive, '/update_report', Sys.Date(), '.txt'),
          overwrite = T)

# ==== Cleaning the archive ====

# Getting available files and naming them by their datestamps
fnames <- list.files('archive') %>% 
  setNames(str_extract(., "\\d{4}-\\d{2}-\\d{2}"))

# Sorting names to get all the unique dates for which data is currently stored
dates <- names(fnames) %>% 
  ymd() %>% 
  unique()

if (length(dates) < 2) {
  print("Only 1 version of data present in the archive. No cleaning performed")
}else{
  # Sorting dates and selecting only the most recent 2
  dates <- dates %>% 
    sort() %>% 
    tail(2)
  
  # Indexing the list to select only those files dated before these 2
  fnames <- fnames[!(ymd(names(fnames)) %in% dates)]
  
  # Removing these files (if there are any to remove)
  if (length(fnames) > 0) file.remove(paste0('archive/', fnames))
  
}
  

