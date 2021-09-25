:: Calling the activation script to run conda
call C:\Users\OWNER\miniconda3\Scripts\activate.bat

:: Activating the pacfish environment
call conda activate gwenv

:: Navigating to the script home directory
E:
cd E:\saeeshProjects\BC_obsWell_scraping

:: Running the update script
call python obswell_scraping.py

:: Running the ancilliary data creation script (mean daily and 1 yr)
Rscript create_ancil_data.R --vanilla

:: Copying files to the destination directory
COPY "ObsWellHourly.csv" "Z:\GWSI server Master Share Entry\GWSI Library and Resources\DATABASES\BC Observation wells\BC_ObsWell_fullScraped\ObsWellHourly.csv" /Y
COPY "ObsWellDailyMean.csv" "Z:\GWSI server Master Share Entry\GWSI Library and Resources\DATABASES\BC Observation wells\BC_ObsWell_fullScraped\ObsWellDailyMean.csv" /Y
COPY "ObsWellHourly1Year.csv" "Z:\GWSI server Master Share Entry\GWSI Library and Resources\DATABASES\BC Observation wells\BC_ObsWell_fullScraped\ObsWellHourly1Year.csv" /Y
COPY "update_report.txt" "Z:\GWSI server Master Share Entry\GWSI Library and Resources\DATABASES\BC Observation wells\BC_ObsWell_fullScraped\update_report.txt" /Y