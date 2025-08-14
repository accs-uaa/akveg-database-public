# Set root directory
drive = 'N:'
root_folder = 'ACCS_Work'

# Define input folders
data_folder = paste(drive,
                    root_folder,
                    'Projects/VegetationEcology/AKVEG_Database/Data/Data_Plots',
                    sep = '/')

# Define input and output
input_file = paste(data_folder,
                   '32_aim_various_2021',
                   '03_sitevisit_aimvarious2021.csv',
                   sep = '/')

# Import libraries
library(dplyr)
library(lubridate)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)

# Read input data
input_data = read_csv(input_file) %>%
  mutate(observe_date = parse_date(observe_date, '%m/%d/%Y'))

# Export data
write.csv(input_data, file = input_file, fileEncoding = 'UTF-8', row.names = FALSE)