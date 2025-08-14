# Determine which plots have lichen data and which do not.

rm(list=ls())

# Load packages ----
library(readxl)
library(tidyverse)

# Define directories ----
drive <- "F:"
root_folder <- "ACCS_Work/Projects/AKVEG_Database"
data_folder <- file.path(drive, root_folder, "Data/Data_Plots")
project_folder <- file.path(data_folder, "26_npsCAKN_2022")

# Define inputs ----
lichen_csv <- file.path(project_folder, "source", "CAKN_lichen_cover_finalized_full.xlsx")
site_csv <- file.path(project_folder,"02_nps_cakn.xlsx")

# Define outputs ----
file_name <- paste0("02_nps_cakn_",str_replace_all(Sys.Date(),pattern="-",replacement=""),".csv")
output_csv <- file.path(project_folder, "temp_files",file_name)

# Read in data ----
lichen <- read_excel(path=lichen_csv)
site <- read_excel(path=site_csv)

# Format lichen data ----

# Convert to long form
lichen <- pivot_longer(lichen,
                       cols = ALAL61:XAPO60,
                       names_to = "name_original",
                       values_to = "cover")

# Remove rows for which cover = 0
lichen <- lichen %>% 
  filter(cover > 0) %>%
  select(-c(actual_latitude,actual_longitude))

# Create site ID that matches cover ID
# Remove -1M_V from 'grid' name and append plot number
lichen$grid <- str_sub(lichen$grid, end=-6)
lichen$site_code <- paste(lichen$grid,lichen$plot,sep="")

# Add lichen scope to site data ----
lichen_sites <- unique(lichen$site_code)
site <- site %>% 
  mutate(scope_lichen = if_else(site_code %in% lichen_sites,"exhaustive","none"))

# Some lichen sites are not in the site dataset because they do not have associated vegetation cover data. We're not adding those sites.
which(!(lichen_sites %in% site$site_code))

# Write CSV ----
write_csv(site,output_csv,na="")
