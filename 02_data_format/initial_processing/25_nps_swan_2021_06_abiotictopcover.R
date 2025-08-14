# Calculate abiotic top cover percent from line-point intercept data. If the first hit is an abiotic_element, cover_percent is calculated and an entry is recorded. If the first hit is NOT an abiotic_element, there is no abiotic top cover for that line.

rm(list=ls())

# Load packages ----
library(readxl)
library(tidyverse)

# Define directories ----
drive <- "F:"
root_folder <- "ACCS_Work/Projects/AKVEG_Database"
data_folder <- file.path(drive, root_folder, "Data/Data_Plots")
project_folder <- file.path(data_folder, "25_npsSWAN_2022")

# Define inputs ----
input_cover <- file.path(project_folder, "source", "AK_Veg_20211115_SWAN_ptint_trees.xlsx")
input_codes <- file.path(project_folder, "temp_files", "swan_codes_biotic_vs_abiotic.xlsx")

# Define outputs ----
file_name <- paste0("06_nps_swan_",str_replace_all(Sys.Date(),pattern="-",replacement=""),".csv")
output_abiotic <- file.path(project_folder, "temp_files", file_name)

# Read in data ----
all_cover <- read_xlsx(path=input_cover, sheet="PointIntercept")
codes <- read_xlsx(path=input_codes)

# Format data ----

# Create unique site_visit_id that combines Plot and Sample_Date
all_cover$date <- gsub("-", "", all_cover$Sample_Date)
all_cover$site_visit_id <- paste(all_cover$Plot,all_cover$date,sep="_")

# Categorize information in "Damage_Code" column into a "dead/live" status.
all_cover <- all_cover %>% 
  dplyr::mutate(dead_status = ifelse(Damage_Code %in% c("SDD","SDL", "SDL-B", "SDD-B"), "TRUE", "FALSE"))

# Calculate cover percent ----

# Extract top hit only from every point
# Assume that the first entry for a point corresponds to the first hit
top_cover <- all_cover %>% 
  group_by(site_visit_id, Transect, Point_m) %>% 
  mutate(hit_number = row_number()) %>% 
  filter(hit_number == 1)

# Calculate max hits for each line transect
max_hits <- length(unique(top_cover$Point_m))*length(unique(top_cover$Transect))

top_cover <- top_cover %>% 
  group_by(site_visit_id, Species_Name, dead_status) %>% 
  summarize(cover_percent = length(Species_Name)/max_hits*100)

# Restrict to only abiotic elements ----
abiotic_codes <- codes %>% 
  filter(abiotic_element != FALSE) %>% 
  select(name_original, abiotic_element)

abiotic_cover <- top_cover %>% 
  filter(Species_Name %in% abiotic_codes$name_original)

# Add standardized names
abiotic_cover <- left_join(abiotic_cover,abiotic_codes,by=c("Species_Name"="name_original"))

# Match data to entry form ----

# Round cover percent to 3 decimal places
abiotic_cover$cover_percent <- round(abiotic_cover$cover_percent, digits = 3)

# Change column names
abiotic_cover <- abiotic_cover %>%
  ungroup() %>% 
  rename(abiotic_top_cover_percent = cover_percent) %>% 
  select(site_visit_id, abiotic_element, abiotic_top_cover_percent)

# Export data ----
write_csv(abiotic_cover,output_abiotic)
