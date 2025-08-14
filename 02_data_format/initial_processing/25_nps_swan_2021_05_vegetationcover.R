# Calculate cover percent from line-point intercept data. Dead plants are calculated separately from live plants. In this dataset, dead status is indicated in the "Damage_Code" column.

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
file_name <- paste0("05_nps_swan_",str_replace_all(Sys.Date(),pattern="-",replacement=""),".csv")
output_veg_cover <- file.path(project_folder, "temp_files", file_name)

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

# Calculate maximum number of hits based on number of points per transect and number of transects per site
hits <- all_cover %>% 
  group_by(site_visit_id, Transect) %>% 
  summarize(distance = length(unique(Point_m))) %>% 
  group_by(site_visit_id) %>% 
  summarize(max_hits = sum(distance))

# Calculate number of hits per species per plot
# If a species shows up multiple times on the same point, it still counts as just 1 hit
all_cover <- all_cover %>% 
  group_by(site_visit_id, Transect, Point_m, Species_Name, dead_status) %>%
  summarize(hits = 1) %>% 
  group_by(site_visit_id, Species_Name, dead_status) %>%
  summarize(total_hits = length(hits))
  
all_cover <- left_join(all_cover, hits, by = "site_visit_id")

# Calculate % cover
all_cover <- all_cover %>% 
  mutate(cover_percent = total_hits / max_hits * 100) 

# Restrict to plants & fungi ----
codes <- codes %>% 
  filter(include_in_cover == TRUE)

veg_cover <- all_cover %>% 
  filter(Species_Name %in% codes$name_original)

# Match data to entry form ----

# Round cover percent to 3 decimal places
veg_cover$cover_percent <- round(veg_cover$cover_percent, digits = 3)

# Add columns: 'cover type', 'name_adjudicated'
# Change 'Species_Name' to 'name_original'
veg_cover <- veg_cover %>%
  mutate(cover_type = "absolute foliar cover",
         name_adjudicated = "",
         dead_status = if_else(grepl(pattern="Dead",Species_Name),"TRUE",dead_status)) %>% 
  rename(name_original = Species_Name) %>% 
  select(site_visit_id, name_original, name_adjudicated, cover_type, dead_status, cover_percent)

# Export data ----
write_csv(veg_cover,output_veg_cover)