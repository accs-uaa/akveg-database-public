# Create site visit table from ABR's 2022 database.

rm(list=ls())

# Load packages ----
library(readxl)
library(tidyverse)

# Define directories ----
drive <- "D:"
root_folder <- file.path(drive,"ACCS_Work/Projects/AKVEG_Database")
data_folder <- file.path(root_folder, "Data/Data_Plots")
project_folder <- file.path(data_folder, "27_ABR_2022")

# Define inputs ----
input_veg <- file.path(project_folder, "export_tables","tnawrocki_deliverable_two_veg.xlsx")
input_els <- file.path(project_folder, "export_tables", "tnawrocki_deliverable_two_els.xlsx")
input_project_code <- file.path(project_folder, "temp_files", "abr_2022_project_code_to_id.xlsx")
input_template <- file.path(root_folder, "Data/Data_Entry","03_Site_Visit.xlsx")
input_observers <- file.path(project_folder, "temp_files", "abr_2019_observers.xlsx")

# Define outputs ----
file_name <- paste0("03_abr_2022_",str_replace_all(Sys.Date(),pattern="-",replacement=""),".csv")
output_visit <- file.path(project_folder, "temp_files",file_name)

# Read in data ----
veg_data <- read_xlsx(path=input_veg,na="NULL")
els_data <- read_xlsx(path=input_els,na="NULL")
project_code <- read_xlsx(path=input_project_code)
template <- colnames(read_xlsx(path=input_template))
observer_data <- read_xlsx(path=input_observers)

# Format data ----

# Join ELS and veg datasheets
# Use ELS for dates since one of the dates listed in the veg data is wrong (plot_id "willow_merlin-03_2018" has a year of 1969) and several veg dates are NA
els_data <- els_data %>% select(plot_id,env_observer_code,env_field_start_ts,veg_structure_ecotype,site_chemistry_calc)
veg_data <- left_join(veg_data,els_data, by="plot_id")

# Parse date
veg_data$observe_date = as.Date(veg_data$env_field_start_ts)
subset(veg_data,is.na(observe_date))

# Create site_visit_id
# I created the site_code column in Excel from the plot_id column to remove _YYYY so that it's not redundant with the date string
veg_data$date_string <- str_replace_all(veg_data$observe_date, pattern="-", replace = "")
veg_data$site_visit_id <- paste(veg_data$site_code,veg_data$date_string,sep="_")

# Attribute project_code (that we assigned)
veg_data <- left_join(veg_data,project_code,by="project_id")

# No need to reclassify structural class. Use veg_structure_ecotype column

# Add observer codes
# Merge veg_data/els_data with observer_data to get full names from initial
veg_data <- left_join(veg_data, observer_data, by=c("veg_observer_code" = "observer_code"))

# Two veg_observer_codes are unresolved: NA, n/data. List as unknown.
veg_data %>% filter(is.na(observer_name)) %>% distinct(veg_observer_code)

veg_data <- veg_data %>% 
  rename(veg_observer = observer_name) %>% 
  mutate(veg_observer = if_else(is.na(veg_observer),"unknown",veg_observer))

veg_data <- left_join(veg_data, observer_data, by=c("env_observer_code" = "observer_code"))

veg_data %>% filter(is.na(observer_name)) %>% distinct(env_observer_code)

veg_data <- veg_data %>% 
  rename(env_observer = observer_name) %>% 
  mutate(env_observer = if_else(is.na(env_observer),"unknown",env_observer))

# Final formatting
# Assume all env observers = soils observers since nobody env & soils data are included in the same datasheet and no other names are listed
site_visit <- veg_data %>% 
  mutate(data_tier = "map development & verification",
         soils_observer = if_else(veg_data$site_chemistry_calc=="no data",
                                  "",env_observer),
         veg_recorder = "unknown") %>% 
  rename(structural_class = veg_structure_ecotype) %>% 
  select(all_of(template))

# Export data ----
write_csv(site_visit,output_visit,na="")