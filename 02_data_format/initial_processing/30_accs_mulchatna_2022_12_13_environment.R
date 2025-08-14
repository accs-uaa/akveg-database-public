# Format table from Survey123 to match data entry requirements for the Environment and Soils tables in the AKVEG Database.

rm(list=ls())

# Load packages ----
library(readxl)
library(tidyverse)

# Define directories ----
drive <- "F:"
root_folder <- file.path(drive,"ACCS_Work/Projects/AKVEG_Database")
data_folder <- file.path(root_folder, "Data/Data_Plots")
project_folder <- file.path(data_folder, "30_accsMulchatna_2022")

# Define inputs ----
input_survey <- file.path(project_folder, "survey123", "mulchatna_2022_environment.xlsx")
input_envr_template <- file.path(root_folder, "Data", "Data_Entry", "12_Environment.xlsx")
input_soils_template <- file.path(root_folder, "Data", "Data_Entry", "13_Soils.xlsx")
input_site_visit <- file.path(project_folder,"03_accs_mulchatna.xlsx")

# Define outputs ----
output_envr <- file.path(project_folder, "temp_files","12_accs_mulchatna.csv")
output_soils <- file.path(project_folder, "temp_files","13_accs_mulchatna.csv")

# Read in data ----
survey_data <- read_xlsx(path=input_survey, sheet="environment")
envr_columns <- colnames(read_xlsx(path=input_envr_template))
soils_columns <- colnames(read_xlsx(path=input_soils_template))
site_visit <- read_xlsx(path=input_site_visit)

# Get Site Visit ID from site_visit datasheet
site_visit <- site_visit %>% 
  select(site_visit_id,site_code)

survey_data <- left_join(survey_data,site_visit,by="site_code")

# Create Environment datasheet ----
envr_formatted <- survey_data %>%
  select(all_of(envr_columns))

# Create Soils datasheet ----
soils_formatted <- survey_data %>%
  rename("depth_15%_rock_cm"  ="depth_15_percent_rock") %>% 
  select(all_of(soils_columns))

# Export data ----
write_csv(x=envr_formatted, file=output_envr, na="")
write_csv(x=soils_formatted,file=output_soils,na="")
