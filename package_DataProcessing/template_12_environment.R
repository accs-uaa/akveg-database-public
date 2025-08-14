# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Environment Data for DATASET_NAME data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: CURRENT_DATE
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Environment Data" appends unique site visit identifier, corrects erroneous and missing values, ensures formatting matches the AKVEG template, and performs QA/QC checks to ensure values match constrained fields and are within a reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(ggplot2)
library(readr)
library(readxl)
library(stringr)
library(tidyr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = plot_folder_path
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, "Data/Data_Entry")

# Define datasets ----

# Define input datasets
environment_input = environment_path
site_visit_input = path(plot_folder, paste0("03_sitevisit_", project_code_name, ".csv"))
dictionary_input = path(project_folder, 'Data', "Tables_Metadata", 'database_dictionary.xlsx')
template_input = path(template_folder, "12_environment.xlsx")

# Define output dataset
environment_output = path(plot_folder, paste0("12_environment_", project_code_name, ".csv"))

# Read in data ----
environment_original = read_xlsx(environment_input)
site_visit_original = read_csv(site_visit_input, show_col_types = FALSE, 
                               col_select= c('site_code', 'site_visit_code'))
dictionary = read_xlsx(dictionary_input)
template = colnames(read_xlsx(template_input))

# Append site visit code ----
environment = environment_original %>%
  left_join(site_visit_original, by ="site_code")

# Ensure all entries have a site visit code
environment %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Ensure that all sites in Site Visit table are associated with an entry in the environment table
which(!site_visit_original$site_visit_code %in% unique(environment$site_visit_code))

# Correct values ----
# Replace null values with appropriate value
# Correct erroneous values

# Populate remaining columns ----
environment_final = environment %>%
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_cover_final, is.na)
    , sum))

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(environment_final, is.na)
    , sum)
)

# Ensure there is an entry for every site
nrow(environment_final) == nrow(site_visit_original)

# Verify constrained values

## Develop list of constrained values
categorical_variables = c('physiography', 'geomorphology', 'macrotopography', 'microtopography', 'moisture', 'drainage', 'disturbance', 'disturbance_severity', 'restrictive_type', 'soil_class')

for (i in 1:length(categorical_variables)) {
  variable = categorical_variables[i]
  
  constrained_values = dictionary %>% 
    filter(field == variable) %>% 
    add_row(data_attribute = 'NULL')
  
  if (variable == 'moisture') {
    variable = 'moisture_regime'
  }
  
  unique_values = environment_final %>% 
    select(all_of(variable)) %>% 
    distinct()
  
  colnames(unique_values) = 'values'
  
  does_not_exist = which(!(unique_values$values %in% constrained_values$data_attribute))
  
  if (length(does_not_exist) != 0) {
    cat(variable, '- CHECK', '\n')
  } else {cat(variable, '- good', '\n')}
}

# Verify that pairing between geomorphology and physiography makes sense
table(environment_final$geomorphology, environment_final$physiography)

# Verify values for disturbance data
table(environment_final$disturbance_severity, 
      environment_final$disturbance_time_y)

# Verify surface water and cryoturbation values (must be boolean)
table(environment_final$surface_water)
table(environment_final$cryoturbation)

# Are there any entries for which surface_water = FALSE but depth_water_cm is not -999?
environment_final %>% 
  filter(surface_water == FALSE & depth_water_cm != -999) %>% 
  select(site_visit_code, moisture_regime, drainage, depth_water_cm, surface_water)

# Depth of moss/duff
environment_final %>% filter(depth_moss_duff_cm != -999) %>% 
  ggplot(aes(x=depth_moss_duff_cm)) + 
  geom_histogram(color="black", fill="white", binwidth = 2) +
  theme_classic()

# Depth of restrictive layer
environment_final %>% filter(depth_restrictive_layer_cm != -999) %>% 
  ggplot(aes(x=depth_restrictive_layer_cm)) + 
  geom_histogram(color="black", fill="white", binwidth = 5) +
  theme_classic()

# Height of microrelief
environment_final %>% filter(microrelief_cm != -999) %>% 
  ggplot(aes(x=microrelief_cm)) + 
  geom_histogram(color="black", fill="white", binwidth = 10) +
  theme_classic()

# Depth at 15% coarse fragments
environment_final %>% filter(depth_15_percent_coarse_fragments_cm != -999) %>% 
  ggplot(aes(x=depth_15_percent_coarse_fragments_cm)) + 
  geom_histogram(color="black", fill="white", binwidth = 5) +
  theme_classic()

# Export data ----
write_csv(environment_final, file=environment_output)

# Clean workspace ----
rm(list=ls())

# Did you remember to update the script header?
