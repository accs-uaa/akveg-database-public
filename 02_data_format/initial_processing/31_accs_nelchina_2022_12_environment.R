# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Environment for ACCS Nelchina data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-03-28
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Environment for ACCS Nelchina data" appends unique site visit identifier, corrects erroneous and missing values, ensures formatting matches the AKVEG template, and performs QA/QC checks to ensure values match constrained fields and are within a reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '31_accs_nelchina_2022')
template_folder = path(project_folder, "Data", "Data_Entry")
source_folder = path(plot_folder, 'source')

# Define datasets ----

# Define input datasets
environment_input = path(source_folder, "Nelchina_2022_Environment.xlsx")
site_visit_input = path(plot_folder, "03_sitevisit_accsnelchina2022.csv")
dictionary_input = path(project_folder, 'Data', "Tables_Metadata", 'database_dictionary.xlsx')
template_input = path(template_folder, "12_environment.xlsx")

# Define output datasets
environment_output = path(plot_folder, '12_environment_accsnelchina2022.csv')

# Read in data ----
environment_original = read_xlsx(environment_input, range="C1:Z23")
site_visit_original = read_csv(site_visit_input, show_col_types = FALSE, col_select= c('site_code', 'site_visit_id'))
dictionary = read_xlsx(dictionary_input)
template = colnames(read_xlsx(template_input))

# Format column names ----
environment = environment_original

# Convert column names to lower case to match AKVEG formatting
colnames(environment) = str_to_lower(colnames(environment))

# Replace space with underscore
colnames(environment) = str_replace_all(string = colnames(environment), 
                                        pattern = "[ |/]", replacement = "_")

# Remove parentheses and special characters
colnames(environment) = str_remove_all(string = colnames(environment), 
                                        pattern = "[(|)|?|%]")

# Append site visit id ----
# Rename remaining columns to match formatting
# Keep only required columns
environment = environment %>%
  left_join(site_visit_original, by="site_code") %>%
  rename(site_visit_code = site_visit_id,
         disturbance_time_y = time_since_disturbance_years,
         depth_water_cm = water_depth_cm,
         depth_moss_duff_cm = moss_duff_depth_cm,
         depth_restrictive_layer_cm = depth_to_restrictive_layer_cm,
         restrictive_type = restrictive_layer_type,
         depth_15_percent_coarse_fragments_cm = depth_to_15_rock_cm,
         dominant_texture_40_cm = dominant_texture_at_40_cm,
         surface_water = surface_water_present) %>%
  select(all_of(template))

# Correct values ----
# Correct missing value for NLS3_351. depth to 15% coarse fragments is blank, assume zero based on depth to restrictive layer, absence of moss/duff, physiography
# Correct erroneous depth_water_cm data point for site NLS3_999: should be -999 not 0
environment_final = environment %>% 
  mutate(surface_water = if_else(surface_water == "no","FALSE","TRUE"),
         cryoturbation = if_else(cryoturbation == "no","FALSE","TRUE"),
         soil_class = replace_na(as.character(soil_class), "not determined"),
         dominant_texture_40_cm = replace_na(as.character(dominant_texture_40_cm), "NULL")) %>% 
  mutate(microtopography = case_when(microtopography == 'tussock' ~ 'tussocks',
                                     .default = microtopography),
         moisture_regime = case_when(moisture_regime == 'mesic-xeric heterogeneous' ~ 'mesic-xeric heterogenous',
                                     .default = moisture_regime),
         disturbance = case_when(disturbance == 'wildlife grazing' ~ 'wildlife foraging',
                                 .default = disturbance)) %>% 
  mutate(depth_15_percent_coarse_fragments_cm = if_else(site_visit_code == 'NLS3_351_20220706',
                                                        0,
                                                        depth_15_percent_coarse_fragments_cm),
         depth_water_cm = if_else(site_visit_code == "NLS3_999_20220705", -999, depth_water_cm))

# QA/QC ----

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
