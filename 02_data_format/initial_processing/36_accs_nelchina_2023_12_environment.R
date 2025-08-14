# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Environment for ACCS Nelchina 2023 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-12-05
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Environment for ACCS Nelchina 2023 data" appends unique site visit identifier, ensures formatting matches the AKVEG template, and performs QA/QC checks to ensure values match constrained fields and are within a reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
project_folder = path(drive, 
                      root_folder, 
                      'OneDrive - University of Alaska', 
                      'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '36_accs_nelchina_2023')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, 'Data', 'Data_Entry')

# Define files ----
# Define input datasets
environment_input = path(source_folder, "12_accs_nelchina_envr_2023.xlsx")
site_visit_input = path(plot_folder, "03_sitevisit_accsnelchina2023.csv")
dictionary_input = path(project_folder, 'Data', "Tables_Metadata", 'database_dictionary.xlsx')
template_input = path(template_folder, "12_environment.xlsx")

# Define output datasets
environment_output = path(plot_folder, '12_environment_accsnelchina2023.csv')

# Read in data ----
environment_original = read_xlsx(environment_input)
site_visit_original = read_csv(site_visit_input, show_col_types = FALSE, col_select= c('site_code', 'site_visit_code'))
dictionary = read_xlsx(dictionary_input)
template = colnames(read_xlsx(template_input))

# Append site visit code ----
environment = environment_original %>%
  select(-site_visit_code) %>% 
  left_join(site_visit_original, by="site_code") %>% # Append site visit code
  select(all_of(template)) # Keep only required columns

# Ensure all site visit codes are accounted for
environment %>% distinct(site_visit_code) %>% nrow() == nrow(environment)
environment %>% filter(is.na(site_visit_code))
which(!(environment$site_visit_code) %in% site_visit_original$site_visit_code)
which(!(site_visit_original$site_visit_code %in% environment$site_visit_code))

# Format macrotopography ----
# Replace values to match database dictionary

environment = environment %>% 
  mutate(macrotopography = case_when(macrotopography == 'lakebed' ~ 'lake bed',
                                     macrotopography == 'floodplain overflow' ~ 'floodplain overflow channel',
                                     macrotopography == 'drainage' ~ 'NULL',
                                     .default = macrotopography))

# Format microtography ----
environment = environment %>% 
  mutate(microtopography = case_when(site_visit_code == 'NLC_201_20230626' ~ 'drainageway',
                                     microtopography == 'soil-cover rocks' ~ 'soil-covered rocks',
                                     microtopography == 'treads-risers' ~ 'treads and risers',
                                     microtopography == 'tussock' ~ 'tussocks',
                                     .default = microtopography))

# Format moisture regime ----
environment = environment %>% 
  mutate(moisture_regime = case_when(moisture_regime == 'mesic-xeric' ~ 'mesic-xeric heterogenous',
                                     moisture_regime == 'hygric-mesic' ~ 'hygric-mesic heterogenous',
                                     moisture_regime == 'aquatic-hydric' ~ 'aquatic-hydric heterogenous',
                                     moisture_regime == 'hydric-hygric' ~ 'hydric-hygric heterogenous',
                                     
                                     .default = moisture_regime))

# Format drainage ----
environment = environment %>% 
  mutate(drainage = case_when(drainage == 'moderate' ~ 'moderately drained',
                              drainage == 'well' ~ 'well drained',
                              drainage == 'poor' ~ 'poorly drained',
                              .default = drainage))

# Format disturbance ----
environment = environment %>% 
  mutate(disturbance = case_when(disturbance == 'wildlife grazing' ~ 'wildlife foraging',
                                 disturbance == 'geomorphic' ~ 'geomorphic process',
                                 disturbance == 'fluvial' ~ 'riparian',
                                 .default = disturbance))

# Format restrictive type ----
environment = environment %>% 
  mutate(restrictive_type = case_when(restrictive_type == 'densic' ~ 'densic layer',
                                      .default = restrictive_type))

# Format soil class
environment = environment %>% 
  mutate(soil_class = case_when(soil_class == 'NULL' ~ 'not determined',
                                .default = soil_class))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(environment, is.na)
    , sum)
)

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
  
  unique_values = environment %>% 
    select(all_of(variable)) %>% 
    distinct()
  
  colnames(unique_values) = 'values'
  
  does_not_exist = which(!(unique_values$values %in% constrained_values$data_attribute))
  
  if (length(does_not_exist) != 0) {
    cat('check', variable, '\n')
  }
}

# Verify that pairing between geomorphology and physiography makes sense
table(environment$geomorphology, environment$physiography)

# Verify values for numerical data
table(environment$disturbance_severity, 
      environment$disturbance_time_y) # Most no data (-999) values are NULL

# Depth of moss/duff
environment %>% filter(depth_moss_duff_cm != -999) %>% 
  ggplot(aes(x=depth_moss_duff_cm)) + 
  geom_histogram(color="black", fill="white", binwidth = 2) +
  theme_classic()

# Depth of restrictive layer
environment %>% filter(depth_restrictive_layer_cm != -999) %>% 
  ggplot(aes(x=depth_restrictive_layer_cm)) + 
  geom_histogram(color="black", fill="white", binwidth = 5) +
  theme_classic()

# Height of microrelief
environment %>% filter(microrelief_cm != -999) %>% 
  ggplot(aes(x=microrelief_cm)) + 
  geom_histogram(color="black", fill="white", binwidth = 10) +
  theme_classic()

# Depth at 15% coarse fragments
environment %>% filter(depth_15_percent_coarse_fragments_cm != -999) %>% 
  ggplot(aes(x=depth_15_percent_coarse_fragments_cm)) + 
  geom_histogram(color="black", fill="white", binwidth = 5) +
  theme_classic()

# Export data ----
write_csv(environment,file=environment_output)

# Clean workspace ----
rm(list=ls())
