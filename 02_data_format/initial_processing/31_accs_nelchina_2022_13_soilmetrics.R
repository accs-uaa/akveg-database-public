# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Soils for ACCS Nelchina data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-03-28
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Soils for ACCS Nelchina data" appends unique site visit identifier, corrects missing values, ensures formatting matches the AKVEG template, and performs QA/QC checks to ensure values match constrained fields and are within a reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(fs)
library(ggplot2)
library(readr)
library(readxl)

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
soil_metrics_input = path(source_folder, "Nelchina_2022_Environment.xlsx")
site_visit_input = path(plot_folder, "03_sitevisit_accsnelchina2022.csv")
template_input = path(template_folder, "13_soil_metrics.xlsx")

# Define output dataset
soil_metrics_output = path(plot_folder, '13_soilmetrics_accsnelchina2022.csv')

# Read in data ----
site_visit_original = read_csv(site_visit_input, show_col_types = FALSE, col_select= c('site_code', 'site_visit_id'))
soil_metrics_original = read_xlsx(soil_metrics_input)
template = colnames(read_xlsx(path=template_input))

# Append site visit code ----
soil_metrics = site_visit_original %>% 
  left_join(soil_metrics_original, join_by('site_code'==`Site Code`))

# Rename & select relevant columns ----
soil_metrics = soil_metrics %>% 
  rename(site_visit_code = site_visit_id, 
         measure_depth_cm = `Soil Measurement Depth (cm)`, 
         ph = `Soil pH`, 
         conductivity_mus = `Soil Conductivity (μS)`, 
         temperature_deg_c = `Soil Temperature (°C)`) %>% 
  mutate(water_measurement = 'FALSE') %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(soil_metrics, is.na)
    , sum)
)

# Verify values are within a reasonable range
table(soil_metrics$water_measurement) # should be Boolean

soil_metrics %>% 
  filter(ph != -999) %>% 
  summarize(minim = min(ph), maxim = max(ph), 
            average = mean(ph)) # Manually check and values below 4 or above 7.5

soil_metrics %>%
  filter(conductivity_mus != -999) %>% 
  ggplot(aes(x=conductivity_mus)) + 
  geom_histogram(color="black", fill="#228833", binwidth = 15) +
  theme_classic()

soil_metrics %>% filter(temperature_deg_c != -999) %>%
  ggplot(aes(x=temperature_deg_c)) + 
  geom_histogram(color="black", fill="#CCBB44", binwidth = 2) +
  theme_classic()

# Export data ----
write_csv(soil_metrics,file=soil_metrics_output)
