# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Create template file for Site table
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-30
# Usage: Should be executed in R 4.4.1+.
# Description: "Create template file for Site table" reads in a .R script file and replaces values as specified in a CSV configuration file. The output is a .R script file with appropriate file paths and project code values that can then be modified for data ingestion needs.
# Credit: Creating template files with R. Nicola Rennie. August 22, 2023. nrennie.rbind.io/blog/script-templates-r
# ---------------------------------------------------------------------------

# Load required packages
library(stringr)

# Define directories
repository_folder = "C:/ACCS_Work/Repositories/akveg-database"
config_folder = "C:/ACCS_Work/Projects/AKVEG_Database"
data_folder = "C:/ACCS_Work/OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database/Data/Data_Plots"
script_folder = file.path(repository_folder, '02_data_format/initial_processing')

# Define input files
template_file = file.path(repository_folder, "package_DataProcessing/template_02_site.R")
config_file =  file.path(config_folder, 'template_config.csv')

# Read in template file
template_txt = readLines(template_file)
parameters = read.csv(config_file, fileEncoding = 'UTF-8')

# Parse authentication parameters from csv
project_name = parameters[which(parameters$parameter == 'project_folder'), 'value']
site_data = parameters[which(parameters$parameter == 'site_path'), 'value']

# Define replacement values
project_folder_path = file.path(data_folder, project_folder)
project_code = str_remove(project_name, "^\\d{2}_")
project_code_compact = str_remove_all(project_code, "_")
source_folder = file.path(data_folder, project_folder, "source")
site_data_path = file.path(source_folder, site_data)

# Replace values in template file ----
template_txt = gsub(pattern = "code_for_output", 
             replacement = paste0("'", project_code_compact, "'"),
             x = template_txt)
template_txt = gsub(pattern = "code_name", 
                    replacement = paste0("'", project_code, "'"),
                    x = template_txt)
template_txt = gsub(pattern = "plot_folder_path", 
             replacement = paste0("'", project_folder_path, "'"), 
             x = template_txt)
template_txt = gsub(pattern = "site_path", 
             replacement = paste0("'", site_data_path, "'"),
             x = template_txt)

# If needed, replace read_csv with read_xlsx
if (endsWith(site_data_path,"xlsx")) {
  message("Source file format XLSX")
  template_txt = gsub(pattern = "site_original = read_csv",
               replacement = "site_original = read_xlsx",
               x = template_txt)
} else {
  message("Source file format CSV")
}

# Write to new file ----

# Define output
output_file = paste(project_name, "02_site.R", sep = "_")
output_path = file.path(script_folder, output_file)

if (!file.exists(output_path)) {
  writeLines(template_txt, con = output_path)
  message(paste(output_file, "copied to", script_folder))
} else {
  message("File already exists.")
}

# Clear workspace ----
rm(list=ls())
