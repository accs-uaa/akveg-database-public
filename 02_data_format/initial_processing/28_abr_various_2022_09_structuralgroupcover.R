# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Whole Tussock Cover for ABR Various 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-06-05
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Vegetation Cover for Whole Tussock Cover data" appends unique site visit identifiers, performs QA/QC checks, and enforces formatting to match the AKVEG template. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr, warn.conflicts=FALSE)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tidyr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '28_abr_various_2022')
template_folder = path(project_folder, "Data", "Data_Entry")
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public_read')

# Define datasets ----

# Define inputs
cover_input = path(source_folder, 'tnawrocki_deliverable_two_veg.txt')
site_visit_input = path(plot_folder, '03_sitevisit_abrvarious2022.csv')
template_input = path(template_folder, "09_Structural_Group_Cover.xlsx")

# Define output
structural_cover_output = path(plot_folder, '09_structuralgroupcover_abrvarious2022.csv')

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Read in data ----
site_visit_original = read_csv(site_visit_input, col_select=c('site_code',
                                                              'site_visit_id'))
cover_original = read_delim(cover_input, delim = '|', 
                                col_select=c('plot_id', 'needleleaf_tree_cover', 'dw_needleleaf_tree_cover', 'broadleaf_tree_cover', 'dw_broadleaf_tree_cover', 'tall_shrub_cover':'dw_shrub_cover', 'forbs_cover':'total_lichens_cover', 'grasses_cover', 'sedges_and_rushes_cover'))
template = colnames(read_xlsx(path=template_input))

# Query AKVEG database ----

# Import database connection function
connection_script = path(repository_folder,
                         'package_DataProcessing',
                         'connect_database_postgresql.R')
source(connection_script)

# Connect to the AKVEG PostgreSQL database
akveg_connection = connect_database_postgresql(authentication)

# Define query
query_structural = "SELECT *
FROM structural_group
ORDER BY structural_group;"

# Read SQL table as dataframe
structural_list = as_tibble(dbGetQuery(akveg_connection, query_structural))

# Append site visit code ----
# Format plot id column to match site code
structural_cover = cover_original %>%
  mutate(observe_year = case_when(str_starts(plot_id, 'SUWA_') ~ str_extract(plot_id, pattern='\\d{4}'),
                                  .default = NA),
         observe_year = case_when(!is.na(observe_year) ~ str_sub(observe_year, start=-2),
                                  .default = NA)) %>% 
  mutate(site_code = case_when(str_ends(plot_id, pattern="_\\d{4}") ~ str_remove(plot_id, pattern="_\\d{4}"),
                               str_ends(plot_id, pattern='-veg') ~ str_remove(plot_id, pattern='-veg'),
                               .default = plot_id)) %>% 
  mutate(site_code = case_when(str_starts(plot_id, 'SUWA_') ~ str_c(site_code,
                                                                    observe_year, 
                                                                    sep = ""),
                               .default = site_code)) %>% 
  right_join(site_visit_original, by='site_code') %>%  # Use right join to drop excluded sites
  rename(site_visit_code = site_visit_id) %>% 
  select(site_visit_code, needleleaf_tree_cover:sedges_and_rushes_cover)

## Ensure all entries have a site visit code
structural_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Convert columns to numeric ----
## Replace character values with either 0 or -999 to allow columns to be appropriately converted.
## NULL value only used for dwarf needleleaf/broadleaf cover for SUWA sites; dwarf trees will be merged with 'regular' tree category so actual value (whether truly missing or 0) does not matter.
structural_cover = structural_cover %>% 
  mutate(across(.cols = needleleaf_tree_cover:sedges_and_rushes_cover, 
            .fns = ~ str_replace(., "0E-16", "0")),
         across(.cols = needleleaf_tree_cover:sedges_and_rushes_cover, 
                 .fns = ~ str_replace(., "NULL", "-999")),
         across(.cols = needleleaf_tree_cover:sedges_and_rushes_cover, 
                .fns = ~ as.numeric(.)))

# Calculate missing categories ----
## For 'other lichens' and 'other bryophytes', species categories need to be subtracted from total cover
structural_cover = structural_cover %>% 
  mutate(other_bryophytes = case_when(total_mosses_cover < (sphagnum_cover + feathermoss_cover) ~ -999,
                                      feathermoss_cover == -999 ~ total_mosses_cover - sphagnum_cover,
                                      feathermoss_cover != -999 ~ total_mosses_cover - sphagnum_cover - feathermoss_cover),
         other_lichens = case_when(total_lichens_cover < cladonia_cladina_cover ~ -999,
                                   .default = total_lichens_cover - cladonia_cladina_cover)) %>% 
  select(-c(total_mosses_cover, total_lichens_cover))

# Create rows for missing groups w/ -999 to indicate data were not collected
missing_groups = data.frame(site_visit_code=rep(unique(structural_cover$site_visit_code), times=3),
                            structural_group = rep(c("aquatic plants", "prostrate shrub", "spore-bearing plant"), times=length(unique(structural_cover$site_visit_code))), 
                            structural_cover_percent=-999
                            )

# Format structural group cover ----
structural_cover = structural_cover %>% 
  pivot_longer(cols='needleleaf_tree_cover':'other_lichens',
               names_to = 'group_original',
               values_to = 'cover_percent') %>% 
  mutate(cover_percent = case_when(cover_percent == -997 ~ -999,
                                   .default = cover_percent)) %>% 
  filter(!(cover_percent == -999 & group_original %in% c("dw_needleleaf_tree_cover", "dw_broadleaf_tree_cover"))) %>% # Drop values that had been coded as "NULL"  
  mutate(group_original = str_replace(group_original, "dw_", "dwarf_"), 
         group_original = str_replace_all(group_original, "_", " "),
         group_original = str_remove_all(group_original, " cover"),
         structural_group = case_when(group_original == 'dwarf needleleaf tree' ~ 'needleleaf tree',
                                      group_original == 'dwarf broadleaf tree' ~ 'broadleaf tree',
                                      group_original == 'sphagnum' ~ 'Sphagnum mosses',
                                      group_original == 'feathermoss' ~ 'feathermosses',
                                      group_original == 'cladonia cladina' ~ 'light macrolichens',
                                      .default = group_original)) %>%  # Rename groups
  group_by(site_visit_code, structural_group) %>% 
  summarize(structural_cover_percent = sum(cover_percent)) %>% # Combine dwarf tree + 'regular' tree into single category 
  bind_rows(missing_groups) %>%  # Add missing groups
  mutate(structural_cover_type = 'absolute foliar cover',
         structural_cover_percent = signif(structural_cover_percent, digits = 3),
  ) %>% 
  select(all_of(template)) %>% 
  arrange(site_visit_code, structural_group, .locale = "en") %>%  # Enforce sorting by letter rather than case
  ungroup()

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(structural_cover, is.na)
    , sum))

# Are the values for percent cover between 0% and 100%?
structural_cover %>% 
  filter(structural_cover_percent != -999) %>% 
  summarise(min_percent = min(structural_cover_percent),
             max_percent = max(structural_cover_percent))

# Is there an entry for each structural group?
structural_cover %>% 
  group_by(site_visit_code) %>% 
  summarize(total_entries = n()) %>% 
  filter(total_entries != nrow(structural_list))

# Export data ----
write_csv(structural_cover, structural_cover_output)

# Clear workspace ----
rm(list=ls())
