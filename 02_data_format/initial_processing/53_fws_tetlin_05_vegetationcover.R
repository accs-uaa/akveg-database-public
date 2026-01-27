# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Tetlin 2022-2024 vegetation and abiotic cover data for AKVEG Database
# Author: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2026-01-26
# Usage: Must be executed in R version 4.5.1+.
# Description: "Format Tetlin 2022-2024 vegetation and abiotic cover data for AKVEG Database" formats vegetation cover and abiotic top cover data for entry into AKVEG Database. Two vegetation surveys were conducted in 2024. The two surveys used different methods.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr, warn.conflicts = FALSE)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tibble)
library(tidyr)

# Define folder structure ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/53_fws_tetlin_2024')
template_folder = path(project_folder, 'Data/Data_Entry')
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive, root_folder, 'Repositories', 'akveg-database-public')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public_read')

# Define inputs ----
site_2022_input = path(source_folder, 'Data_2022', 'TNWR_2022_points_sampled_field_data.csv')
species_2022_input = path(source_folder, 'Data_2022', 'TNWR_2022_species_dictionary.csv')
site_2024_input = path(source_folder, 'Data_2024', 'v2', 'TetlinNWR_2024_sites_compiled.csv')
species_2024_input = path(source_folder, 'Data_2024', 'v2', 'TetlinNWR_2024_species_compiled_v2.csv')
lpi_2024_input = path(source_folder, 'Data_2024', 'extra_sites', '05_fws_tetlin_2024_LPI.xlsx')
trace_2024_input = path(source_folder, 'Data_2024', 'extra_sites', '05_fws_tetlin_2024_trace.xlsx')
visit_input = path(plot_folder, '03_sitevisit_fwstetlin2024.csv')

# Define templates
vegetation_template_input = path(template_folder, '05_vegetation_cover.xlsx')
abiotic_template_input = path(template_folder, '06_abiotic_top_cover.xlsx')

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Define outputs ----
vegetation_output = path(plot_folder, '05_vegetationcover_fwstetlin2024.csv')
abiotic_output = path(plot_folder, '06_abiotictopcover_fwstetlin2024.csv')

# Read in input files ----
vegetation_template = colnames(read_xlsx(vegetation_template_input))
abiotic_template = colnames(read_xlsx(abiotic_template_input))
visit_original = read_csv(visit_input, col_select = c("site_code", "site_visit_code"))

#### Query AKVEG Database ----

# Import database connection function
connection_script = path(repository_folder,
                         'pull_functions','connect_database_postgresql.R')
source(connection_script)

# Connect to the AKVEG PostgreSQL database
akveg_connection = connect_database_postgresql(authentication)

# Define taxonomic query
query_taxa = "SELECT taxon_all.taxon_code as taxon_code
, taxon_all.taxon_name as taxon_name
, taxon_status.taxon_status as taxon_status
, taxon_accepted_name.taxon_name as taxon_name_accepted
FROM taxon_all
  LEFT JOIN taxon_accepted ON taxon_all.taxon_accepted_code = taxon_accepted.taxon_accepted_code
  LEFT JOIN taxon_all taxon_accepted_name ON taxon_all.taxon_accepted_code = taxon_accepted_name.taxon_code
  LEFT JOIN taxon_status ON taxon_all.taxon_status_id = taxon_status.taxon_status_id
ORDER BY taxon_name_accepted, taxon_status, taxon_name;"

## Read SQL table as dataframe
taxa_all = as_tibble(dbGetQuery(akveg_connection, query_taxa))

# Define abiotic element query
query_abiotic = "SELECT * FROM ground_element
WHERE ground_element.element_type <> 'ground';"

## Read SQL table as dataframe
abiotic_elements_list = as_tibble(dbGetQuery(akveg_connection, query_abiotic))

#### Parse 2022 data ----

# Read species codes and names
species_2022_data = read_csv(species_2022_input) %>%
  select(abbreviation, name_original) %>%
  mutate(abbreviation = str_to_upper(abbreviation))

# Prepare 2022 vegetation cover and abiotic top cover data
vegetation_2022 = read_csv(site_2022_input) %>%
  # Create site visit code
  mutate(site_visit_code = case_when(TransectID < 10 ~ paste('TET22_00', TransectID, '_20220715', sep = ''),
                                     TransectID < 100 ~ paste('TET22_0', TransectID, '_20220715', sep = ''),
                                     TRUE ~ paste('TET22_', TransectID, '_20220715', sep = ''))) %>%
  # Add vegetation cover metadata
  mutate(dead_status = 'FALSE',
         cover_type = 'top canopy cover') %>%
  # Format percent cover
  mutate(cover_percent = round(as.numeric(CoverTx), digits=3)) %>%
  # Join original species names
  mutate(SpeciesCode = str_to_upper(SpeciesCode)) %>%
  left_join(species_2022_data, by = join_by(SpeciesCode == abbreviation)) %>%
  # Join adjudicated and accepted names
  left_join(taxa_all, by = join_by(name_original == taxon_name), keep = TRUE) %>%
  rename(name_adjudicated = taxon_name) %>%
  # Adjudicate names
  mutate(name_adjudicated = case_when(name_original == 'Sagittatus species' ~ 'Sagittaria cuneata',
                                      grepl(" species", name_original) ~ str_remove(name_original, " species"),
                                      name_original == 'Arctostaphlyos rubra' ~ 'Arctostaphylos rubra',
                                      name_original == 'Arctostaphlyos uva-ursi' ~ 'Arctostaphylos uva-ursi',
                                      name_original == 'Calamagrostis purperea' ~ 'Calamagrostis purpurascens ssp. purpurascens',
                                      name_original == 'Carex capita' ~ 'Carex capitata',
                                      name_original == 'Epibolium angustifolium' ~ 'Epilobium angustifolium',
                                      name_original == 'Rubus articus' ~ 'Rubus arcticus',
                                      name_original == 'Vaccinium vitis-idea' ~ 'Vaccinium vitis-idaea',
                                      name_original == 'Lichen and mosses grouped' ~ 'unknown',
                                      TRUE ~ name_adjudicated)) %>%
  # Select final columns
  select(site_visit_code, name_original, name_adjudicated, cover_type, dead_status, cover_percent) %>%
  # Join adjudicated and accepted names
  left_join(taxa_all, by = join_by(name_adjudicated == taxon_name), keep = TRUE)

# Ensure range of percent cover values is reasonable
print(summary(vegetation_2022$cover_percent))

# Parse 2022 abiotic top cover
abiotic_2022 = vegetation_2022 %>%
  # Remove vegetation observations
  filter(is.na(name_adjudicated)) %>%
  # Assign abiotic element
  mutate(abiotic_element = case_when(name_original == 'Bare ground' ~ 'soil',
                                     name_original == 'Burned or fallen wood' ~ 'dead down wood (≥ 2 mm)',
                                     name_original == 'Dead graminoids and forbes' ~ 'litter (< 2 mm)',
                                     name_original == 'Submerged in water' ~ 'water',
                                     TRUE ~ 'error')) %>%
  # Change cover name
  rename(abiotic_top_cover_percent = cover_percent) %>%
  # Select final columns
  select(all_of(abiotic_template))

# Parse 2022 vegetation cover
vegetation_2022 = vegetation_2022 %>%
  # Remove abiotic classes
  filter(!is.na(name_adjudicated)) %>%
  # Remove absences
  filter(cover_percent != 0) %>%
  # Select final columns
  select(all_of(vegetation_template))

# Ensure there are no 'error' flags in abiotic dataset
print(abiotic_2022 %>% filter(abiotic_element == 'error') %>% nrow())

#### Parse 2024 data: First survey ----

# Parse 2024 abiotic top cover data
abiotic_2024_first = read_csv(site_2024_input) %>%
  # Create site code
  mutate(site_code = str_replace(Site_code, '-', '_')) %>%
  # Create site visit code
  mutate(site_visit_code = paste(site_code, '_', Date, sep='')) %>%
  # Select columns for pivot
  select(site_visit_code, Cover_litter, Cover_water, Cover_bare_rock, Cover_soil) %>%
  # Pivot data to long form
  pivot_longer(!site_visit_code, names_to = "abiotic_element", values_to = "abiotic_top_cover_percent") %>%
  # Format abiotic element names
  mutate(abiotic_element = case_when(abiotic_element == 'Cover_bare_rock' ~ 'rock fragments',
                                     abiotic_element == 'Cover_litter' ~ 'litter (< 2 mm)',
                                     abiotic_element == 'Cover_soil' ~ 'soil',
                                     abiotic_element == 'Cover_water' ~ 'water',
                                     TRUE ~ 'error')) %>%
  # Correct na to zero
  mutate(abiotic_top_cover_percent = case_when(is.na(abiotic_top_cover_percent) ~ 0,
                                               TRUE ~ abiotic_top_cover_percent))

# Parse 2024 non-vascular and lichen data
nonvascular_2024 = read_csv(site_2024_input) %>%
  # Create site code
  mutate(site_code = str_replace(Site_code, '-', '_')) %>%
  # Create site visit code
  mutate(site_visit_code = paste(site_code, '_', Date, sep='')) %>%
  # Select columns for pivot
  select(site_visit_code, Cover_lichens, Cover_sphagnum, Cover_feather_moss) %>%
  # Pivot data to long form
  pivot_longer(!site_visit_code, names_to = "name_original", values_to = "cover_percent") %>%
  # Format functional group names
  mutate(name_original = case_when(name_original == 'Cover_lichens' ~ 'lichen',
                                   name_original == 'Cover_sphagnum' ~ 'Sphagnum moss',
                                   name_original == 'Cover_feather_moss' ~ 'feathermoss (other)'))

# Parse 2024 vegetation cover data
vegetation_2024_first = read_csv(species_2024_input) %>%
  # Format site_code
  rename(site_code = 1) %>%
  mutate(site_code = str_replace(site_code, '-', '_')) %>%
  # Pivot data to long form
  pivot_longer(!site_code, names_to = 'name_original', values_to = 'cover_percent') %>%
  # Join site visit code
  left_join(visit_original, by = join_by('site_code' == 'site_code')) %>%
  # Select columns
  select(site_visit_code, name_original, cover_percent) %>%
  # Add non-vascular and lichen data
  bind_rows(nonvascular_2024) %>% 
  # Join taxon names
  mutate(name_adjudicated = name_original) %>%
  mutate(name_adjudicated = str_remove(name_adjudicated, ' species')) %>%
  mutate(name_adjudicated = str_replace(name_adjudicated, ' s. ', ' ssp. ')) %>%
  mutate(name_adjudicated = case_when(name_adjudicated == 'Arctostaphylos rubra' ~ 'Arctous rubra',
                                      .default = name_adjudicated)) %>%
  # Filter out zero and n/a values
  filter(!is.na(cover_percent)) %>%
  filter(cover_percent != 0) %>%
  # Join taxon data
  left_join(taxa_all, by = join_by('name_adjudicated' == 'taxon_name')) %>%
  # Add metadata
  mutate(dead_status = 'FALSE',
         cover_type = 'top canopy cover') %>%
  # Select final columns
  select(site_visit_code, name_original, name_adjudicated, cover_type, dead_status, cover_percent) %>%
  # Correct duplicates
  filter(site_visit_code != 'TET24_008_20240725' | name_original != 'Arctous rubra') %>%
  mutate(cover_percent = case_when(site_visit_code == 'TET24_008_20240725' & name_adjudicated == 'Arctous rubra' ~ 3.2,
                                   .default = round(cover_percent, digits=3)))

# Ensure there are no 'error' flags in abiotic dataset
print(abiotic_2024_first %>% 
        filter(abiotic_element == 'error') %>% 
        nrow())

#### Parse 2024 data: Second survey ----

# Parse LPI data
lpi_2024 = read_xlsx(lpi_2024_input) %>% 
  # Format site_code
  mutate(site_code = str_replace(site_code, '-', '_')) %>%
  # Append site visit code
  left_join(visit_original, by="site_code") %>% 
  # Convert to long format
  pivot_longer(cols = layer_1:layer_7,
               names_to = NULL,
               values_to = "taxon_code",
               values_drop_na = TRUE) %>% # Drop empty layers (no hits) 
  mutate(dead_status = if_else(grepl(pattern="-d", x=taxon_code), # Create dead status
                               "TRUE",
                               "FALSE"),
         # Remove dead status + basal hits designation
         taxon_code = str_replace_all(taxon_code, pattern=c("-d"="", "-b"="")), 
         # Convert to lowercase
         taxon_code = str_to_lower(taxon_code)) %>%
  # Exclude non-veg codes
  filter(!(taxon_code %in% c('l', 'os', 'wa', 'wl'))) %>% 
  # Correct unknown taxon code 'diantrif'
  mutate(taxon_code = case_when(taxon_code == 'diantrif' ~ 'mentri',
                                taxon_code == 'bc' ~ 'fgbiocru',
                                .default = taxon_code)) %>% 
  # Obtain name adjudicated
  left_join(taxa_all, by = join_by('taxon_code'))

# Ensure all taxon codes matched with a name in the checklist 
print(lpi_2024 %>% filter(is.na(taxon_name)))

# Calculate maximum number of hits per plot
## If a species were to occur on every point and every line, how many times could it appear per plot?
## Resulting value will be the denominator to calculate percent cover
max_hits = read_xlsx(lpi_2024_input) %>% 
  group_by(site_code) %>% 
  summarize(max_line = max(line),
            max_point = max(point)) %>% 
  mutate(max_hits = max_point * max_line) %>% 
  # Format site_code
  mutate(site_code = str_replace(site_code, '-', '_')) %>%
  # Append site visit code
  left_join(visit_original, by="site_code") %>% 
  select(site_visit_code, max_hits)

# Calculate cover percent
# Group by line & point to make sure species don't get counted twice per point
# Then group by plot
# Each species can appear a maximum of 120 times per plot
lpi_2024 = lpi_2024 %>% 
  group_by(site_visit_code, 
           line, 
           point, 
           taxon_name, 
           dead_status) %>% 
  summarize(hits = 1) %>% 
  group_by(site_visit_code, 
           taxon_name, 
           dead_status) %>% 
  summarize(total_hits = sum(hits)) %>% 
  left_join(max_hits, by="site_visit_code") %>% 
  mutate(cover_percent = round(total_hits/max_hits*100, 
                               digits = 3),
         cover_type = "absolute foliar cover") %>% 
  ungroup() %>% 
  select(-c(total_hits, max_hits))

# Format trace cover data
trace_2024 = read_xlsx(trace_2024_input) %>% 
  # Format site_code
  mutate(site_code = str_replace(site_code, '-', '_')) %>%
  # Append site visit code
  left_join(visit_original, by="site_code") %>% 
  # Add metadata
  mutate(cover_type = "absolute foliar cover",
         dead_status = "FALSE") %>% 
  select(-site_code) %>% 
  # Obtain name adjudicated
  left_join(taxa_all, by = join_by('taxon_code')) %>% 
  select(all_of(colnames(lpi_2024)))

# Ensure all taxon codes matched with a name in the checklist 
print(trace_2024 %>% filter(is.na(taxon_name)))

# Append trace species data
# Look for duplicates
# Create unique site-taxon-dead status combination on the LPI and the trace datasets
lpi_2024 = lpi_2024 %>% 
  mutate(site_taxon_id = paste(site_visit_code, 
                               taxon_name, 
                               dead_status,sep="_"))

trace_2024 = trace_2024 %>% 
  mutate(site_taxon_id = paste(site_visit_code, 
                               taxon_name, 
                               dead_status,sep="_"))

# Are there any duplicated entries within the trace data itself?
# These are worth checking manually to see if that is a field error or a data entry error, and if the cover percentages are the same
print(which(duplicated(trace_2024$site_taxon_id)))

# Are there any duplicated entries between the LPI and the trace data?
duplicate_entries = trace_2024 %>% 
  mutate(duplicated = site_taxon_id %in% lpi_2024$site_taxon_id) %>% 
  filter(duplicated == TRUE) %>% 
  select(site_taxon_id)

## In both cases, trace estimates are higher than LPI estimates; keep trace.
lpi_2024 = lpi_2024 %>% 
  filter(!(site_taxon_id %in% duplicate_entries$site_taxon_id))

# Append trace data to LPI data
vegetation_2024_second = bind_rows(lpi_2024, trace_2024)

# Perform final check for duplicated taxa
print(vegetation_2024_second[which(duplicated(vegetation_2024_second$site_taxon_id)),])

# Format to match data entry template
vegetation_2024_second = vegetation_2024_second %>% 
  rename(name_original = taxon_name) %>% 
  # No corrections made to name_original; name_original and name_adjudicated are the same
  mutate(name_adjudicated = name_original) %>% 
  arrange(site_visit_code) %>% 
  select(all_of(vegetation_template))

# Parse abiotic top cover data
abiotic_2024_second = read_xlsx(lpi_2024_input) %>% 
  # Format site_code
  mutate(site_code = str_replace(site_code, '-', '_')) %>%
  # Append site visit code
  left_join(visit_original, by="site_code") %>% 
  # Drop all layers except top-most (canopy) layer (layer 1)
  select(-c(layer_2:layer_7)) %>% 
  rename(taxon_code = layer_1) %>% 
  # Convert taxon codes to lowercase
  mutate(taxon_code = str_to_lower(taxon_code)) %>% 
  # Keep only abiotic codes
  filter(taxon_code %in% c('l', 'os', 'wa', 'wl')) %>% 
  # Convert abiotic codes to abiotic elements
  mutate(abiotic_element = case_when(taxon_code == 'l' ~ 'litter (< 2 mm)',
                                     taxon_code == 'wa' ~ 'water',
                                     taxon_code == 'wl' ~ 'dead down wood (≥ 2 mm)',
                                     taxon_code == 'os' ~ 'soil',
                                     .default = 'error')) %>% 
  # Calculate top cover percent for each plot
  mutate(hits = 1) %>% 
  group_by(site_visit_code, 
           abiotic_element) %>% 
  summarize(total_hits = sum(hits)) %>% 
  left_join(max_hits, by="site_visit_code") %>% 
  mutate(abiotic_top_cover_percent = round(total_hits/max_hits*100, 
                               digits = 3)) %>% 
  select(all_of(abiotic_template))

# Ensure there are no 'error' flags in abiotic dataset
print(abiotic_2024_second %>% 
        filter(abiotic_element == 'error') %>% 
        nrow())

#### Merge data ----
vegetation_data = rbind(vegetation_2022, vegetation_2024_first, vegetation_2024_second) %>%
  filter(site_visit_code != 'TET24_036_20240728' & site_visit_code != 'TET22_540_20220715')
abiotic_data = rbind(abiotic_2022, abiotic_2024_first, abiotic_2024_second) %>%
  filter(site_visit_code != 'TET24_036_20240728' & site_visit_code != 'TET22_540_20220715')

##### Add 'empty' abiotic elements ----
# Even abiotic elements with 0% must be listed for each site visit
complete_abiotic_cover <- abiotic_data %>%
  complete(
    site_visit_code, 
    abiotic_element = abiotic_elements_list$ground_element, 
    fill = list(abiotic_top_cover_percent = 0)
  ) %>%
  select(all_of(abiotic_template))

# Check for completeness
unique(table(complete_abiotic_cover$site_visit_code)) # 7 entries for each site (total number of abiotic elements)
unique(table(complete_abiotic_cover$abiotic_element)) # 115 entries for each abiotic element (total number of sites)

#### Export data ----
write_csv(vegetation_data, vegetation_output)
write_csv(complete_abiotic_cover, abiotic_output)
