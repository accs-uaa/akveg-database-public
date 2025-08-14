# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Vegetation Cover for ABR Various 2019 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-11-09
# Usage: Must be executed in R version 4.4.1+.
# Description: "Calculate Vegetation Cover for ABR Various 2019 data" uses data from vegetation surveys to calculate site-level percent foliar cover for each recorded species. It also appends unique site visit identifiers, corrects taxonomic names using the AKVEG database as a taxonomic standard, populates required metadata fields, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import libraries ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)

# Define directories ---- 

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 
                      'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '27_abr_various_2019')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, 'Data', 'Data_Entry')

# Set repository directory
repository_folder = path(drive,root_folder, 'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_private_read')

# Define datasets ----

# Define input datasets
veg_cover_input = path(source_folder, 'deliverable_tnawrocki_veg_cover.xlsx')
site_visit_input = path(plot_folder, '03_sitevisit_abrvarious2019.csv')
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define output dataset
veg_cover_output = path(plot_folder, '05_vegetationcover_abrvarious2019.csv')

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_private.csv')

# Read in data ----
veg_cover_original = read_xlsx(veg_cover_input, 
                               col_type = c('numeric', 'skip', 'text', 'skip', 
                                            'skip', 'text', 'text', 'numeric', 
                                            rep('skip', 14))) # Specify column types to avoid warnings
site_visit_original = read_csv(site_visit_input)
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
query_taxa = "SELECT taxon_all.taxon_code as taxon_code
, taxon_all.taxon_name as taxon_name
, taxon_accepted_name.taxon_name as taxon_name_accepted
FROM taxon_all
  LEFT JOIN taxon_accepted ON taxon_all.taxon_accepted_code = taxon_accepted.taxon_accepted_code
  LEFT JOIN taxon_all taxon_accepted_name ON taxon_all.taxon_accepted_code = taxon_accepted_name.taxon_code
ORDER BY taxon_name_accepted, taxon_name;"

# Read SQL table as dataframe
taxa_all = as_tibble(dbGetQuery(akveg_connection, query_taxa))

# Remove duplicates ----
# Some entries are duplicated, as indicated by the same 'serial id' number being used a set number of times for certain sites
veg_cover_original %>% filter(is.na(serial)) %>% nrow() # Ensure every entry has a serial number

veg_cover_data = veg_cover_original %>% 
  distinct(serial, .keep_all = TRUE) # More than 50,000 records will be dropped

# Format site code ----
# Drop year suffix 
# Capitalize all site codes for consistency
# Convert dashes to underscores
veg_cover_data = veg_cover_data %>% 
  mutate(site_code = str_remove(plot_id, "_\\d+$|-\\d+$"),
         site_code = str_to_upper(site_code),
         site_code = str_replace_all(site_code, "-", "_"))

# Append site visit code ----
veg_cover_data = site_visit_original %>% 
  select(site_code, site_visit_code) %>% 
  left_join(veg_cover_data, by = 'site_code') # Drop problem sites. Join ensures that all sites in the veg cover table are associated with a site in the site visit table

# Ensure correct number of sites
length(unique(veg_cover_data$site_code))
veg_cover_data %>% filter(is.na(plot_id)) # Looking for nulls in the plot id column ensures that all sites in the site visit table have been linked to entries in the veg cover table

# Explore cover data ----
summary(veg_cover_data$cover_percent) # Range is between 0 and 100

veg_cover_data %>% 
  filter(is.na(veg_taxonomy)) # All entries are associated with a scientific name

# Drop rows with no % cover (n=2). One of them has a note that it was found off plot
veg_cover_data = veg_cover_data %>% 
  filter(!is.na(cover_percent))

# Remove abiotic elements from plant species list ----
abiotic_elements = c("Bare Mineral Soil", "Bare Rock", 'Cryptobiotic Crust', "Gravel", 'Litter', "Moose Scat", 'Snow and Ice', "Water")

veg_cover_data = veg_cover_data %>% 
  filter(!(veg_taxonomy %in% abiotic_elements))

# Obtain accepted taxonomic name ----

# Standardize spelling conventions to match AKVEG checklist
# Remove 'sp.' at the end of genera names
# Correct minor spelling errors
veg_cover_taxa = veg_cover_data %>%
  rename(name_original = veg_taxonomy) %>% 
  filter(name_original != 'Unknown fungus') %>% 
  mutate(name_original = str_remove(name_original, " sp\\."),
         name_original = str_remove(name_original, 'Unidentified '),
         name_original = str_remove(name_original, 'Unknown '),
         name_original = str_replace(name_original, 'arborial', 'arboreal'),
         name_original = str_remove(name_original, ' hybrids')) %>% 
  mutate(name_original = case_when(str_starts(name_original, 'Unspecified moss') ~ 'moss',
                                   str_starts(name_original, '[a-zA-Z]+ \\d') ~ str_remove(name_original, ' \\d'),
                                   name_original == 'Androsace chamaejasme ssp. lehmannia' ~ 'Androsace chamaejasme ssp. lehmanniana',
                                   name_original == 'Carex albo-nigra' ~ 'Carex albonigra',
                                   name_original == 'Carex amblyorhyncha' ~ 'Carex amblyorrhyncha',
                                   name_original == 'Cladothamnus pyrolaeflorus' ~ 'Cladothamnus pyroliflorus',
                                   name_original == 'Corallorrhiza trifida' ~ 'Corallorhiza trifida',
                                   name_original == 'Crucifer' ~ 'Brassicaceae',
                                   name_original == 'Hedysarum mackenzii' ~ 'Hedysarum mackenziei',
                                   name_original == 'Nuphar polysepalum' ~ 'Nuphar polysepala',
                                   name_original == 'Oxytropis deflexa var. foliosa' ~ 'Oxytropis deflexa var. foliolosa',
                                   name_original == 'Papaver radicatum ssp. kluanensis' ~ 'Papaver radicatum ssp. kluanense',
                                   name_original == 'Picea x lutzii' ~ 'Picea ×lutzii',
                                   name_original == 'Potentilla egedii ssp. grandis' ~ 'Potentilla egedii var. grandis',
                                   name_original == 'Potentilla pennsylvanica' ~ 'Potentilla pensylvanica',
                                   name_original == 'Ranunculus gmelinii ssp. gmelini' ~ 'Ranunculus gmelinii ssp. gmelinii',
                                   .default = name_original)) 

# Join with AKVEG comprehensive checklist
veg_cover_taxa = veg_cover_taxa %>%
  left_join(taxa_all, by=c("name_original" = "taxon_name")) %>% 
  rename(name_adjudicated = taxon_name_accepted)

# Correct remaining codes
veg_cover_taxa = veg_cover_taxa %>% 
  mutate(name_adjudicated = case_when(name_original == 'Antennaria neglecta ssp. howellii' ~ 'Antennaria',
                                      name_original == 'Arabidopsis lyrata ssp. lyrata' ~ 'Arabidopsis kamchatica', 
                                      name_original == 'Astragalus adsurgens' ~ 'Astragalus laxmannii var. tananaicus',
                                      name_original == 'Brachythecium collinum' ~ 'Sciuro-hypnum oedipodium',
                                      name_original == 'Brassicaceae' ~ 'forb',
                                      name_original == 'Carduus tenuiflorus' ~ 'Carex tenuiflora', # Based on provided taxon code, Carduus tenuiflorus likely a data entry mistake
                                      name_original == 'Chenopodium glaucum' ~ 'Oxybasis salina',
                                      name_original == 'Chrysanthemum' ~ 'forb',
                                      name_original == 'Coeloglossum viride ssp. viride' ~ 'Coeloglossum viride',
                                      name_original == 'Compositae' ~ 'forb',
                                      name_original == 'crustose lichen, light' ~ 'crustose lichen (non-orange)',
                                      name_original == 'Dwarf Ericaceous Shrub' ~ 'shrub dwarf',
                                      name_original == 'Elymus trachycaulus ssp. novae-angliae' ~ 'Elymus trachycaulus ssp. trachycaulus',
                                      name_original == 'Euphrasia disjuncta' ~ 'Euphrasia subarctica',
                                      name_original == 'Festuca vivipara' ~ 'Festuca viviparoidea',
                                      name_original == 'foliose lichen, dark' ~ 'foliose lichen',
                                      name_original == 'foliose/fruticose lichen' ~ 'lichen',
                                      str_starts(name_original, 'fruticose lichen,') ~ 'fruticose lichen',
                                      name_original == 'Fucus' ~ 'algae',
                                      name_original == 'Gentiana propinqua ssp. propinqua' ~ 'Gentianella propinqua ssp. propinqua',
                                      name_original == 'grass' ~ 'grass (Poaceae)',
                                      name_original == 'Hepaticae' ~ 'liverwort',
                                      name_original == 'legume' ~ 'forb',
                                      name_original == 'Luzula multiflora ssp. multiflora var. kjellmaniana' ~ 'Luzula kjellmaniana',
                                      name_original == 'Luzula wahlenbergii ssp. wahlenbergii' ~ 'Luzula wahlenbergii',
                                      name_original == 'Melandrium apetalum' ~ 'Silene uralensis',
                                      name_original == 'Montia fontana ssp. fontana' ~ 'Montia fontana',
                                      name_original == 'Myriophyllum spicatum' ~ 'Myriophyllum sibiricum',
                                      name_original == 'orchid' ~ 'forb',
                                      name_original == 'Papaver lapponicum ssp. porsildii' ~ 'Papaver hultenii',
                                      name_original == 'Plantago maritima ssp. juncoides' ~ 'Plantago maritima',
                                      name_original == 'Primula stricta' ~ 'Primula egaliksensis',
                                      name_original == 'Pyrola secunda ssp. secunda' ~ 'Orthilia secunda',
                                      name_original == 'mint' ~ 'forb',
                                      name_original == 'Minuartia' ~ 'forb',
                                      name_original == 'Rumex acetosa ssp. alpestris' ~ 'Rumex acetosa',
                                      name_original == 'Scrophulariaceae' ~ 'forb',
                                      name_original == 'sedge' ~ 'sedge (Cyperaceae)',
                                      name_original == 'Solidago decumbens var. oreophila' ~ 'Solidago glutinosa',
                                      name_original == 'Solidago multiradiata var. multiradiata' ~ 'Solidago multiradiata',
                                      str_starts(name_original, 'Sphagnum \\(') ~ 'Sphagnum',
                                      name_original == 'Sphagnum fallax' ~ 'Sphagnum angustifolium',
                                      name_original == 'Sphagnum flexuosum' ~ 'Sphagnum angustifolium',
                                      name_original == 'Stellaria sitchana var. sitchana' ~ 'Stellaria borealis ssp. sitchana',
                                      name_original == 'Trientalis europaea ssp. europaea' ~ 'Lysimachia europaea',
                                      name_original == 'Umbelliferae' ~ 'forb',
                                      name_original == 'Xanthoria' ~ 'foliose lichen',
                                      .default = name_adjudicated)) %>% 
  mutate(name_adjudicated = case_when(is.na(name_adjudicated) & 
                                        grepl('^[A-Z][a-z]+ [a-z]+$', name_original) ~ str_split_i(name_original, ' ', i = 1),
                                      .default = name_adjudicated))

# Ensure that there are no other entries w/o a name_adjudicated
veg_cover_taxa %>% 
       filter(is.na(name_adjudicated)) %>% 
       distinct(name_original)

# Ensure that all accepted names are in the AKVEG checklist
which(!(unique(veg_cover_taxa$name_adjudicated) %in% unique(taxa_all$taxon_name_accepted)))

# Summarize percent cover ----
# Cover percent for some species split by wetland strata (tree vs sapling), or two unknowns need to be combined e.g., in a single 'moss' category
veg_cover_distinct = veg_cover_taxa %>% 
  group_by(site_visit_code, name_original, name_adjudicated) %>% 
  summarize(cover_percent = sum(cover_percent))

# Populate remaining columns ----
veg_cover_final = veg_cover_distinct %>%
  mutate(cover_type = "absolute foliar cover",
         dead_status = "FALSE",
         cover_percent = signif(cover_percent, digits = 3)) %>% # Round to 3 decimal places
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_cover_final, is.na)
    , sum))

# Are the values for percent cover between 0% and 100%?
summary(veg_cover_final$cover_percent)

# Are the correct number of sites included?
nrow(site_visit_original) == length(unique(veg_cover_final$site_visit_code))

missing_sites = site_visit_original[which(!(site_visit_original$site_visit_code %in% unique(veg_cover_final$site_visit_code))), 3]

# Missing sites are sites with only abiotic cover - OK to proceed
veg_cover_original %>%
  mutate(site_code = str_remove(plot_id, "_\\d+$|-\\d+$"),
         site_code = str_to_upper(site_code),
         site_code = str_replace_all(site_code, "-", "_")) %>% 
  filter(site_code %in% missing_sites$site_code) %>% 
  distinct(veg_taxonomy)

# Are there any duplicates?
# Consider entries with different percent cover as well (indicates that summary step was not completed)
veg_cover_final %>% 
  distinct(site_visit_code, name_adjudicated, dead_status) %>% 
  nrow() == nrow(veg_cover_final) # If TRUE, no duplicates to address

# Export data ----
write_csv(veg_cover_final, veg_cover_output)

# Clean workspace ----
rm(list=ls())
