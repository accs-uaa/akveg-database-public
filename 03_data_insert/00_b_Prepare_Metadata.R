# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Prepare metadata and constraints for upload
# Author: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-09-24
# Usage: Script should be executed in R 4.5.1+.
# Description: "Prepare metadata and constraints for upload" parses metadata and constraints into a SQL query for upload into empty tables.
# ---------------------------------------------------------------------------

# Import required libraries
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)

# Set root directory
drive <- "C:"
root_folder <- "ACCS_Work/OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database/Data"

# Define input folders
data_folder <- path(
  drive,
  root_folder,
  "Tables_Metadata"
)
output_folder <- path(drive, root_folder, "Data_Plots", "sql_statements")

# Designate output sql file
sql_metadata <- path(
  output_folder,
  "00_b_Insert_Metadata.sql"
)

# Identify metadata tables
dictionary_file <- path(
  data_folder,
  "database_dictionary.xlsx"
)
schema_file <- path(
  data_folder,
  "database_schema.xlsx"
)
organization_file <- path(
  data_folder,
  "organization.xlsx"
)

# Read taxonomy tables into dataframes
dictionary_data <- read_excel(dictionary_file, sheet = "dictionary")
schema_data <- read_excel(schema_file, sheet = "schema")
organization_data <- read_excel(organization_file, sheet = "organization")

# Parse constraint tables
completion_table <- dictionary_data %>%
  filter(field == "completion") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(completion_id = data_attribute_id) %>%
  rename(completion = data_attribute)
cover_method_table <- dictionary_data %>%
  filter(field == "cover_method") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(cover_method_id = data_attribute_id) %>%
  rename(cover_method = data_attribute)
cover_type_table <- dictionary_data %>%
  filter(field == "cover_type") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(cover_type_id = data_attribute_id) %>%
  rename(cover_type = data_attribute)
crown_class_table <- dictionary_data %>%
  filter(field == "crown_class") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(crown_class_id = data_attribute_id) %>%
  rename(crown_class = data_attribute)
data_tier_table <- dictionary_data %>%
  filter(field == "data_tier") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(data_tier_id = data_attribute_id) %>%
  rename(data_tier = data_attribute)
data_type_table <- dictionary_data %>%
  filter(field == "data_type") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(data_type_id = data_attribute_id) %>%
  rename(data_type = data_attribute)
disturbance_table <- dictionary_data %>%
  filter(field == "disturbance") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(disturbance_id = data_attribute_id) %>%
  rename(disturbance = data_attribute)
disturbance_severity_table <- dictionary_data %>%
  filter(field == "disturbance_severity") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(disturbance_severity_id = data_attribute_id) %>%
  rename(disturbance_severity = data_attribute)
drainage_table <- dictionary_data %>%
  filter(field == "drainage") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(drainage_id = data_attribute_id) %>%
  rename(drainage = data_attribute)
geomorphology_table <- dictionary_data %>%
  filter(field == "geomorphology") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(geomorphology_id = data_attribute_id) %>%
  rename(geomorphology = data_attribute)
ground_element_table <- dictionary_data %>%
  filter(field == "ground_element") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(ground_element_code = data_attribute_id) %>%
  rename(ground_element = data_attribute)
h_datum_table <- dictionary_data %>%
  filter(field == "h_datum") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(h_datum_epsg = data_attribute_id) %>%
  rename(h_datum = data_attribute)
height_type_table <- dictionary_data %>%
  filter(field == "height_type") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(height_type_id = data_attribute_id) %>%
  rename(height_type = data_attribute)
location_type_table <- dictionary_data %>%
  filter(field == "location_type") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(location_type_id = data_attribute_id) %>%
  rename(location_type = data_attribute)
macrotopography_table <- dictionary_data %>%
  filter(field == "macrotopography") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(macrotopography_id = data_attribute_id) %>%
  rename(macrotopography = data_attribute)
microtopography_table <- dictionary_data %>%
  filter(field == "microtopography") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(microtopography_id = data_attribute_id) %>%
  rename(microtopography = data_attribute)
moisture_table <- dictionary_data %>%
  filter(field == "moisture") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(moisture_id = data_attribute_id) %>%
  rename(moisture = data_attribute)
organization_type_table <- dictionary_data %>%
  filter(field == "organization_type") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(organization_type_id = data_attribute_id) %>%
  rename(organization_type = data_attribute)
personnel_table <- dictionary_data %>%
  filter(field == "personnel") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(personnel_id = data_attribute_id) %>%
  rename(personnel = data_attribute)
perspective_table <- dictionary_data %>%
  filter(field == "perspective") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(perspective_id = data_attribute_id) %>%
  rename(perspective = data_attribute)
physiography_table <- dictionary_data %>%
  filter(field == "physiography") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(physiography_id = data_attribute_id) %>%
  rename(physiography = data_attribute)
plot_dimensions_table <- dictionary_data %>%
  filter(field == "plot_dimensions_m") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(plot_dimensions_id = data_attribute_id) %>%
  rename(plot_dimensions_m = data_attribute)
positional_accuracy_table <- dictionary_data %>%
  filter(field == "positional_accuracy") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(positional_accuracy_id = data_attribute_id) %>%
  rename(positional_accuracy = data_attribute)
restrictive_type_table <- dictionary_data %>%
  filter(field == "restrictive_type") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(restrictive_type_id = data_attribute_id) %>%
  rename(restrictive_type = data_attribute)
schema_category_table <- dictionary_data %>%
  filter(field == "schema_category") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(schema_category_id = data_attribute_id) %>%
  rename(schema_category = data_attribute)
schema_table_table <- dictionary_data %>%
  filter(field == "schema_table") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(schema_table_id = data_attribute_id) %>%
  rename(schema_table = data_attribute)
scope_table <- dictionary_data %>%
  filter(field == "scope") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(scope_id = data_attribute_id) %>%
  rename(scope = data_attribute)
shrub_class_table <- dictionary_data %>%
  filter(field == "shrub_class") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(shrub_class_id = data_attribute_id) %>%
  rename(shrub_class = data_attribute)
soil_class_table <- dictionary_data %>%
  filter(field == "soil_class") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(soil_class_id = data_attribute_id) %>%
  rename(soil_class = data_attribute)
soil_nonmatrix_table <- dictionary_data %>%
  filter(field == "nonmatrix_feature") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(nonmatrix_feature_code = data_attribute_id) %>%
  rename(nonmatrix_feature = data_attribute)
soil_structure_table <- dictionary_data %>%
  filter(field == "soil_structure") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(soil_structure_code = data_attribute_id) %>%
  rename(soil_structure = data_attribute)
soil_texture_table <- dictionary_data %>%
  filter(field == "soil_texture") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(soil_texture_code = data_attribute_id) %>%
  rename(soil_texture = data_attribute)
soil_horizon_type_table <- dictionary_data %>%
  filter(field == "soil_horizon_type") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(soil_horizon_type_code = data_attribute_id) %>%
  rename(soil_horizon_type = data_attribute)
soil_horizon_suffix_table <- dictionary_data %>%
  filter(field == "soil_horizon_suffix") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(soil_horizon_suffix_code = data_attribute_id) %>%
  rename(soil_horizon_suffix = data_attribute)
soil_hue_table <- dictionary_data %>%
  filter(field == "soil_hue") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(soil_hue_code = data_attribute_id) %>%
  rename(soil_hue = data_attribute)
structural_class_table <- dictionary_data %>%
  filter(field == "structural_class") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(structural_class_code = data_attribute_id) %>%
  rename(structural_class = data_attribute)
structural_group_table <- dictionary_data %>%
  filter(field == "structural_group") %>%
  select(data_attribute_id, data_attribute) %>%
  rename(structural_group_id = data_attribute_id) %>%
  rename(structural_group = data_attribute)

# Parse organization table
organization_table <- organization_data %>%
  left_join(organization_type_table, by = "organization_type") %>%
  select(organization_id, organization, organization_type_id)

# Parse schema table
database_schema_table <- schema_data %>%
  left_join(schema_category_table, by = "schema_category") %>%
  left_join(schema_table_table, by = c("link_table" = "schema_table")) %>%
  rename(link_table_id = schema_table_id) %>%
  left_join(schema_table_table, by = "schema_table") %>%
  left_join(data_type_table, by = "data_type") %>%
  rowid_to_column("field_id") %>%
  mutate(link_table_id = replace_na(link_table_id, "NULL")) %>%
  select(field_id, standards_section, schema_category_id, schema_table_id, field, data_type_id, field_length, is_unique, is_key, required, link_table_id, field_description) %>%
  mutate_if(is.character,
    str_replace_all,
    pattern = "'NA'", replacement = "NULL"
  )

# Parse dictionary table
database_dictionary_table <- dictionary_data %>%
  left_join(database_schema_table, by = "field") %>%
  rowid_to_column("dictionary_id") %>%
  select(dictionary_id, field_id, data_attribute_id, data_attribute, definition)

# Add column to ground_element table
abiotic_elements <- c("rock fragments", "soil")
ground_elements <- c("animal litter", "biotic", "gravel", "cobble", "stone", "boulder", "mineral soil", "organic soil")

ground_element_table <- ground_element_table %>%
  mutate(element_type = case_when(ground_element %in% abiotic_elements ~ "abiotic",
    ground_element %in% ground_elements ~ "ground",
    .default = "both"
  ))

# Export tables
field_list <- c(
  "completion", "cover_method", "cover_type", "crown_class",
  "data_tier", "data_type", "database_dictionary", "database_schema",
  "disturbance", "disturbance_severity", "drainage",
  "geomorphology", "ground_element", "h_datum", "height_type",
  "location_type",
  "macrotopography", "microtopography", "moisture",
  "organization", "organization_type",
  "personnel", "perspective", "physiography",
  "plot_dimensions", "positional_accuracy", "restrictive_type",
  "schema_category", "schema_table", "scope",
  "shrub_class", "soil_class", "soil_texture",
  "structural_class", "structural_group"
)
table_list <- list(
  completion_table, cover_method_table, cover_type_table, crown_class_table,
  data_tier_table, data_type_table, database_dictionary_table, database_schema_table,
  disturbance_table, disturbance_severity_table, drainage_table,
  geomorphology_table, ground_element_table, h_datum_table, height_type_table,
  location_type_table,
  macrotopography_table, microtopography_table, moisture_table,
  organization_table, organization_type_table,
  personnel_table, perspective_table, physiography_table,
  plot_dimensions_table, positional_accuracy_table, restrictive_type_table,
  schema_category_table, schema_table_table, scope_table,
  shrub_class_table, soil_class_table, soil_texture_table,
  structural_class_table, structural_group_table
)
for (field in field_list) {
  csv_output <- path(data_folder, "csv", paste(field, ".csv", sep = ""))
  export_table <- table_list[match(field, field_list)][[1]]
  write_csv(export_table, file = csv_output)
}

#### WRITE DATA TO SQL FILE

# Write statement header
statement <- c(
  "-- -*- coding: utf-8 -*-",
  "-- ---------------------------------------------------------------------------",
  "-- Insert metadata and constraints",
  "-- Author: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science",
  paste("-- Last Updated: ", Sys.Date()),
  "-- Usage: Script should be executed in a PostgreSQL 16+ database.",
  '-- Description: "Insert metadata and constraints" pushes data from the database dictionary and schema into the database server. The script "Build metadata and constraint tables" should be run prior to this script to start with empty, properly formatted tables.',
  "-- ---------------------------------------------------------------------------",
  "",
  "-- Initialize transaction",
  "START TRANSACTION;",
  ""
)

# Add author statement
statement <- c(
  statement,
  "-- Insert data into constraint tables",
  "INSERT INTO completion (completion_id, completion) VALUES"
)
completion_sql <- completion_table %>%
  mutate(completion = paste("'", completion, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
completion_sql[nrow(completion_sql), ] <-
  paste(str_sub(completion_sql[nrow(completion_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in completion_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO cover_method (cover_method_id, cover_method) VALUES"
)
cover_method_sql <- cover_method_table %>%
  mutate(cover_method = paste("'", cover_method, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
cover_method_sql[nrow(cover_method_sql), ] <-
  paste(str_sub(cover_method_sql[nrow(cover_method_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in cover_method_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO cover_type (cover_type_id, cover_type) VALUES"
)
cover_type_sql <- cover_type_table %>%
  mutate(cover_type = paste("'", cover_type, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
cover_type_sql[nrow(cover_type_sql), ] <-
  paste(str_sub(cover_type_sql[nrow(cover_type_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in cover_type_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO crown_class (crown_class_id, crown_class) VALUES"
)
crown_class_sql <- crown_class_table %>%
  mutate(crown_class = paste("'", crown_class, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
crown_class_sql[nrow(crown_class_sql), ] <-
  paste(str_sub(crown_class_sql[nrow(crown_class_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in crown_class_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO data_tier (data_tier_id, data_tier) VALUES"
)
data_tier_sql <- data_tier_table %>%
  mutate(data_tier = paste("'", data_tier, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
data_tier_sql[nrow(data_tier_sql), ] <-
  paste(str_sub(data_tier_sql[nrow(data_tier_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in data_tier_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO data_type (data_type_id, data_type) VALUES"
)
data_type_sql <- data_type_table %>%
  mutate(data_type = paste("'", data_type, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
data_type_sql[nrow(data_type_sql), ] <-
  paste(str_sub(data_type_sql[nrow(data_type_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in data_type_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO disturbance (disturbance_id, disturbance) VALUES"
)
disturbance_sql <- disturbance_table %>%
  mutate(disturbance = paste("'", disturbance, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
disturbance_sql[nrow(disturbance_sql), ] <-
  paste(str_sub(disturbance_sql[nrow(disturbance_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in disturbance_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO disturbance_severity (disturbance_severity_id, disturbance_severity) VALUES"
)
disturbance_severity_sql <- disturbance_severity_table %>%
  mutate(disturbance_severity = paste("'", disturbance_severity, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
disturbance_severity_sql[nrow(disturbance_severity_sql), ] <-
  paste(str_sub(disturbance_severity_sql[nrow(disturbance_severity_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in disturbance_severity_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO drainage (drainage_id, drainage) VALUES"
)
drainage_sql <- drainage_table %>%
  mutate(drainage = paste("'", drainage, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
drainage_sql[nrow(drainage_sql), ] <-
  paste(str_sub(drainage_sql[nrow(drainage_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in drainage_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO geomorphology (geomorphology_id, geomorphology) VALUES"
)
geomorphology_sql <- geomorphology_table %>%
  mutate(geomorphology = paste("'", geomorphology, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
geomorphology_sql[nrow(geomorphology_sql), ] <-
  paste(str_sub(geomorphology_sql[nrow(geomorphology_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in geomorphology_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO ground_element (ground_element_code, ground_element, element_type) VALUES"
)
ground_element_sql <- ground_element_table %>%
  mutate(ground_element_code = paste("'", ground_element_code, "'", sep = "")) %>%
  mutate(ground_element = paste("'", ground_element, "'", sep = "")) %>%
  mutate(element_type = paste("'", element_type, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
ground_element_sql[nrow(ground_element_sql), ] <-
  paste(str_sub(ground_element_sql[nrow(ground_element_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in ground_element_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO h_datum (h_datum_epsg, h_datum) VALUES"
)
h_datum_sql <- h_datum_table %>%
  mutate(h_datum = paste("'", h_datum, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
h_datum_sql[nrow(h_datum_sql), ] <-
  paste(str_sub(h_datum_sql[nrow(h_datum_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in h_datum_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO height_type (height_type_id, height_type) VALUES"
)
height_type_sql <- height_type_table %>%
  mutate(height_type = paste("'", height_type, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
height_type_sql[nrow(height_type_sql), ] <-
  paste(str_sub(height_type_sql[nrow(height_type_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in height_type_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO location_type (location_type_id, location_type) VALUES"
)
location_type_sql <- location_type_table %>%
  mutate(location_type = paste("'", location_type, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
location_type_sql[nrow(location_type_sql), ] <-
  paste(str_sub(location_type_sql[nrow(location_type_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in location_type_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO macrotopography (macrotopography_id, macrotopography) VALUES"
)
macrotopography_sql <- macrotopography_table %>%
  mutate(macrotopography = paste("'", macrotopography, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
macrotopography_sql[nrow(macrotopography_sql), ] <-
  paste(str_sub(macrotopography_sql[nrow(macrotopography_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in macrotopography_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO microtopography (microtopography_id, microtopography) VALUES"
)
microtopography_sql <- microtopography_table %>%
  mutate(microtopography = paste("'", microtopography, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
microtopography_sql[nrow(microtopography_sql), ] <-
  paste(str_sub(microtopography_sql[nrow(microtopography_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in microtopography_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO moisture (moisture_id, moisture) VALUES"
)
moisture_sql <- moisture_table %>%
  mutate(moisture = paste("'", moisture, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
moisture_sql[nrow(moisture_sql), ] <-
  paste(str_sub(moisture_sql[nrow(moisture_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in moisture_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO organization_type (organization_type_id, organization_type) VALUES"
)
organization_type_sql <- organization_type_table %>%
  mutate(organization_type = paste("'", organization_type, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
organization_type_sql[nrow(organization_type_sql), ] <-
  paste(str_sub(organization_type_sql[nrow(organization_type_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in organization_type_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO personnel (personnel_id, personnel) VALUES"
)
personnel_sql <- personnel_table %>%
  mutate(personnel = paste("'", personnel, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
personnel_sql[nrow(personnel_sql), ] <-
  paste(str_sub(personnel_sql[nrow(personnel_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in personnel_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO perspective (perspective_id, perspective) VALUES"
)
perspective_sql <- perspective_table %>%
  mutate(perspective = paste("'", perspective, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
perspective_sql[nrow(perspective_sql), ] <-
  paste(str_sub(perspective_sql[nrow(perspective_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in perspective_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO physiography (physiography_id, physiography) VALUES"
)
physiography_sql <- physiography_table %>%
  mutate(physiography = paste("'", physiography, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
physiography_sql[nrow(physiography_sql), ] <-
  paste(str_sub(physiography_sql[nrow(physiography_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in physiography_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO plot_dimensions (plot_dimensions_id, plot_dimensions_m) VALUES"
)
plot_dimensions_sql <- plot_dimensions_table %>%
  mutate(plot_dimensions_m = paste("'", plot_dimensions_m, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
plot_dimensions_sql[nrow(plot_dimensions_sql), ] <-
  paste(str_sub(plot_dimensions_sql[nrow(plot_dimensions_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in plot_dimensions_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO positional_accuracy (positional_accuracy_id, positional_accuracy) VALUES"
)
positional_accuracy_sql <- positional_accuracy_table %>%
  mutate(positional_accuracy = paste("'", positional_accuracy, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
positional_accuracy_sql[nrow(positional_accuracy_sql), ] <-
  paste(str_sub(positional_accuracy_sql[nrow(positional_accuracy_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in positional_accuracy_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO restrictive_type (restrictive_type_id, restrictive_type) VALUES"
)
restrictive_type_sql <- restrictive_type_table %>%
  mutate(restrictive_type = paste("'", restrictive_type, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
restrictive_type_sql[nrow(restrictive_type_sql), ] <-
  paste(str_sub(restrictive_type_sql[nrow(restrictive_type_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in restrictive_type_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO schema_category (schema_category_id, schema_category) VALUES"
)
schema_category_sql <- schema_category_table %>%
  mutate(schema_category = paste("'", schema_category, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
schema_category_sql[nrow(schema_category_sql), ] <-
  paste(str_sub(schema_category_sql[nrow(schema_category_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in schema_category_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO schema_table (schema_table_id, schema_table) VALUES"
)
schema_table_sql <- schema_table_table %>%
  mutate(schema_table = paste("'", schema_table, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
schema_table_sql[nrow(schema_table_sql), ] <-
  paste(str_sub(schema_table_sql[nrow(schema_table_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in schema_table_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO scope (scope_id, scope) VALUES"
)
scope_sql <- scope_table %>%
  mutate(scope = paste("'", scope, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
scope_sql[nrow(scope_sql), ] <-
  paste(str_sub(scope_sql[nrow(scope_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in scope_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO shrub_class (shrub_class_id, shrub_class) VALUES"
)
shrub_class_sql <- shrub_class_table %>%
  mutate(shrub_class = paste("'", shrub_class, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
shrub_class_sql[nrow(shrub_class_sql), ] <-
  paste(str_sub(shrub_class_sql[nrow(shrub_class_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in shrub_class_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO soil_class (soil_class_id, soil_class) VALUES"
)
soil_class_sql <- soil_class_table %>%
  mutate(soil_class = paste("'", soil_class, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
soil_class_sql[nrow(soil_class_sql), ] <-
  paste(str_sub(soil_class_sql[nrow(soil_class_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in soil_class_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO soil_nonmatrix_features (nonmatrix_feature_code, nonmatrix_feature) VALUES"
)
soil_nonmatrix_sql <- soil_nonmatrix_table %>%
  mutate(nonmatrix_feature_code = paste("'", nonmatrix_feature_code, "'", sep = "")) %>%
  mutate(nonmatrix_feature = paste("'", nonmatrix_feature, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
soil_nonmatrix_sql[nrow(soil_nonmatrix_sql), ] <-
  paste(str_sub(soil_nonmatrix_sql[nrow(soil_nonmatrix_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in soil_nonmatrix_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO soil_structure (soil_structure_code, soil_structure) VALUES"
)
soil_structure_sql <- soil_structure_table %>%
  mutate(soil_structure_code = paste("'", soil_structure_code, "'", sep = "")) %>%
  mutate(soil_structure = paste("'", soil_structure, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
soil_structure_sql[nrow(soil_structure_sql), ] <-
  paste(str_sub(soil_structure_sql[nrow(soil_structure_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in soil_structure_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO soil_texture (soil_texture_code, soil_texture) VALUES"
)
soil_texture_sql <- soil_texture_table %>%
  mutate(soil_texture_code = paste("'", soil_texture_code, "'", sep = "")) %>%
  mutate(soil_texture = paste("'", soil_texture, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
soil_texture_sql[nrow(soil_texture_sql), ] <-
  paste(str_sub(soil_texture_sql[nrow(soil_texture_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in soil_texture_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO soil_horizon_type (soil_horizon_type_code, soil_horizon_type) VALUES"
)
soil_horizon_type_sql <- soil_horizon_type_table %>%
  mutate(soil_horizon_type_code = paste("'", soil_horizon_type_code, "'", sep = "")) %>%
  mutate(soil_horizon_type = paste("'", soil_horizon_type, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
soil_horizon_type_sql[nrow(soil_horizon_type_sql), ] <-
  paste(str_sub(soil_horizon_type_sql[nrow(soil_horizon_type_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in soil_horizon_type_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO soil_horizon_suffix (soil_horizon_suffix_code, soil_horizon_suffix) VALUES"
)
soil_horizon_suffix_sql <- soil_horizon_suffix_table %>%
  mutate(soil_horizon_suffix_code = paste("'", soil_horizon_suffix_code, "'", sep = "")) %>%
  mutate(soil_horizon_suffix = paste("'", soil_horizon_suffix, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
soil_horizon_suffix_sql[nrow(soil_horizon_suffix_sql), ] <-
  paste(str_sub(soil_horizon_suffix_sql[nrow(soil_horizon_suffix_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in soil_horizon_suffix_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO soil_hue (soil_hue_code, soil_hue) VALUES"
)
soil_hue_sql <- soil_hue_table %>%
  mutate(soil_hue_code = paste("'", soil_hue_code, "'", sep = "")) %>%
  mutate(soil_hue = paste("'", soil_hue, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
soil_hue_sql[nrow(soil_hue_sql), ] <-
  paste(str_sub(soil_hue_sql[nrow(soil_hue_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in soil_hue_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO structural_class (structural_class_code, structural_class) VALUES"
)
structural_class_sql <- structural_class_table %>%
  mutate(structural_class_code = paste("'", structural_class_code, "'", sep = "")) %>%
  mutate(structural_class = paste("'", structural_class, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
structural_class_sql[nrow(structural_class_sql), ] <-
  paste(str_sub(structural_class_sql[nrow(structural_class_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in structural_class_sql) {
  statement <- c(statement, line)
}

statement <- c(
  statement,
  "INSERT INTO structural_group (structural_group_id, structural_group) VALUES"
)
structural_group_sql <- structural_group_table %>%
  mutate(structural_group = paste("'", structural_group, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
structural_group_sql[nrow(structural_group_sql), ] <-
  paste(str_sub(structural_group_sql[nrow(structural_group_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in structural_group_sql) {
  statement <- c(statement, line)
}

# Add organization statement
statement <- c(
  statement,
  "INSERT INTO organization (organization_id, organization, organization_type_id) VALUES"
)
organization_sql <- organization_table %>%
  mutate(organization = paste("'", organization, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
organization_sql[nrow(organization_sql), ] <-
  paste(str_sub(organization_sql[nrow(organization_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in organization_sql) {
  statement <- c(statement, line)
}

# Add database schema statement
statement <- c(
  statement,
  "",
  "-- Insert data into database schema",
  "INSERT INTO database_schema (field_id, standards_section, schema_category_id, schema_table_id, field, data_type_id, field_length, is_unique, is_key, required, link_table_id, field_description) VALUES"
)
schema_sql <- database_schema_table %>%
  mutate(field = paste("'", field, "'", sep = "")) %>%
  mutate(field_length = paste("'", field_length, "'", sep = "")) %>%
  mutate(field_description = paste("'", field_description, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
schema_sql[nrow(schema_sql), ] <-
  paste(str_sub(schema_sql[nrow(schema_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in schema_sql) {
  statement <- c(statement, line)
}

# Add database schema statement
statement <- c(
  statement,
  "",
  "-- Insert data into database dictionary",
  "INSERT INTO database_dictionary (dictionary_id, field_id, data_attribute_id, data_attribute, definition) VALUES"
)
dictionary_sql <- database_dictionary_table %>%
  mutate_if(is.character,
    str_replace_all,
    pattern = "'", replacement = "''"
  ) %>%
  mutate(data_attribute_id = paste("'", data_attribute_id, "'", sep = "")) %>%
  mutate(data_attribute = paste("'", data_attribute, "'", sep = "")) %>%
  mutate(definition = paste("'", definition, "'", sep = "")) %>%
  unite(sql, sep = ", ", remove = TRUE) %>%
  mutate(sql = paste("(", sql, "),", sep = ""))
dictionary_sql[nrow(dictionary_sql), ] <-
  paste(str_sub(dictionary_sql[nrow(dictionary_sql), ],
    start = 1, end = -2
  ), ";", sep = "")
for (line in dictionary_sql) {
  statement <- c(statement, line)
}

# Finalize statement
statement <- c(
  statement,
  "",
  "-- Commit transaction",
  "COMMIT TRANSACTION;"
)

# Replace NA values in statement
statement <- str_replace(statement, ", 'NA',", ", NULL,")

# Write statement to SQL file
write_lines(statement, sql_metadata)

# Clear workspace
rm(list = ls())
