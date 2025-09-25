# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Vegetation Cover for 2024 ACCS NWI Southcentral data"
# Author: Amanda Droghini
# Last Updated: 2025-09-24
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format Vegetation Cover for 2024 ACCS NWI Southcentral data" formats information about vegetation cover
# for ingestion into AKVEG Database. The script corrects taxonomic names using the AKVEG Comprehensive Checklist as a
# taxonomic standard. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# -------------------

# Import libraries
import polars as pl
from pathlib import Path
from akutils import connect_database_postgresql
from akutils import query_to_dataframe

# Define directories
drive = Path('C:/')
root = drive / 'ACCS_Work'
project_folder = root / 'OneDrive - University of Alaska' /'ACCS_Teams' /'Vegetation' / 'AKVEG_Database'
plot_folder = project_folder / 'Data' / 'Data_Plots' / '54_accs_nwisouthcentral_2024'
credential_folder = project_folder / "Credentials"

# Define inputs
visit_input = plot_folder / '03_sitevisit_accsnwisouthcentral2024.csv'
vegetation_input = plot_folder / 'source' / '05_vegetationcover_accsnwisouthcentral2024.xlsx'
template_input = project_folder / 'Data' / 'Data_Entry' / '05_vegetation_cover.xlsx'
akveg_credentials = (
    credential_folder / "akveg_public_read" / "authentication_akveg_public_read.csv"
)

# Define output
vegetation_output = plot_folder / '05_vegetationcover_accsnwisouthcentral2024.csv'

# Read in data
template = pl.read_excel(template_input)
visit_original = pl.read_csv(visit_input, columns='site_visit_code')
vegetation_original = pl.read_excel(vegetation_input)

# Query AKVEG database
akveg_db_connection = connect_database_postgresql(akveg_credentials)

## Query database for taxonomy checklist
taxonomy_query = """SELECT taxon_all.taxon_code, taxon_all.taxon_name, taxon_all.taxon_accepted_code
                    FROM taxon_all;"""

taxonomy_original = query_to_dataframe(akveg_db_connection, taxonomy_query)
taxonomy_original = pl.from_pandas(taxonomy_original)

## Close the database connection
akveg_db_connection.close()

# Create accepted taxonomy table
taxonomy_accepted = (
    taxonomy_original.filter(pl.col("taxon_code") == pl.col("taxon_accepted_code"))
    .rename({"taxon_name": "name_adjudicated"})
    .drop("taxon_code")
)

taxonomy_akveg = (taxonomy_original.join(taxonomy_accepted, on='taxon_accepted_code', how='left')
                  .drop(["taxon_code", "taxon_accepted_code"]))

# Ensure that all site visit codes are included in the Site Visit table
print(vegetation_original.join(visit_original, on='site_visit_code', how='anti').shape[0])
print(visit_original.join(vegetation_original, on='site_visit_code', how='anti').shape[0])

# Verify constrained values
print(vegetation_original['cover_type'].unique())
print(vegetation_original['dead_status'].dtype)

# Verify range of cover percent
vegetation_original['cover_percent'].describe()

# Verify taxonomy

## Correct typos
replace_names = {"Artimesia tilesii": "Artemisia tilesii",
                 "Arunus dioicus": "Aruncus dioicus",
                 "Athyrium felix-femina": "Athyrium filix-femina",
                 "Bryophyte": "bryophyte",
                 "Calamagrostis caandensis var. canadensis": "Calamagrostis canadensis var. canadensis",
                 "Carex michrochaeta": "Carex microchaeta",
                 "Dasiflora fruticosa florabunda": "Dasiphora fruticosa ssp. floribunda",
                 "Epilobium hornemannii ssp. Behringianum": "Epilobium hornemannii ssp. behringianum",
                 "Epilobium playphyllum": "Epilobium palustre",  ## taxon code EPPA
                 "Equisetum fluviatale": "Equisetum fluviatile",
                 "Hockenya peploides": "Honckenya peploides",
                 "Linnea borealis": "Linnaea borealis",
                 "Liverwort": "liverwort",
                 "Menziesa ferruginea": "Menziesia ferruginea",
                 "Moss": "moss",
                 "Pellaea glabella": "Picea glauca", ## taxon code PIGL
                 "Plantathera dilatata": "Platanthera dilatata",
                 "Pleurozium schreberii": "Pleurozium schreberi",
                 "Populus balsamifera var. pilosa": "Populus balsamifera ssp. trichocarpa",  ## taxon code should be
                 # POBAT
                 "Racometrium canescens": "Racomitrium canescens",
                 "Rhytidiadelphis triquestrus": "Rhytidiadelphus triquetrus",
                 "Salix barclayii": "Salix barclayi",
                 "Sanicula bipinnatifida": "Salix barclayi", ## taxon code SABA3
                 "Spirea stevenii": "Spiraea stevenii",
                 "Swetia perennis": "Swertia perennis",
                 "Trifolium campestre": "Trichophorum cespitosum", ## taxon code should be TRCE3
                 "Vaccinium vitis-idea": "Vaccinium vitis-idaea",
                 "Vertrum viride": "Veratrum viride"
                 }

vegetation = (vegetation_original.rename({'name_original':'taxon_code',
                                          'name_adjudicated': 'name_original'})
              .with_columns(pl.col('name_original').str.replace_many(
    replace_names).str.strip_suffix("sp.").str.strip_chars())
              .join(taxonomy_akveg, how='left', left_on='name_original', right_on='taxon_name'))

## Identify missing names
missing_names = (vegetation.filter(pl.col('name_adjudicated').is_null())
                 .unique(subset=pl.col(
    'name_original')).select(['taxon_code', 'name_original'])).sort(by='name_original')  ## All unmatched names
# addressed

## Ensure all name_adjudicated are in the AKVEG Comprehensive Checklist
print(vegetation.join(taxonomy_accepted, how='anti', on='name_adjudicated'))

# Ensure column names & order match the template
vegetation = vegetation.select(template.columns)

# Export as CSV
vegetation.write_csv(vegetation_output)
