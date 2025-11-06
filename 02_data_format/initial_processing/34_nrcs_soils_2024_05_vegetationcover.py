# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format NRCS Alaska 2024 Vegetation Cover Data
# Author: Amanda Droghini
# Last Updated: 2025-11-05
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format NRCS Alaska 2024 Vegetation Cover Data" reads in tables from the NRCS SQLite export received
# in May 2025. The script drops sites with incomplete data and corrects taxonomic names according to the AKVEG
# Comprehensive Checklist. The output is a CSV file that can be included in an SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import libraries
from pathlib import Path
import sqlite3
import pandas as pd
import numpy as np
from akutils import connect_database_postgresql
from akutils import query_to_dataframe

# Set root directory
drive = Path("C:/")
root_folder = Path("ACCS_Work")

# Define folder structure
project_folder = (
    drive
    / root_folder
    / "OneDrive - University of Alaska"
    / "ACCS_Teams"
    / "Vegetation"
    / "AKVEG_Database"
)
plot_folder = project_folder / "Data" / "Data_Plots" / "34_nrcs_soils_2024"
workspace_folder = plot_folder / "working"
repository_folder = drive / root_folder / "Repositories" / "akveg-database"
credential_folder = project_folder / "Credentials"

# Define input files
nrcs_database = plot_folder / "source" / "Alaska_NRCS_SPSD_data_May2025.sqlite"
lookup_input = workspace_folder / "lookup_visit.csv"
visit_input = workspace_folder / "03_sitevisit_nrcssoils2024.csv"
template_input = project_folder / "Data" / "Data_Entry" / "05_vegetation_cover.xlsx"

akveg_credentials = (
    credential_folder / "akveg_public_read" / "authentication_akveg_public_read.csv"
)

# Define output file
vegcover_output = workspace_folder / "05_vegetationcover_nrcssoils2024.csv"  ## Output to workspace; includes duplicates

# Read in data
lookup_visit = pd.read_csv(lookup_input)
visit_original = pd.read_csv(visit_input)
template_vegetation = pd.read_excel(template_input)

# Connect to SQLite database
nrcs_db_connection = sqlite3.connect(str(nrcs_database))  ## Ensure path is a string

# Extract data from tables
cursor = nrcs_db_connection.cursor()

# Query vegetation plot data
cursor.execute("SELECT * FROM plotplantinventory")
rows = cursor.fetchall()
column_names = [description[0] for description in cursor.description]
vegcover_original = pd.DataFrame(rows, columns=column_names)
print(vegcover_original.head())

# Query plant id table
cursor.execute("SELECT * FROM plant")
rows = cursor.fetchall()
column_names = [description[0] for description in cursor.description]
plantid_original = pd.DataFrame(rows, columns=column_names)
print(plantid_original.head())

## Close the cursor
cursor.close()

## Close the database connection
nrcs_db_connection.close()

# Query AKVEG database
akveg_db_connection = connect_database_postgresql(akveg_credentials)

# Query database for taxonomy checklist
taxonomy_query = """SELECT taxon_all.taxon_code, taxon_all.taxon_name, taxon_all.taxon_accepted_code
                    FROM taxon_all;"""

taxonomy_original = query_to_dataframe(akveg_db_connection, taxonomy_query)

## Close the database connection
akveg_db_connection.close()

# Create accepted taxonomy table
taxonomy_accepted = (
    taxonomy_original.loc[
        taxonomy_original["taxon_code"] == taxonomy_original["taxon_accepted_code"]
    ]
    .rename(columns={"taxon_name": "name_adjudicated"})
    .drop(columns=("taxon_code"))
)

# Simplify plant species name table
plantid = plantid_original.loc[
    :, ["plantiid", "plantsciname", "plantsym", "plantnatvernm"]
]

# Obtain site visit code
vegcover = vegcover_original.merge(
    right=lookup_visit, how="right", left_on="vegplotiidref", right_on="vegplotiid"
)

# Ensure all rows have a site visit code
print(vegcover.loc[vegcover["site_visit_code"].isna()].shape[0])

# Explore distribution of null values
vegcover = vegcover.dropna(axis="columns", how="all")
print(vegcover.isna().sum())

# Drop entries with null canopy cover
## While it's possible that the trace amount flag was mistakenly entered as 0
# instead of 1, the list of plants doesn't immediately strike me as only consisting of trace species.
vegcover = vegcover.loc[
    ~((vegcover["speciescancovpct"].isna()) & (vegcover["speciestraceamtflag"] == 0))
]

# Replace null values with 0% for trace species
vegcover = vegcover.assign(
    cover_percent=np.where(
        vegcover["speciestraceamtflag"] == 1, 0, vegcover["speciescancovpct"]
    )
)

# Obtain plant species name
vegcover_taxa = vegcover.merge(
    plantid, how="left", left_on="plantiidref", right_on="plantiid"
)

# Correct one SAAR site to Salix arctica
## Other SAAR sites will be corrected to Salix arbusculoides
vegcover_taxa.loc[
    (vegcover_taxa["site_visit_code"] == "nrcs_997941_20220818")
    & (vegcover_taxa["plantsym"] == "SAAR"),
    "plantsciname",
] = "Salix arctica"
vegcover_taxa.loc[
    (vegcover_taxa["site_visit_code"] == "nrcs_997941_20220818")
    & (vegcover_taxa["plantsym"] == "SAAR"),
    "plantsym",
] = "SAAR27"

# Correct unknown names & incorrect plant codes
replace_usda_codes = {
    "2ALGA": "algae",
    "2FERN": "fern",
    "2FF": "fungus",
    "2FM": "forb",
    "2FORB": "forb",
    "2FUNGI": "fungus",
    "2GN": "graminoid",
    "2GRAM": "graminoid",
    "2LC": "crustose lichen",
    "2LF": "foliose lichen",
    "2LICHN": "lichen",
    "2LU": "fruticose lichen",
    "2LW": "liverwort",
    "2MOSS": "moss",
    "2SHRUB": "shrub",
    "2PLANT": "unknown",
    "UNKNOWN": "unknown",
    "ACAL6": "Aconogonon alaskanum",
    "ACMI": "Achillea millefolium",  # Changed
    "ACMI3": "Achillea millefolium",
    "ACRU": "Actaea rubra",
    "ACRU3": "Actaea rubra",
    "ALINI": "Alnus incana",
    "ALNI": "Alectoria nigricans",
    "ANFR2": "Antennaria friesiana",
    "ARAL": "Arctostaphylos alpina",
    "ARAL29": "Arnica",
    "ARCAB4": "Artemisia campestris ssp. borealis",
    "ARFR": "Arnica frigida",  # Changed
    "ARLA": "Arctagrostis latifolia",
    "ARRU2": "Arctostaphylos rubra",
    "ASTER": "Aster",
    "ASTERA": "forb",
    "ASUM3": "Astragalus umbellatus",
    "AUPA": "Aulacomnium palustre",
    "BEAD": "forb",  # Changed
    "BEDU": "Betula ×dugleana",
    "BEEA": "Betula ×eastwoodiae",
    "BEHO": "Betula ×hornei",
    "BEMA": "Alnus incana",
    "BENA2": "Betula nana",
    "BENE": "Betula neoalaskana",  # Changed
    "BENE5": "Betula neoalaskana",
    "BEOC": "Betula occidentalis",
    "BESA3": "Betula ×sargentii",
    "BOBO4": "Botrychium boreale ssp. obtusilobum",
    "BORI2": "Boykinia richardsonii",
    "BRACH": "Brachythecium",
    "BRINA": "Bromus pumpellianus var. arcticus",
    "BRINP5": "Bromus pumpellianus var. pumpellianus",
    "BROME": "Bromus",
    "BRPY": "moss",
    "CAAB10": "Carex",
    "CACA": "Calamagrostis canadensis",
    "CACA14": "Calamagrostis canadensis",
    "CALO6": "Carex loliacea",
    "CAME25": "Carex",
    "CAMI5": "Carex",
    "CANI5": "Carex nigricans",
    "CAOL": "Cardamine oligosperma var. kamtschatica",
    "CAPR": "Cardamine pratensis",
    "CAPR5": "Cardamine pratensis",  # Site is on Kodiak, C. praegracilis unlikely
    "CAPY": "Carex pyrenaica",
    "CARYOP": "forb",
    "CASC": "Carex scirpoidea",
    "CASI2": "Carex",
    "CASTI": "Castilleja",
    "CAVA4": "Carex vaginata",
    "CEBE": "Cerastium beeringianum",  # Changed
    "CEPU": "Ceratodon purpureus",
    "CEPU7": "Ceratodon purpureus",
    "CHAN": "Chamaenerion angustifolium",
    "CHLA": "Chamaenerion latifolium",
    "CILA": "Cinna latifolia",
    "CLAD": "Cladina",
    "CLADI": "Cladina",
    "CLGR": "Cladonia",
    "CLGR6": "Cladonia",  # C. granulans seems unlikely
    "CLGRG3": "Cladonia gracilis var. gracilis",
    "CLMI": "Cladina mitis",
    "CLMI5": "Cladina mitis",
    "CLST": "Cladonia",
    "CLSQ2": "Cladonia squamosa",
    "CYPA": "Cypripedium passerinum",
    "DAFR2": "Dasiphora fruticosa",
    "DAFR5": "Dasiphora fruticosa",
    "DECA18": "Deschampsia cespitosa",
    "DEGL": "Delphinium glaucum",
    "DEGL2": "Delphinium glaucum",
    "DITRIC": "moss",
    "DREPA2": "Drepanocladus",
    "DRRE": "Drepanocladus revolvens",
    "ELPA": "Eleocharis palustris",
    "ELTR3": "Elymus trachycaulus",
    "ENMI": "Empetrum nigrum",
    "ERCH11": "Eriophorum ×churchillianum",
    "ERNA": "Eritrichium nanum",
    "EROP": "Eriophorum opacum",
    "ERVA": "Eriophorum vaginatum",
    "FABACE": "forb",
    "Fungi": "fungus",
    "GABO": "Galium boreale",
    "GABO3": "Galium boreale",
    "GATR10": "Galium trifidum",
    "GEER": "Geranium erianthum",
    "GELI3": "Geocaulon lividum",
    "GEMA": "Geum macrophyllum",
    "GENTIA": "forb",
    "GERANI": "forb",
    "HELA": "Heracleum lanatum",
    "HIAL3": "Hierochloë alpina",
    "HIALA": "Hierochloë alpina ssp. alpina",
    "HIHIA": "Hierochloë hirta ssp. arctica",
    "HOBR5": "Hordeum brachyantherum",
    "HYPOG": "Hypogymnia",
    "HYSP": "Hylocomium splendens",
    "HYSP2": "Hylocomium splendens",
    "JUCO7": "Juniperus communis",
    "LIBA": "Listera banksiana",
    "LIBO": "Linnaea borealis",
    "LIBO2": "Linnaea borealis",
    "LICHEN": "lichen",
    "LICO": "Listera cordata",
    "LICO5": "Listera cordata",  # L. convallarioides only found in the Aleutians
    "LIPE2": "Linum perenne ssp. lewisii",
    "LOOR": "Lobaria oregana",
    "LUAN2": "Lycopodium annotinum",
    "LUAR": "Lupinus arcticus",  # Changed
    "LUARS2": "Lupinus arcticus",
    "LUCO": "Luzula confusa",
    "LUWA2": "Luzula wahlenbergii",
    "LYAN": "Lycopodium annotinum",
    "LYCO": "Lycopodium complanatum",  # Changed
    "LYCO2": "Lycopodium complanatum",  # Changed
    "MNIACE": "moss",  # Changed
    "MOUN": "Moneses uniflora",
    "MOUN4": "Moneses uniflora",
    "MYALA": "Myosotis alpestris ssp. asiatica",
    "NULU": "Nuphar",
    "ORCHID": "forb",
    "PACY2": "Packera cymbalaria",
    "PEAP": "Peltigera aphthosa",
    "PEEP": "Pellia epiphylla",
    "PEFR": "Petasites frigidus",
    "PEFR3": "Petasites frigidus",
    "PEGR": "Pedicularis groenlandica",
    "PELIT2": "Peltigera",
    "PELTIG": "Peltigera",  # Changed
    "PESU": "Pedicularis",
    "PHPR10": "Phleum pratense",
    "PILU": "Picea ×lutzii",
    "PLSC": "Pleurozium schreberi",
    "PLSQ": "moss",
    "POACEA": "grass (Poaceae)",
    "POAR": "Poa arctica",
    "POARA": "Poa arctica ssp. arctica",
    "POBI6": "Polygonum bistorta",
    "POCO7": "Polytrichum commune",
    "POEG": "Potentilla egedei",
    "POHY5": "Potentilla hyparctica",
    "POLYT": "Polytrichum",
    "POPI7": "Polytrichum piliferum",
    "POPR2": "Poa pratensis",  # Changed
    "POPU9": "Polemonium pulcherrimum",
    "POTR7": "Populus tremuloides",
    "POTR10": "Populus tremuloides",
    "RHLA": "Rhododendron lapponicum",
    "ROIS2": "Rorippa islandica var. hispida",
    "SAAN": "Saussurea angustifolia",
    "SAAR": "Salix arbusculoides",
    "SAAR6": "Salix arctica",
    "SABA2": "Salix",
    "SABE": "Salix bebbiana",
    "SAPLP2": "Salix pulchra",
    "SAPU2": "Salix pulchra",
    "SAPU10": "Salix pulchra",
    "SAPUA": "Sambucus pubens",
    "SARAA3": "Sambucus racemosa",
    "SARAR3": "Sambucus racemosa",
    "SARO": "Salix rotundifolia",
    "SAUNU": "Sanionia uncinata",
    "SOSC": "Sorbus scopulina",
    "SOSIS3": "Solidago simplex var. simplex",
    "SPAN7": "Sphagnum angustifolium",
    "SPBE2": "Spiraea beauverdiana",
    "SPST": "Spiraea stevenii",
    "STAMA2": "Streptopus amplexifolius ssp. americanus",
    "STER2": "Stereocaulon",
    "SUCA": "Suaeda calceoliformis",
    "TRMA4": "Triglochin maritima",  # Corrected to feminine form
    "TRMAE": "Triglochin maritima var. elata",  # Corrected
    "TRSP5": "Trisetum spicatum",
    "XACH3": "lichen",
    "ZIEL": "Zigadenus elegans",
    "ZIELE": "Zigadenus elegans",
}

for old_sym, new_sciname in replace_usda_codes.items():
    bool_mask = vegcover_taxa["plantsym"] == old_sym
    vegcover_taxa.loc[bool_mask, "plantsciname"] = new_sciname

# Obtain accepted taxonomic names from comprehensive checklist
vegcover_taxa = vegcover_taxa.merge(
    taxonomy_original, how="left", left_on="plantsciname", right_on="taxon_name"
)
vegcover_taxa = vegcover_taxa.merge(
    taxonomy_accepted, how="left", on="taxon_accepted_code"
)

vegcover_taxa = vegcover_taxa.rename(columns={"plantsciname": "name_original"}).drop(
    columns=["taxon_name"]
)

# Add accepted names for entries that did not have a match in the taxonomy checklist
replace_accepted_names = {
    "Acosta maculosa": "Centaurea stoebe ssp. micranthos",
    "Agrostis microphylla": "Agrostis exarata",
    "Agropyron violaceum ssp. andinum": "Elymus trachycaulus ssp. trachycaulus",
    "Alopecurus alpinus": "Alopecurus",
    "Alopecurus magellanicus": "Alopecurus",
    "Amelanchier alnifolia var. semiintegrifolia": "Amelanchier alnifolia",
    "Argentina egedii": "Potentilla anserina",
    "Argentina egedii ssp. egedii": "Potentilla anserina",
    "Argentina egedii ssp. groenlandica": "Potentilla anserina",
    "Aruncus dioicus var. vulgaris": "Aruncus dioicus",
    "Aster": "forb",
    "Astragalus alpinus var. alpinus": "Astragalus alpinus",
    "Betula ×dugleana": "Betula cf. occidentalis",
    "Betula ×eastwoodiae": "Betula cf. occidentalis",
    "Betula ×hornei": "Betula",
    "Betula ×sargentii": "Betula",
    "Cardamine pratensis var. pratensis": "Cardamine polemonioides",
    "Carex lenticularis": "Carex kelloggii",
    "Carex pyrenaica": "Carex micropoda",
    "Chrysanthemum": "Arctanthemum",
    "Cladonia abbreviatula": "Cladonia",
    "Convallaria trifolia": "Maianthemum dilatatum",
    "Corallorrhiza trifida": "Corallorhiza trifida",
    "Cornus sericea ssp. occidentalis": "Cornus sericea",
    "Draba alpina": "Draba",
    "Dryas octopetala": "Dryas ajanensis ssp. beringensis",
    "Dryopteris austriaca": "Dryopteris carthusiana",
    "Eleocharis acicularis var. acicularis": "Eleocharis acicularis",
    "Elymus trachycaulus ssp. andinus": "Elymus trachycaulus ssp. trachycaulus",
    "Elymus trachycaulus ssp. novae-angliae": "Elymus trachycaulus ssp. trachycaulus",
    "Empetrum nigrum var. atropurpureum": "Empetrum nigrum",
    "Eriophorum ×churchillianum": "Eriophorum",
    "Eurhynchium": "moss",
    "Festuca brachyphylla ssp. brachyphylla": "Festuca brachyphylla",
    "Festuca ovina": "Festuca",
    "Huperzia chinensis": "Huperzia miyoshiana",
    "Huperzia selago var. selago": "Huperzia selago",
    "Hypnaceae": "moss",
    "Kalmia polifolia": "Kalmia microphylla",
    "Luzula arctica ssp. arctica": "Luzula nivalis",
    "Melandrium apetalum": "Silene uralensis ssp. arctica",
    "Minuartia": "forb",
    "Muhlenbergia": "grass (Poaceae)",
    "Myriophyllum spicatum": "Myriophyllum sibiricum",
    "Pedicularus sudetica": "Pedicularis",
    "Plantago maritima var. juncoides": "Plantago maritima",
    "Polemonium caeruleum": "Polemonium acutiflorum",
    "Polygonum bistorta": "Bistorta vivipara",
    "Potentilla uniflora": "Potentilla",
    "Pyrolaceae": "forb",
    "Ranunculus gmelinii ssp. purshii": "Ranunculus gmelinii",
    "Ranunculaceae": "forb",
    "Rhinanthus minor ssp. groenlandicus": "Rhinanthus minor",
    "Rhodiola rosea": "Rhodiola integrifolia",
    "Rubus idaeus ssp. idaeus": "Rubus idaeus",
    "Rubus pubescens": "Rubus arcticus",
    "Rumex acetosa ssp. alpestris": "Rumex acetosa",
    "Salix brachycarpa": "Salix niphoclada",
    "Salix planifolia": "Salix pulchra",
    "Saxifraga bronchialis": "Saxifraga funstonii",
    "Saxifraga flagellaris": "Saxifraga flagellaris ssp. setigera",
    "Saxifragaceae": "forb",
    "Sedum roseum": "Rhodiola integrifolia",
    "Senecio integerrimus var. integerrimus": "Senecio integerrimus",
    "Smelowskia calycina": "Smelowskia",
    "Smilacaceae": "forb",
    "Solidago multiradiata var. multiradiata": "Solidago multiradiata",
    "Symphyotrichum subspicatum var. subspicatum": "Symphyotrichum subspicatum",
    "Taraxacum lyratum": "Taraxacum",
    "Thelypteris": "fern",
    "Trientalis borealis": "Lysimachia europaea",
    "Vaccinium oxycoccos": "Oxycoccus microcarpus",
}

for old_name, accepted_name in replace_accepted_names.items():
    bool_mask = vegcover_taxa["name_original"] == old_name
    vegcover_taxa.loc[bool_mask, "name_adjudicated"] = accepted_name

# Extract abiotic cover codes
abiotic_cover = vegcover_taxa.loc[
    vegcover_taxa["plantsym"].isin(["2BARE", "2LTRH", "2RB", "2RF", "2W"])
]

## Drop abiotic codes from vegetation cover df
vegcover_taxa = vegcover_taxa.loc[
    ~(vegcover_taxa["plantsym"].isin(["2BARE", "2LTRH", "2RB", "2RF", "2W"]))
]

# Explore names without a match
unknown_taxa = vegcover_taxa.loc[vegcover_taxa.name_adjudicated.isna()]
unknown_taxa = (
    unknown_taxa.groupby(by=["plantsym", "name_original"])["cover_percent"]
    .sum()
    .sort_values(ascending=False)
)

# Resolve remaining taxa to generic 'unknown'
vegcover_taxa = vegcover_taxa.assign(
    name_adjudicated=np.where(
        vegcover_taxa["name_adjudicated"].isna(),
        "unknown",
        vegcover_taxa["name_adjudicated"],
    )
)

# Ensure each site visit has only one entry per species
vegcover_taxa = (
    vegcover_taxa.groupby(by=["site_visit_code", "name_original", "name_adjudicated"])[
        "cover_percent"
    ]
    .sum()
    .reset_index()
)

# Ensure all accepted names are in the AKVEG database
taxon_list = pd.Series(vegcover_taxa["name_adjudicated"].unique())
missing_taxa = taxon_list[~taxon_list.isin(taxonomy_accepted["name_adjudicated"])]
print(missing_taxa.shape[0])

# Ensure all entries in the Site Visit table are included in the Vegetation Cover table (and vice-versa)
missing_sites = visit_original[
    ~visit_original["site_visit_code"].isin(vegcover_taxa["site_visit_code"])
]
print(missing_sites.shape[0])

# Populate remaining columns
vegcover_final = vegcover_taxa.assign(
    cover_type="absolute canopy cover", dead_status="FALSE"
)

# Reorder columns to match data entry template
vegcover_final = vegcover_final[template_vegetation.columns]

# Export to CSV
vegcover_final.to_csv(vegcover_output, index=False, encoding="UTF-8")
