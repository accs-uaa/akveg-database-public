# Alaska Vegetation Database
Scripts to query public data stored in the Alaska Vegetation (AKVEG) Database.

## Getting Started

These instructions will enable you to query public data in the Alaska Vegetation (AKVEG) Database. The AKVEG Database is built in PostgreSQL and hosted on a cloud server that can be queried in numerous ways. This repository provides example scripts to query the database in Python and R.

To query the database, you will need server credentials. To request server credentials, fill out a 
[Database Access Form](https://akveg.uaa.alaska.edu/request-access/). The database is public and 
access is free; the purpose of the server credentials is to prevent excessive loads on the server and for us to know 
how many people are connecting.

### Prerequisites
1. Python 3.9+
   1. psycopg2
   2. pandas
   3. geopandas
   4. os
   5. [akutils](https://github.com/accs-uaa/akutils)

2. R 4.0.0+
   1. dplyr
   2. fs
   3. janitor
   4. lubridate
   5. readr
   6. readxl
   7. RPostgres
   8. sf
   9. stringr
   10. terra
   11. tibble
   12. tidyr


### Preparing credentials

Once you have received server credentials from the data manager, you will need to unzip the credentials in a location accessible to your script environment. The credentials include a csv file. In that csv file, you will need to update the file paths for the following rows: 9 (sslrootcert), 10 (sslcert), and 11 (sslkey).

### Preparing files for example scripts

The example scripts use three input files that are not included in this repository. Each of the three inputs is available for download from a publicly accessible location (see links for each dataset below).

- domain_input ('region_data/AlaskaYukon_ProjectDomain_v2.0_3338.shp'): A shapefile containing the spatial domain of the AKVEG Map project domain, which is the focal area of data integration and taxonomy for the AKVEG Database. You can [download the domain input](https://storage.googleapis.com/akveg-public/AlaskaYukon_ProjectDomain_v2.0_3338.zip) and unzip it into an accessible local folder.
- region_input ('region_data/AlaskaYukon_Regions_v2.0_3338.shp'): A shapefile containing features for major biomes and vegetation regions of Alaska & adjacent Canada (Yukon and Northern British Columbia). You can [download the region input](https://storage.googleapis.com/akveg-public/AlaskaYukon_Regions_v2.0_3338.zip) and unzip it into an accessible local folder.
- fireyear_input ('ancillary_data/AlaskaYukon_FireYear_10m_3338.tif'): A 10 m raster dataset of most recent burn year from 1940 to 2023 where zero indicates that no burn was documented during that time frame. You can [download the fire year input (~ 800 mb)](https://storage.googleapis.com/akveg-public/AlaskaYukon_FireYear_10m_3338.zip) and unzip it into an accessible local folder. Alternatively, you can remove interactions with this dataset from the example script, but you will then not be able to compare observed year to burn year.

These files are provided for the purpose of providing examples for how to relate the data in the AKVEG Database to other geospatial data. These data are not required to use or access data in the AKVEG Database. To run the example scripts with the example data included, file paths in the script must be modified to match where you store the geospatial datasets locally.  

### Column aliases for shapefiles

The queries employ shorthand field names where the database field names are too long for the ESRI shapefile format field character length constraint. When viewing the shapefile attributes, please refer to the scripts for relationship between shapefile aliases and original field names used in the AKVEG Database.

### Metadata tables

The AKVEG Database contains two metadata tables that are likely to be useful to users:

1. database_schema: Information on the structure of the database, including how field relationships.
2. database_dictionary: List and definitions of constrained values.

The AKVEG Database follows the [Minimum Standards for Field Observation of Vegetation and Related Properties](https://agc-vegetation-soa-dnr.hub.arcgis.com/documents/817be3b0405a42aea91cee0b92d77f98/explore) developed by the Alaska Vegetation Working Group (VWG). The Minimum Standards document provides additional description of the fields in the database and how the fields relate to data tiers.
## Credits

### Built With
* PostgreSQL 17
* DataGrip 2020.2.1
* R 4.0.2
* RStudio 1.3.1073
* Python 3.12
* PyCharm

### Authors

* **Timm Nawrocki** - *Alaska Center for Conservation Science, University of Alaska Anchorage*
* **Amanda Droghini** - *Alaska Center for Conservation Science, University of Alaska Anchorage*

### Support

The U.S. Fish & Wildlife Service, Bureau of Land Management, National Park Service, U.S. Forest Service, Alaska Department of Fish & Game, and Alaska Department of Natural Resources provided funding in support of the development of the AKVEG Database.

### Usage Requirements

Citing the database is not required to use the data as all data are public. Where a citation is sensible, we would appreciate you citing the AKVEG Database as follows:

Droghini, A., T.W. Nawrocki, A.F. Wells, M.J. Macander, and L.A. Flagstad. 2025. Alaska Vegetation (AKVEG) Database: Standardized, multi-project field and classification data for Alaska. Alaska Geospatial Council, Vegetation Working Group. Available: [https://akveg.uaa.alaska.edu](https://akveg.uaa.alaska.edu). Data downloaded on [date of query].

### License

This project is provided under the GNU General Public License v3.0. It is free to use and modify in part or in whole.
