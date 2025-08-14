# Import required libraries
library(reader)
library(RPostgres)
library(tibble)

# Set file paths
repository_path = 'C:/Users/timmn/Documents/Repositories/akveg-database' # UPDATE FILE PATH
authentication_path = 'N:/ACCS_Work/Administrative/Credentials/akveg_private_read' # UPDATE FILE PATH

# Identify files
connection_script = paste(repository_path, 'package_DataProcessing', 'connect_database_postgresql.R',
                          sep = '/')
authentication_file = paste(authentication_path, 'authentication_akveg_private.csv',
                            sep = '/')
query_file = paste(repository_path, '05_queries', 'standard', 'Query_02_Site.sql',
                   sep = '/')

# Import database connection function
source(connection_script)

# Create a connection to the AKVEG Database
database_connection = connect_database_postgresql(authentication_file)

# Query the database and return result as tibble
query = n.readLines(query_file, 100, comment = '--', skip = 10, header = FALSE)
query = paste(query, collapse = '\n')
query_table = as_tibble(dbGetQuery(database_connection, query))