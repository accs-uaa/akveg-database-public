# INSTALL psycopg2 ON YOUR PYTHON DISTRIBUTION IF NOT ALREADY INSTALLED

# Import packages
import os
from package_DataProcessing import connect_database_postgresql
from package_DataProcessing import query_to_dataframe

# Set file paths
repository_path = 'C:/Users/timmn/Documents/Repositories/akveg-database' # UPDATE FILE PATH
authentication_path = 'N:/ACCS_Work/Administrative/Credentials/akveg_private_read' # UPDATE FILE PATH

# Identify files
authentication_file = os.path.join(authentication_path, 'authentication_akveg_private.csv')
query_file = os.path.join(repository_path, '05_queries', 'standard', 'Query_02_Site.sql')

# Create a connection to the AKVEG Database
database_connection = connect_database_postgresql(authentication_file)

# Query the database and return result as dataframe
file_read = open(query_file, 'r')
query = file_read.read()
file_read.close()
query_table = query_to_dataframe(database_connection, query)

# Print dataframe
print(query_table)
