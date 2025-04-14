# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Create connection to PostgreSQL database
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2024-07-26
# Usage: Can be executed in a Python 3.9+ distribution.
# Description: "Create connection to PostgreSQL database" is a function that loads a PostgreSQL connection and returns that connection to a variable.
# ---------------------------------------------------------------------------

# Define a function to create a connection to a PostgreSQL database
def connect_database_postgresql(authentication):
    """
    Description: creates a connection to a PostgreSQL database.
    Inputs: authentication -- a csv file containing the connection and authentication parameters
    Returned Value: function returns connection to PostgreSQL database.
    Preconditions: requires an existing PostgreSQL database with proper authentication by SSL set up and authentication files with the client
    """

    # Import packages
    import psycopg2
    import pandas as pd

    # Parse authentication parameters from csv
    parameters = pd.read_csv(authentication)
    parameter_dictionary = {
        'hostaddr': parameters.at[parameters.index[parameters['parameter'] == 'host'][0], 'value'],
        'port': parameters.at[parameters.index[parameters['parameter'] == 'port'][0], 'value'],
        'user': parameters.at[parameters.index[parameters['parameter'] == 'user'][0], 'value'],
        'password': parameters.at[parameters.index[parameters['parameter'] == 'password'][0], 'value'],
        'dbname': parameters.at[parameters.index[parameters['parameter'] == 'dbname'][0], 'value'],
        'sslmode': parameters.at[parameters.index[parameters['parameter'] == 'sslmode'][0], 'value'],
        'sslrootcert': parameters.at[parameters.index[parameters['parameter'] == 'sslrootcert'][0], 'value'],
        'sslcert': parameters.at[parameters.index[parameters['parameter'] == 'sslcert'][0], 'value'],
        'sslkey': parameters.at[parameters.index[parameters['parameter'] == 'sslkey'][0], 'value'],
    }

    # Establish database connection using authentication parameters
    try:
        print('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect(**parameter_dictionary)
    # Return error if connection fails
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return None
    print("Connection successful")
    # Return the database connection
    return connection
