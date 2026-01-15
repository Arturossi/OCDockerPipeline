"""
Module name: fileSystem

This module contains a set of Snakemake rules to organize the file system of
the OCDocker pipeline.
Author: Artur Duque Rossi

Created: 06-11-2023
Last modified: 06-11-2023
"""


# Initial directives
###############################################################################
configfile: "config.yaml"


# License
###############################################################################
"""
OCDocker pipeline
Authors: Rossi, A.D.; Pascutti, P.G.; Torres, P.H.M;
[Federal University of Rio de Janeiro, UFRJ, Brazil]
Contact info:
Carlos Chagas Filho Institute of Biophysics (IBCCF),
Modeling and Molecular Dynamics Laboratory,
Av. Carlos Chagas Filho 373 - CCS - bloco G1-19, Cidade Universitária - Rio de Janeiro, RJ - Brazil
E-mail address: arturossi10@gmail.com
This project is licensed under the GNU General Public License v3.0
"""

# Wildcards
###############################################################################

# Python functions and imports
###############################################################################
import os

from OCDocker.Config import get_config

oc_config = get_config()
ocdb_path = oc_config.paths.ocdb_path or ""
dudez_archive = oc_config.dudez_archive or os.path.join(ocdb_path, "DUDEz")
pdbbind_archive = oc_config.pdbbind_archive or os.path.join(ocdb_path, "PDBbind")

# Rules
###############################################################################

rule createDatabaseFolders:
    """
    Creates the base folders for the database.

    Inputs:
        (string): Database path where the folders will be created.
    Outputs:
        (directory): Database folders.
    Params:
        No parameters
    Threads: 1
    """
    priority: 999  # This rule should be executed first
    threads: 1  # Only one thread is more than enough
    #log:
    #    log=config["logDir"] + "/Basefiles.log",  # Basefiles log file 
    output:
        dudezDir=directory(dudez_archive), # Needed for the DUDEz database
        logDir=directory(config["logDir"]), # Needed for the log files
        pdbbindDir=directory(pdbbind_archive), # Needed for the PDBbind database
    run:
        # Check if the log directory does not exist
        if not os.path.exists(output.logDir):
            try:
                # Attempt to create the log directory
                os.makedirs(output.logDir)
                # Log an informational message if the directory is created to the general log file
                #logging.info(f"Log directory created at {output.logDir}")
            except Exception as e:
                # Log an error message if an error occurs during directory creation
                #logging.error(f"Error creating log directory: {str(e)}", exc_info=True)
                print(f"Error creating log directory: {str(e)}")

        # Check if the DUDEz directory already exists
        if os.path.exists(output.dudezDir):
            # Log an informational message if the directory exists
            #logging.info("DUDEz directory already exists at {output.dudezDir}")
            print("DUDEz directory already exists at {output.dudezDir}")
        else:
            try:
                # Attempt to create the DUDEz directory
                os.makedirs(output.dudezDir)
            except Exception as e:
                # Log an error message if an error occurs during directory creation
                #logging.error(f"Error creating DUDEz directory: {str(e)}", exc_info=True)
                print(f"Error creating DUDEz directory: {str(e)}")

        # Check if the PDBbind directory already exists
        if os.path.exists(output.pdbbindDir):
            # Log an informational message if the directory exists
            #logging.info("PDBbind directory already exists at {output.pdbbindDir}")
            print("PDBbind directory already exists at {output.pdbbindDir}")
        else:
            try:
                # Attempt to create the PDBbind directory
                os.makedirs(output.pdbbindDir)
            except Exception as e:
                # Log an error message if an error occurs during directory creation
                #logging.error(f"Error creating PDBbind directory: {str(e)}", exc_info=True)
                print(f"Error creating PDBbind directory: {str(e)}")
