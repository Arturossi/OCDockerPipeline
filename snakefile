"""
Module name: main

this is the main file from snakemake pipeline. It is responsible for the 
execution of the pipeline.

Author: Artur Duque Rossi

Created: 06-11-2023
Last modified: 06-11-2023
"""

# Python functions and imports
###############################################################################
import os
import sys

sys.path.append("/data/hd4tb/OCDocker/OCDocker")
from OCDocker.Initialise import *

# Program imports
###############################################################################
include: "system/fileSystem.smk"
include: "system/database/pdbbind.smk"

# Python definitions
###############################################################################

# Set the output_level according to the log_level from the config file
if config["log_level"] == "debug":
    output_level = 5
elif config["log_level"] == "info":
    output_level = 4
elif config["log_level"] == "warning":
    output_level = 2
elif config["log_level"] == "error":
    output_level = 1
elif config["log_level"] == "none":
    output_level = 0
else:
    output_level = 3

# Set some more arguments
cpu_cores = config["cpu_cores"]
available_cores = cpu_cores - 1 # The main thread is not counted
multiprocess = 1                # 0: single process; 1: multiprocess
generate_report = False         # Generate a report at the end of the pipeline
zip_output = False              # Zip the output files
update = False                  # Update the pipeline
overwrite = False               # Overwrite the output files

# Wildcards
###############################################################################

# Initial directives
###############################################################################
configfile: "config.yaml"

# Parameterise the pdbbind database index path from the config file
pdbbind_database_index_path = config["pdb_database_index"]

# If the pdb_database_index file exists
if os.path.isfile(pdbbind_database_index_path):
    # Open the file
    with open(pdbbind_database_index_path, "r") as f:
        # Read the pdbbind database indexes from the specified file in the config file
        pdbbind_targets = list(set(line.strip() for line in f if line.strip()))
else:
    sys.exit("The pdb_database_index file does not exist. Please, check the config file.")

# Parameterise the ignored pdbbind database index path from the config file
ignored_pdbbind_database_index_path = config["ignored_pdb_database_index"]

# If the ignored_pdb_database_index file exists
if os.path.isfile(ignored_pdbbind_database_index_path):
    # Open the file
    with open(ignored_pdbbind_database_index_path, "r") as f:
        # Read the ignored pdbbind database indexes from the specified file in the config file
        ignored_pdbbind_targets = [line.strip() for line in f if line.strip()]
    
    # Remove from the pdbbind_targets list the ignored pdbbind database indexes
    pdbbind_targets = [x for x in pdbbind_targets if x not in ignored_pdbbind_targets]

# License
###############################################################################
'''
OCDocker pipeline
Authors: Rossi, A.D.; Pascutti, P.G.; Torres, P.H.M;
[Federal University of Rio de Janeiro, UFRJ, Brazil]
Contact info:
Carlos Chagas Filho Institute of Biophysics (IBCCF),
Modeling and Molecular Dynamics Laboratory,
Av. Carlos Chagas Filho 373 - CCS - bloco G1-19, Cidade Universitária - Rio de Janeiro, RJ - Brazil
E-mail address: arturossi10@gmail.com
This project is licensed under the GNU General Public License v3.0
'''

# Rules
###############################################################################

rule all:
    """
    Run all the steps of the pipeline.
    
    Inputs:
        (str): PDBbind database path.
        (str): DUDEz database path.
    Example usage:
        snakemake all --cores 12 --use-conda --keep-going --conda-frontend mamba
    """
    input:
        #dudezDir=dudez_archive, # Needed for the DUDEz database
        #logDir=config["logDir"], # Needed for the log files
        #dbbindDir = pdbbind_archive, # Needed for the PDBbind database
        tmpFile = expand("/tmp/ocdocker/{pdbbind_target}", pdbbind_target = pdbbind_targets),
    run:
        # Remove sentinel files
        for file in os.listdir("tmp"):
            if file.endswith(".sentinel"):
                os.remove(os.path.join("tmp", file))
        
        # Concatenate refined-set path
        refinedset = os.path.join(pdbbind_archive, "refined-set")

        # If refined set exists
        if os.path.isdir(refinedset):
            import shutil
            # Remove the refined-set folder
            shutil.rmtree(refinedset)
            
        # Print Finished
        print("Finished!")

rule process:
    """
    Process the data.

    Inputs:
        (str): temporary file.

    Outputs:
        (str): PDBbind database receptor files.
        (str): PDBbind database ligand files.
    """
    params:
        protein = pdbbind_archive + "/{pdbbind_target}",
        pdbbind_log = config["logDir"] + "/pdbbind.log",
    input:
        pdbbind_receptor = pdbbind_archive + "/{pdbbind_target}/receptor.pdb",
        pdbbind_ligand = pdbbind_archive + "/{pdbbind_target}/compounds/ligands/ligand/ligand.smi",
    output:
        tmpFile = temp(touch("/tmp/ocdocker/{pdbbind_target}")),
    run:
        # Missing error string
        error_string = ""

        # Check if the receptor exists
        if not os.path.isfile(input.pdbbind_receptor):
            # Set the error string
            error_string += f" is missing receptor"

        # Check if the ligand exists
        if not os.path.isfile(input.pdbbind_ligand):
            # If the error string is empty
            if error_string == "":
                # Set the error string
                error_string += f" is missing ligand"
            else:
                # Set the error string
                error_string += f" and ligand"

        # If the folder has incomplete data
        if error_string:
            # Add the pdbbind_target to the pdbbind_log
            with open(params.pdbbind_log, "a") as f:
                f.write(params.protein + f"{error_string} file(s).\n")
            # Remove the folder
            import shutil
            shutil.rmtree(params.protein)


        
