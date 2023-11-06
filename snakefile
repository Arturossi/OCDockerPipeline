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
import sys
sys.path.append("/data/hd4tb/OCDocker/OCDocker")
sys.argv.append("--noArgs")
from OCDocker.Initialise import *

# Program imports
###############################################################################
include: "system/fileSystem.smk"
include: "system/database/pdbbind.smk"

# Python definitions
###############################################################################

# Set the output_level according to the log_level from the config file
if config["log_level"] == "debug":
    args.output_level = 4
elif config["log_level"] == "info":
    args.output_level = 2
elif config["log_level"] == "warning":
    args.output_level = 1
elif config["log_level"] == "error":
    args.output_level = 0
else:
    args.output_level = 3

# Set some more arguments
args.cpu_cores = config["cpu_cores"]
args.available_cores = args.cpu_cores - 1 # The main thread is not counted
args.multiprocess = 1                     # 0: single process; 1: multiprocess
args.generate_report = False              # Generate a report at the end of the pipeline
args.zip_output = False                   # Zip the output files
args.update = False                       # Update the pipeline

# Wildcards
###############################################################################

# Initial directives
###############################################################################
configfile: "config.yaml"


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
        dudezDir=dudez_archive, # Needed for the DUDEz database
        logDir=config["logDir"], # Needed for the log files
        pdbbindDir=pdbbind_archive, # Needed for the PDBbind database
        
        

