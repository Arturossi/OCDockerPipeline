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

## DUDEz database ##



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
    input:
        expand("{output}/", output=config["output"])
