"""
Module name: main

this is the main file from snakemake pipeline. It is responsible for the 
execution of the pipeline.

Author: Artur Duque Rossi

Created: 06-11-2023
Last modified: 06-11-2023
"""

# Initial directives
###############################################################################
configfile: "config.yaml"


# Python functions and imports
###############################################################################
import sys

from glob import glob

sys.path.append("/data/hd4tb/OCDocker/OCDocker")
from OCDocker.Initialise import *

import OCDP.preload as OCDPpre

pdb_database_index = config["pdb_database_index"]

pdbbind_targets = OCDPpre.preload_PDBbind(pdb_database_index, config["ignored_pdb_database_index"])


# Program imports
###############################################################################
include: "system/fileSystem.smk"
include: "system/database/pdbbind.smk"
#include: "system/database/dudez.smk"
include: "docking/plants.smk"
include: "docking/vina.smk"


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

def find_mols(wildcards):
    """
    Find the molecules from the desired database.
    """
    
    return ["tmp/" + wildcards.database + "!x!" + wildcards.receptor + "!x!" + wildcards.kind + "!x!" + os.path.basename(target) for target in glob(ocdb_path + "/" + wildcards.database + "/" + wildcards.receptor + "/compounds/" + wildcards.kind + "/*")]


# Wildcards
###############################################################################


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

rule db_pdbbind:
    """
    Set up the PDBbind database.
    """
    input:
        expand(ocdb_path + "/PDBbind/{pdbbind_target}/receptor.pdb",
            pdbbind_target = pdbbind_targets,
        ),


rule run_dockings_flag:
    """
    Run the docking software.
    """
    input:
        #plants_output = ocdb_path + "/{database}/{receptor}/compounds/{kind}/{target}/plantsFiles/run/prepared_ligand_entry_00001_conf_01.mol2",
        vina_output = ocdb_path + "/{database}/{receptor}/compounds/{kind}/{target}/vinaFiles/ligand_split_1.pdbqt"
    output:
        temp(touch("tmp/{database}!x!{receptor}!x!{kind}!x!{target}")),
    run:
        print(os.path.join(wildcards.database, wildcards.receptor, "compounds", wildcards.kind, wildcards.target, "plantsFiles", "run","prepared_ligand_entry_00001_conf_01.mol2"))

rule GetLigands:
    """
    Discover the ligands from the desired database.
    """
    input:
        ligands = find_mols
    output:
        temp(touch("tmp/{database}!-!{receptor}!-!{kind}")),
    run:
        print(input.ligands)

rule all:
    """
    Prepare the input files for PLANTS docking software.

    Usage:
        snakemake all --cores 20 --use-conda --conda-frontend mamba --keep-going --wms-monitor http://127.0.0.1:5000
    """
    input:
        allkinds = expand(
            "tmp/{database}!-!{receptor}!-!{kind}",
            database = ["PDBbind"], 
            receptor = pdbbind_targets, 
            kind = ["ligands", "decoys", "compounds"],
        ),
