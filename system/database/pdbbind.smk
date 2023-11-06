"""
Module name: pdbbind

This module contains a set of Snakemake rules to deal with the pdbbind dataset.
Author: Artur Duque Rossi

Created: 06-11-2023
Last modified: 06-11-2023
"""

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

# Wildcards
###############################################################################

# Python functions and imports
###############################################################################
import sys
sys.path.append("/data/hd4tb/OCDocker/OCDocker")
global emptyArgs
emptyArgs = True
from OCDocker.InitialiseNoArgs import *

# Rules
###############################################################################

rule downloadPdbBind:
    """
    Downloads the PDBbind dataset. [For now this is not possible, since it needs to login in the website]. This rule is present here only for future use.

    Inputs:
        (str): Output directory.
    Outputs:
        (file): PDBbind dataset.
    """
    run:
        print("This rule is not available yet. Please, download the PDBbind dataset manually and put it in the input directory.")

rule processPdbBind:
    """
    Processes the PDBbind dataset.

    Outputs:
        (file): Receptor file.
        (file): Ligand file.
    """
    input:
        pdbbindPath = pdbbind_archive, # Needed for the DUDEz database
    output:
        receptor = pdbbind_archive + "/{target}/receptor.pdb",
        ligand = pdbbind_archive + "/{target}/compounds/ligand/ligand/ligand.smi",
    run:
        print("This rule is not available yet. Please, download the PDBbind dataset manually and put it in the input directory.")

rule extractPdbBind:
    """
    Extracts the PDBbind dataset.

    Inputs:
        (str): Input pdbbind .tar.gz file.
        (str): Output directory.
    Outputs:
        (directory): PDBbind dataset.
    """
    input:
        pdbbindTarGzPath = config["pdbbindTarGzPath"],
    output:
        pdbbindPath = directory(pdbbind_archive)
    threads: 1
    run:
        import OCDocker.Toolbox.FilesFolders as ocff
        ocff.untar(input.pdbbindTarGzPath, pdbbind_archive)
