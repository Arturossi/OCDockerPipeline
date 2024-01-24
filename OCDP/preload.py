#!/usr/lib/python3
'''
Functions to preload data
'''

# Imports
###############################################################################
import os
import sys
from typing import List

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

# Definitions
###############################################################################

# Classes
###############################################################################

# Functions
###############################################################################
## Private ##

## Public ##

def preload_PDBbind(pdbbind_database_index_path: str, ignored_pdbbind_database_index_path: str) -> List[str]:
    ''' Read the pdbbind database indexes from the specified files, filter the ignored indexes and return the pdbbind database indexes list.

    Parameters
    ----------
    pdbbind_database_index_path : str
        Path to the pdbbind database indexes file.
    ignored_pdbbind_database_index_path : str
        Path to the ignored pdbbind database indexes file.

    Returns
    -------
    pdbbind_targets : List[str]
        List of pdbbind database indexes.
    '''

    # If the pdb_database_index file exists
    if os.path.isfile(pdbbind_database_index_path):
        # Open the file
        with open(pdbbind_database_index_path, "r") as f:
            # Read the pdbbind database indexes from the specified file in the config file
            pdbbind_targets = list(set(line.strip() for line in f if line.strip()))
    else:
        sys.exit("The pdb_database_index file does not exist. Please, check the config file.")

    # If the ignored_pdb_database_index file exists
    if os.path.isfile(ignored_pdbbind_database_index_path):
        # Open the file
        with open(ignored_pdbbind_database_index_path, "r") as f:
            # Read the ignored pdbbind database indexes from the specified file in the config file
            ignored_pdbbind_targets = [line.strip() for line in f if line.strip()]
        
        # Remove from the pdbbind_targets list the ignored pdbbind database indexes
        pdbbind_targets = [x for x in pdbbind_targets if x not in ignored_pdbbind_targets]
    
    return pdbbind_targets

def preload_DUDEz(dudez_database_index_path: str, ignored_dudez_database_index_path: str) -> List[str]:
    ''' Read the DUDEz database indexes from the specified files, filter the ignored indexes and return the dudez database indexes list.

    Parameters
    ----------
    dudez_database_index_path : str
        Path to the dudez database indexes file.
    ignored_dudez_database_index_path : str
        Path to the ignored dudez database indexes file.

    Returns
    -------
    dudez_targets : List[str]
        List of dudez database indexes.
    '''

    # If the pdb_database_index file exists
    if os.path.isfile(dudez_database_index_path):
        # Open the file
        with open(dudez_database_index_path, "r") as f:
            # Read the dudez database indexes from the specified file in the config file
            dudez_targets = list(set(line.strip() for line in f if line.strip()))
    else:
        sys.exit("The dudez_database_index file does not exist. Please, check the config file.")

    # If the ignored_pdb_database_index file exists
    if os.path.isfile(ignored_dudez_database_index_path):
        # Open the file
        with open(ignored_dudez_database_index_path, "r") as f:
            # Read the ignored dudez database indexes from the specified file in the config file
            ignored_dudez_targets = [line.strip() for line in f if line.strip()]
        
        # Remove from the dudez_targets list the ignored dudez database indexes
        dudez_targets = [x for x in dudez_targets if x not in ignored_dudez_targets]
    
    return dudez_targets


# Aliases
###############################################################################
