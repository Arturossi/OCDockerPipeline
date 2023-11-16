"""
Module name: dudez

This module contains a set of Snakemake rules to deal with the dudez dataset.
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
from OCDocker.Initialise import *
from typing import List

def get_targets(file: str) -> List[str]:
    with open("tmp/DUDE-Z_targets", 'r') as file:
        return file.read().splitlines()
    
# If there is an file for DUDEz in the path
if os.path.isfile(config["dudez_database_index"]):
    # Get the targets
    dudez_targets = get_targets(config["dudez_database_index"])
else:
    import OCDocker.Toolbox.Downloading as ocdown
    # Download the benchmark grids indexes
    ocdown.download_url(f"{dudez_download}/DUDE-Z-benchmark-grids/DUDE-Z_targets", "tmp/DUDE-Z_targets")
    # Get the targets
    dudez_targets = get_targets("tmp/DUDE-Z_targets")


# Rules
###############################################################################

rule update_DUDEz:
    """
    Updates the DUDEz database.
    
    Inputs:
        (str): DUDEz database path.

    Example usage:
        snakemake all --cores 12 --use-conda --keep-going --conda-frontend mamba
    """
    input:
        tmpFile = expand("/tmp/ocdocker/{dudez_target}", dudez_target = dudez_targets),
    output:
        dudez_complete = temp(touch("/tmp/ocdocker/dudez_complete.sentinel")),
    run:
        # Remove sentinel files
        for file in os.listdir("tmp"):
            if file.endswith("dudez_complete.sentinel"):
                os.remove(os.path.join("tmp", file))
            
        # Print Finished
        print("Finished!")

rule process_DUDEz:
    """
    Process the DUDEz data.

    Inputs:
        (str): temporary file.

    Outputs:
        (str): DUDEz database receptor files.
        (str): DUDEz database ligand files.
    """
    params:
        protein = dudez_archive + "/{dudez_target}",
        dudez_log = config["logDir"] + "/dudez.log",
    input:
        dudez_receptor = dudez_archive + "/{dudez_target}/receptor.pdb",
    output:
        tmpFile = temp(touch("/tmp/ocdocker/{dudez_target}")),
    run:
        # Missing error string
        error_string = ""

        # Check if the receptor exists
        if not os.path.isfile(input.dudez_receptor):
            # Set the error string
            error_string += f" is missing receptor"

        # If the folder has incomplete data
        if error_string:
            # Add the pdbbind_target to the dudez_log
            with open(params.dudez_log, "a") as f:
                f.write(params.protein + f"{error_string} file(s).\n")
            # Remove the folder
            import shutil
            shutil.rmtree(params.protein)

rule download_process_DUDEz:
    """
    Downdloads the DUDEz database.

    Outputs:
        (file): Receptor file.
        (file): Ligand file.
    """
    output:
        receptor = dudez_archive + "/{target}/receptor.pdb",
        #ligand = dudez_archive + "/{target}/compounds/ligands/{ligand_id}/ligand.smi",
        #decoy = dudez_archive + "/{target}/compounds/decoys/{decoy_id}/ligand.smi",
    #conda:
    #    "../../envs/ocdocker.yaml"
    threads: 1
    run:
        import OCDocker.Toolbox.Conversion as occonversion
        import OCDocker.Toolbox.Downloading as ocdown
        import OCDocker.Toolbox.FilesFolders as ocff

        if wildcards.target == "D4":
            ptn_target = "DRD4"
        else:
            ptn_target = wildcards.target

        # Create a folder for the target in the archive
        #_ = ocff.safe_create_dir(f"{dudez_archive}/{ptn_target}")

        # Check if the target receptor does not exists or the user wants to overwrite it
        if not os.path.isfile(f"{dudez_archive}/{ptn_target}/receptor.pdb") or overwrite:
            # Download the target receptor
            ocdown.download_url(f"{dudez_download}/DOCKING_GRIDS_AND_POSES/{ptn_target}/rec.crg.pdb", f"{dudez_archive}/{ptn_target}/receptor.pdb")

        # Check if the reference ligand does not exists or the user wants to overwrite it
        if not os.path.isfile(f"{dudez_archive}/{ptn_target}/ligand.mol2") or overwrite:
            # Download the target receptor
            ocdown.download_url(f"{dudez_download}/DOCKING_GRIDS_AND_POSES/{ptn_target}/xtal-lig.pdb", f"{dudez_archive}/{ptn_target}/reference_ligand.pdb")

        # Check if the target dudez ligands does not exists or the user wants to overwrite it
        if not os.path.isfile(f"{dudez_archive}/{ptn_target}/ligands.smi") or overwrite:
            # Download the dudeZ ligands
            ocdown.download_url(f"{dudez_download}/DUDE-Z-benchmark-grids/{wildcards.target}/ligands.smi", f"{dudez_archive}/{ptn_target}/ligands.smi")

        # Check if the target dudez decoys does not exists or the user wants to overwrite it
        if not os.path.isfile(f"{dudez_archive}/{ptn_target}/decoys.smi") or overwrite:
            # Download the dudeZ ligands
            ocdown.download_url(f"{dudez_download}/DUDE-Z-benchmark-grids/{wildcards.target}/decoys.smi", f"{dudez_archive}/{ptn_target}/decoys.smi")
        
        # Parameterize the compounds path
        targetc = os.path.join(dudez_archive, wildcards.target, "compounds")

        # Create the compound folder (will hold all compounds, no matter if they are ligand or decoy)
        _ = ocff.safe_create_dir(targetc)

        # List to hold the tuples for each processing that will be made
        process_list = ["ligands", "decoys"]

        # For each data
        for data in process_list:
            # Create the ligands folder
            _ = ocff.safe_create_dir(f"{targetc}/{data}")
            # Process the ligands, splitting them into the multiple files
            with open(os.path.join(dudez_archive, wildcards.target, f"{data}.smi"), 'r') as f:
                for line in f:
                    # Get the smiles and name of the ligand
                    smiles, name = line.split()

                    # Create the ligand folder using its name
                    _ = ocff.safe_create_dir(f"{targetc}/{data}/{name}")
                    
                    # Test if the file exists
                    if overwrite or not os.path.isfile(f"{targetc}/{data}/{name}/ligand.mol2"):
                        # Check if the outputfile exists
                        if os.path.isfile(f"{targetc}/{data}/{name}/ligand.mol2"):
                            # Remove the file
                            os.remove(f"{targetc}/{data}/{name}/ligand.mol2")
                        # Convert it to mol2 (NOTE: There are many molecules with SAME name... currently I am not handling this. I am just accounting the first molecule and discarding the others. IMPORTANT: Error messages WILL pop while processing the data here! They may be safe to ignore, I guess...)
                        _ = occonversion.convertMolsFromString(smiles, f"{targetc}/{data}/{name}/ligand.mol2")
                        # Save a smiles file (to avoid compatibility issues)
                        with open(f"{targetc}/{data}/{name}/ligand.smi", 'w') as f:
                            f.write(f"{smiles}")
                    else:
                        ocerror.Error.file_exists(f"File '{targetc}/{data}/{name}/ligand.mol2' already exists. Skipping...", level = ocerror.ReportLevel.WARNING)
