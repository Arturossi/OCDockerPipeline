"""
Module name: plants

This module contains a set of Snakemake rules to run the Vina docking
software.
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

ocdb_path = get_config().paths.ocdb_path or ""

# Rules
###############################################################################


rule runVina:
    """
    Run Vina docking software.

    Inputs:
        (file): The prepared ligand file.
        (file): The prepared receptor file.
    Outputs:
        (file): The Vina output file.
    """
    params:
        vina_log=config["logDir"] + "/vina.log",
    input:
        ligand = os.path.join(
            ocdb_path, "{database}", "{receptor}", "compounds", "{kind}",
            "{target}", "ligand.smi"
        ),
        receptor = os.path.join(ocdb_path, "{database}", "{receptor}", "receptor.pdb"),
        box = os.path.join(
            ocdb_path, "{database}", "{receptor}", "compounds", "{kind}",
            "{target}", "boxes", "box0.pdb"
        ),
    output:
        prepared_ligand = os.path.join(
            ocdb_path, "{database}", "{receptor}", "compounds", "{kind}",
            "{target}", "prepared_{target}.pdbqt"
        ),
        vina_output = os.path.join(
            ocdb_path, "{database}", "{receptor}", "compounds", "{kind}",
            "{target}", "vinaFiles", "{target}_split_1.pdbqt"
        ),
    threads: 1
    run:
        import OCDocker.Docking.Vina as ocvina
        import OCDocker.Receptor as ocr
        import OCDocker.Ligand as ocl
        import OCDocker.Error as ocerror
        from OCDocker.DB.Models.Ligands import Ligands
        from OCDocker.DB.Models.Receptors import Receptors

        # Create ligand and receptor objects
        ligand = input.ligand
        receptor = input.receptor

        # Set the base paths
        baseLigPath = os.path.dirname(ligand)

        # Set the boxFile
        boxFile = input.box
        confFile = os.path.join(baseLigPath, "vinaFiles", "conf_vina.txt")

        prepared_receptor = os.path.join(ocdb_path, wildcards.database, wildcards.receptor, "prepared_receptor.pdbqt")

        # Create the Receptor object
        vina_receptor = ocr.Receptor(
            receptor, relativeASAcutoff = 0.7, name = wildcards.receptor
        )
        vina_ligand = ocl.Ligand(ligand, name = wildcards.target)

        # Set the receptor dictionary to insert in the database
        receptorDict = vina_receptor.get_descriptors()
        receptorDict["name"] = wildcards.receptor
        # Insert it
        Receptors.insert(receptorDict, ignorePresence = True)

        # Set the ligand dictionary to insert in the database
        ligandDict = vina_ligand.get_descriptors()
        ligandDict["name"] = wildcards.receptor + "_" + wildcards.target
        # Insert it
        Ligands.insert(ligandDict, ignorePresence = True)

        # If there is no box, finish the rule
        if not os.path.exists(boxFile):
            return ocerror.Error.file_not_exist(
                f"Box file '{boxFile}' does not exist.", ocerror.ReportLevel.ERROR
            )

        # Create the Vina object
        vina_obj = ocvina.Vina(
            confFile,
            boxFile,
            vina_receptor,
            prepared_receptor,
            vina_ligand,
            output.prepared_ligand,
            f"{baseLigPath}/vinaFiles/" + wildcards.target + ".log",
            f"{baseLigPath}/vinaFiles/" + wildcards.target + ".pdbqt",
            name = f"Vina " + wildcards.receptor + "-" + wildcards.target)

        # Check if there is already a prepared receptor file
        if not os.path.exists(prepared_receptor):
            # Prepare the receptor
            vina_obj.run_prepare_receptor()

        # Check if there is already a prepared ligand file
        if not os.path.exists(output.prepared_ligand):
            # Prepare the ligand
            vina_obj.run_prepare_ligand()

        # Run Vina
        vina_obj.run_docking()

        # Split the docking results into multiple files
        vina_obj.split_poses(f"{baseLigPath}/vinaFiles", logFile = "")

        # Get the docking poses
        vinaDockingPoses = vina_obj.get_docked_poses()
