"""
Module name: plants

This module contains a set of Snakemake rules to run the PLANTS docking
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

# Rules
###############################################################################


rule runPLANTS:
    """
    Run PLANTS docking software.

    Inputs:
        (file): The prepared ligand file.
        (file): The prepared receptor file.
    Outputs:
        (file): The PLANTS output file.
    """
    params:
        plants_log=config["logDir"] + "/plants.log",
    input:
        ligand="{database}/{pdbbind_target}/compounds/{kind}/{target}/ligand.smi",
    output:
        prepared_receptor = "{database}/{pdbbind_target}/compounds/{kind}/{target}/prepared_receptor.mol2",
        prepared_ligand = "{database}/{pdbbind_target}/compounds/{kind}/{target}/plantsFiles/run/prepared_ligand.mol2",
        plants_output = "{database}/{pdbbind_target}/compounds/{kind}/{target}/plantsFiles/run/prepared_ligand_entry_00001_conf_01.mol2",
    threads: 1
    run:
        import OCDocker.PLANTS as ocplants

        # Set the base paths
        baseLigPath = os.path.dirname(input.ligand)

        # Set the boxFile
        boxFile = f"{baseLigPath}/boxes/box0.pdb"
        confFile = f"{baseLigPath}/{wildcards.target}/plantsFiles/conf_plants.txt"

        # Set the prepared receptor file name
        # prepared_receptor = "{wildcards.database}/{wildcards.pdbbind_target}/prepared_receptor.mol2",

        # If there is no box, finish the rule
        if not os.path.exists(boxFile):
            return ocerror.Error.file_not_exists(
                f"Box file '{boxFile}' does not exist.", ocerror.ReportLevel.ERROR
            )

            # Create the Receptor object
        plants_receptor = ocr.Receptor(
            input.receptor, relativeASAcutoff=0.7, name=f"{wildcards.pdbbind_target}"
        )
        plants_ligand = ocl.Ligand(input.ligand, name=f"{wildcards.target}")

        # Create the PLANTS object
        plants_obj = ocplants.PLANTS(
            confFile,
            boxFile,
            plants_receptor,
            output.prepared_receptor,
            plants_ligand,
            output.prepared_ligand,
            f"{baseLigPath}/{wildcards.target}/plantsFiles/{wildcards.target}.log",
            f"{baseLigPath}/{wildcards.target}/plantsFiles",
            name=f"PLANTS {wildcards.pdbbind_target}-{wildcards.target}",
        )

        # Check if there is already a prepared receptor file
        if not os.path.exists(output.prepared_receptor):
            # Prepare the receptor
            plants_obj.prepare_receptor()

            # Prepare the ligand
        plantsTest.run_prepare_ligand()

        # Run PLANTS
        plantsTest.run_docking()

        # Get the docking poses
        plantsdockingPoses = plantsTest.get_docked_poses()

        print(plantsdockingPoses)
