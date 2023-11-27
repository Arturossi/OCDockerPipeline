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
    output:
        prepared_ligand = "{database}/{receptor}/compounds/{kind}/{target}/prepared_ligand.mol2",
        plants_output = "{database}/{receptor}/compounds/{kind}/{target}/plantsFiles/run/prepared_ligand_entry_00001_conf_01.mol2",
    threads: 1
    run:
        import OCDocker.Docking.PLANTS as ocplants
        import OCDocker.Receptor as ocr
        import OCDocker.Ligand as ocl
        import shutil

        # If the run folder exists
        if os.path.exists(os.path.join(wildcards.database, wildcards.receptor, "compounds", wildcards.kind, wildcards.target, "plantsFiles", "run")):
            shutil.rmtree(os.path.join(wildcards.database, wildcards.receptor, "compounds", wildcards.kind, wildcards.target, "plantsFiles", "run"))

        ligand = os.path.join(wildcards.database, wildcards.receptor, "compounds", wildcards.kind, wildcards.target, "ligand.smi")
        receptor = os.path.join(wildcards.database, wildcards.receptor, "receptor.pdb")

        # Set the base paths
        baseLigPath = os.path.dirname(ligand)

        # Set the boxFile
        boxFile = f"{baseLigPath}/boxes/box0.pdb"
        confFile = os.path.join(baseLigPath, "plantsFiles", "conf_plants.txt")

        # Set the input receptor variable
        prepared_receptor = os.path.join(wildcards.database, wildcards.receptor, "prepared_receptor.mol2")

        # If there is no box, finish the rule
        if not os.path.exists(boxFile):
            return ocerror.Error.file_not_exist(
                f"Box file '{boxFile}' does not exist.", ocerror.ReportLevel.ERROR
            )

        # Create the Receptor object
        plants_receptor = ocr.Receptor(
            receptor, relativeASAcutoff = 0.7, name = wildcards.receptor
        )
        plants_ligand = ocl.Ligand(ligand, name = wildcards.target)

        # Create the PLANTS object
        plants_obj = ocplants.PLANTS(
            confFile,
            boxFile,
            plants_receptor,
            prepared_receptor,
            plants_ligand,
            output.prepared_ligand,
            os.path.join(baseLigPath, "plantsFiles", wildcards.target + ".log"),
            os.path.join(baseLigPath, "plantsFiles"),
            name = "PLANTS " + wildcards.receptor + "-" + wildcards.target,
        )

        # Check if there is already a prepared receptor file
        if not os.path.exists(prepared_receptor):
            # Prepare the receptor
            plants_obj.run_prepare_receptor()

        # Check if there is already a prepared ligand file
        if not os.path.exists(output.prepared_ligand):
            # Prepare the ligand
            plants_obj.run_prepare_ligand()

        # Run PLANTS
        plants_obj.run_docking()

        # Get the docking poses
        plantsdockingPoses = plants_obj.get_docked_poses()

        print(plantsdockingPoses)
