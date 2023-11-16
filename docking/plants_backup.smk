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

# Rules
###############################################################################

rule prepareLigand:
    """
    Prepare the ligand for docking with plants.

    Inputs:
        (file): Input smi file.
    Outputs:
        (file): The prepared ligand file.
    """
    params:
        plants_log = config["logDir"] + "/plants.log",
    input:
        ligand = "{database}/{pdbbind_target}/compounds/{kind}/{target}/ligand.smi",
    output:
        prepared_ligand = "{database}/{pdbbind_target}/compounds/{kind}/{target}/prepared_ligand.mol2",
    threads: 1
    run:
        import OCDocker.PLANTS as ocplants
        # Prepare the ligand
        ocplants.run_prepare_ligand(input.ligand, output.prepared_ligand, params.plants_log)

rule prepareReceptor:
    """
    Prepare the receptor for docking with plants.

    Inputs:
        (file): Input pdb file.
    Outputs:
        (file): The prepared receptor file.
    """
    params:
        plants_log = config["logDir"] + "/plants.log",
    input:
        receptor = "{database}/{pdbbind_target}/receptor.pdb",
    output:
        prepared_receptor = "{database}/{pdbbind_target}/prepared_receptor.mol2",
    threads: 1
    run:
        import OCDocker.PLANTS as ocplants
        # Prepare the receptor
        ocplants.run_prepare_receptor(input.receptor, output.prepared_receptor, params.plants_log)

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
        plants_log = config["logDir"] + "/plants.log",
        plants_config = config["plants_config"],
    input:
        prepared_ligand = "{database}/{pdbbind_target}/compounds/{kind}/{target}/prepared_ligand.mol2",
        prepared_receptor = "{database}/{pdbbind_target}/prepared_receptor.mol2",
    output:
        plants_output = "{database}/{pdbbind_target}/compounds/{kind}/{target}/plants_output.mol2",
    threads: 1
    run:
        import OCDocker.PLANTS as ocplants
        # Set the base paths
        baseLigPath = os.path.dirname(input.prepared_ligand)

        # Set the boxFile
        boxFile = f"{baseLigPath}/boxes/box0.pdb"
        confFile = f"{baseLigPath}/{wildcards.target}/plantsFiles/conf_plants.txt"

        # If there is no box, finish the rule
        if not os.path.exists(boxFile):
            return ocerror.Error.file_not_exists(f"Box file '{boxFile}' does not exist.", ocerror.ReportLevel.ERROR)

        # Create the Receptor object
        plants_receptor = ocr.Receptor(input.prepared_ligand, relativeASAcutoff = 0.7, name=f"{wildcards.pdbbind_target}")
        plants_ligand = ocl.Ligand(input.prepared_receptor, name=f"{wildcards.target}")
        
        # Create the PLANTS object
        plants_obj = ocplants.PLANTS(confFile, boxFile, plants_receptor, input.prepared_receptor, plants_ligand, input.prepared_ligand, f"{baseLigPath}/{wildcards.target}/plantsFiles/{wildcards.target}.log", f"{baseLigPath}/{wildcards.target}/plantsFiles", name=f"PLANTS {wildcards.pdbbind_target}-{wildcards.target}")
        # Run PLANTS
        ocplants.run_plants(input.prepared_ligand, input.prepared_receptor, output.plants_output, params.plants_config, params.plants_log)
