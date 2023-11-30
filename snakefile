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

pdbbind_targets = OCDPpre.preload_PDBbind(pdb_database_index, config["ignored_pdb_database_index"])[:5]


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

rule run_rescoring:
    """
    Run the rescoring of the poses from all docking software.
    """
    input:
        plants_output = ocdb_path + "/{database}/{receptor}/compounds/{kind}/{target}/plantsFiles/run/prepared_ligand_entry_00001_conf_01.mol2",
        vina_output = ocdb_path + "/{database}/{receptor}/compounds/{kind}/{target}/vinaFiles/ligand_split_1.pdbqt"
    output:
        temp(touch("tmp/{database}!x!{receptor}!x!{kind}!x!{target}")),
    threads: 1
    run:
        # Test if the output files exists
        if os.path.isfile(input.plants_output) and os.path.isfile(input.vina_output):
            # To fix Loky backend issue
            from joblib import parallel_backend

            # Import the libraries
            import OCDocker.Docking.PLANTS as ocplants
            import OCDocker.Docking.Smina as ocsmina
            import OCDocker.Docking.Vina as ocvina
            import OCDocker.Rescoring.ODDT as ocoddt
            import OCDocker.Toolbox.Conversion as occonversion
            import OCDocker.Toolbox.FilesFolders as ocff
            import OCDocker.Toolbox.MoleculeProcessing as ocmolproc
            import OCDocker.Processing.Preprocessing.RmsdClustering as ocrmsdclust

            # Get the base directory for the output files
            plants_base_dir = os.path.dirname(input.plants_output)
            vina_base_dir = os.path.dirname(input.vina_output)

            # Set the base path for the ligand
            basePath = os.path.dirname(vina_base_dir)

            # Make the smina base dir from the base path (it is similar to the vina base dir, but with sminaFiles instead of vinaFiles)
            smina_base_dir = os.path.join(basePath, "sminaFiles")

            # Create the base smina dir if it does not exists
            ocff.safe_create_dir(smina_base_dir)

            # Find the poses contained in this directory
            plants_poses = ocplants.get_docked_poses(plants_base_dir)
            vina_poses = ocvina.get_docked_poses(vina_base_dir)

            # Concatenate the poses lists from vina and plants into a single list
            poses_list = vina_poses + plants_poses

            # Get the rmsd matrix from the poses list
            mols_mat = ocmolproc.get_rmsd_matrix(poses_list)

            # Get the rmsd matrix from the poses list
            rmsdMatrix = ocmolproc.get_rmsd_matrix(poses_list)

            # Get the clusters
            clusters = ocrmsdclust.cluster_rmsd(rmsdMatrix, algorithm = 'agglomerativeClustering', outputPlot = f"{basePath}/medoids.png")

            # Get the medoids (The plot is just for visualization, it is not required)
            medoids = ocrmsdclust.get_medoids(rmsdMatrix, clusters, onlyBiggest = True) # type: ignore

            # If there is no medoid, return
            if len(medoids) == 0:
                return

            # Create the processed_Medoids dict
            processedMedoids = {
                    "plants" : [],
                    "vina" : [],
                    "smina" : []
                }

            ## Prepare the files for rescoring
            # For each file in the medoids list
            for medoid in medoids:
                # Get the directory of the file
                fdirectory = os.path.dirname(medoid)
                # If the file is a vina like file (.pdbqt)
                if medoid.endswith(".pdbqt"):
                    ## Prepare it for PLANTS
                    # Set the output and prepared output file path
                    outfile = f"{fdirectory}/{os.path.basename(medoid).replace('.pdbqt', '.mol2')}"
                    preparedOutfile = f"{fdirectory}/{os.path.basename(medoid).replace('.pdbqt', '_prepared.mol2')}"
                    # Convert the file to mol2
                    occonversion.convertMols(medoid, outfile)
                    # Prepare the mol2 file for PLANTS
                    ocplants.run_prepare_ligand(outfile, preparedOutfile)
                    # Append the prepared file to the processedMedoids dict
                    processedMedoids["vina"].append(medoid)
                    processedMedoids["smina"].append(medoid)
                    processedMedoids["plants"].append(preparedOutfile)
                # If the file is a plants like file (.mol2)
                elif medoid.endswith(".mol2"):
                    ## Prepare it for Vina like programs
                    # Set the prepared output file path
                    preparedOutfile = f"{fdirectory}/{os.path.basename(medoid).replace('.mol2', '_prepared.pdbqt')}"
                    # Prepare the pdbqt file for Vina
                    ocvina.run_prepare_ligand(medoid, preparedOutfile)
                    # Append the prepared file to the processedMedoids dict
                    processedMedoids["vina"].append(preparedOutfile)
                    processedMedoids["smina"].append(preparedOutfile)
                    processedMedoids["plants"].append(medoid)
            # Dictionary with the medoids and its docking method (to be correctly parsed by the next function)
            medoidsDict = {}

            #########################
            ## Run Smina rescoring  #
            #########################

            # Set the config file
            sminaConf = f"{smina_base_dir}/conf_smina.txt"

            boxFile = f"{basePath}/boxes/box0.pdb"

            # Set the input prepared receptor (must be already prepared, which we got from Vina docking rule)
            preparedReceptor = os.path.join(ocdb_path, wildcards.database, wildcards.receptor, "prepared_receptor.pdbqt")

            # Create it
            ocsmina.gen_smina_conf(boxFile, sminaConf, preparedReceptor)

            # For each scoring function for PLANTS
            for sf in smina_scoring_functions:
                ocsmina.run_rescore(sminaConf, processedMedoids["smina"], smina_base_dir, sf, splitLigand = False)
            
            #########################
            ## Run Vina rescoring   #
            #########################

            # Set the config file (No need to create it, it is already created)
            vinaConf = f"{vina_base_dir}/conf_vina.txt"

            # For each scoring function for Vina
            for sf in vina_scoring_functions:
                ocvina.run_rescore(vinaConf, processedMedoids["vina"], vina_base_dir, sf, splitLigand = False)
            
            #########################
            ## Run PLANTS rescoring #
            #########################

            # Get the parent directory of the PLANTS run (to run the rescoring)
            plants_parent_base_dir = os.path.dirname(plants_base_dir)

            # Set the pose list path
            poseListPath = f"{plants_parent_base_dir}/pose_list.txt"

            # Write the pose_list file for rescoring
            pose_list = ocplants.write_pose_list(processedMedoids["plants"], poseListPath)

            # Set the input prepared receptor (must be already prepared, which we got from PLANTS docking rule)
            preparedReceptor_plants = os.path.join(ocdb_path, wildcards.database, wildcards.receptor, "prepared_receptor.mol2")

            # For each scoring function for PLANTS
            for sf in plants_scoring_functions:
                # Set the output path
                outPath = f"{plants_parent_base_dir}/run_{sf}"

                # Set the config file
                confFile = os.path.join(plants_parent_base_dir, wildcards.target + f"_rescoring_{sf}.txt")

                # Run the rescoring
                ocplants.run_rescore(confFile, pose_list, outPath, preparedReceptor_plants, sf, logFile = "", overwrite = False) # type: ignore

            #########################
            ## Run ODDT rescoring   #
            #########################

            # Use the threading backend context manager instead of the Loky backend
            with parallel_backend("threading"):
                df = ocoddt.run_oddt(preparedReceptor, medoids, wildcards.target, f"{basePath}/oddt") # type: ignore

            ##############################
            ## Get the rescoring results #
            ##############################

            # Vina
            vinaRescoringResult = ocvina.read_rescore_logs(ocvina.get_rescore_log_paths(vina_base_dir))

            # Smina
            sminaRescoringResult = ocsmina.read_rescore_logs(ocsmina.get_rescore_log_paths(smina_base_dir))

            # PLANTS
            plantsRescoringResult = {}

            # For each scoring function
            for sf in plants_scoring_functions:
                # Read the rescoring results and save it in the dictionary
                plantsRescoringResult[sf] = ocplants.read_rescore_logs(f"{plants_parent_base_dir}/run_{sf}/ranking.csv")
            
            print({'vina': vinaRescoringResult, 'smina': sminaRescoringResult, 'plants': plantsRescoringResult})#, 'oddt': ocoddt.df_to_dict(df)})

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
