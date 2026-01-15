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
import argparse
import os

from glob import glob

# Disable auto-bootstrap so we can explicitly load the pipeline config.
os.environ.setdefault("OCDOCKER_NO_AUTO_BOOTSTRAP", "1")

import OCDocker.Error as ocerror
import OCDocker.Initialise as ocinit
from OCDocker.Config import get_config

import OCDP.preload as OCDPpre

# Bootstrap OCDocker using the pipeline config to populate the shared Config object.
pipeline_root = os.path.dirname(os.path.abspath(__file__))
config_file = os.getenv("OCDOCKER_CONFIG", os.path.join(pipeline_root, "OCDocker.cfg"))
log_level = str(config.get("log_level", "info")).lower()
log_level_map = {
    "debug": ocerror.ReportLevel.DEBUG,
    "info": ocerror.ReportLevel.INFO,
    "warning": ocerror.ReportLevel.WARNING,
    "error": ocerror.ReportLevel.ERROR,
    "none": ocerror.ReportLevel.NONE,
}
output_level = log_level_map.get(log_level, ocerror.ReportLevel.INFO)
bootstrap_ns = argparse.Namespace(
    multiprocess=config.get("cpu_cores", 1) > 1,
    update=False,
    config_file=config_file,
    output_level=output_level,
    overwrite=bool(config.get("overwrite", False)),
)
ocinit.bootstrap(bootstrap_ns)

oc_config = get_config()
ocdb_path = oc_config.paths.ocdb_path or ""
if not ocdb_path:
    raise RuntimeError("OCDocker ocdb path is not set. Update OCDocker.cfg (ocdb) and rerun.")
pdb_database_index = config["pdb_database_index"]

pdbbind_targets = OCDPpre.preload_PDBbind(pdb_database_index, config["ignored_pdb_database_index"])

dudez_database_index = config["dudez_database_index"]

dudez_targets = OCDPpre.preload_DUDEz(dudez_database_index, config["ignored_dudez_database_index"])


# Program imports
###############################################################################
include: "system/fileSystem.smk"
include: "system/database/pdbbind.smk"
include: "system/database/dudez.smk"
include: "docking/plants.smk"
include: "docking/vina.smk"


# Python definitions
###############################################################################

# Set some more arguments
cpu_cores = config["cpu_cores"]
available_cores = max(cpu_cores - 1, 1) # The main thread is not counted
multiprocess = 1                # 0: single process; 1: multiprocess
generate_report = False         # Generate a report at the end of the pipeline
zip_output = False              # Zip the output files
update = False                  # Update the pipeline
overwrite = bool(config.get("overwrite", False)) # Overwrite the output files
vina_scoring_functions = list(oc_config.vina.scoring_functions)
smina_scoring_functions = list(oc_config.smina.scoring_functions)
plants_scoring_functions = list(oc_config.plants.scoring_functions)

def find_mols(database, receptor, kind):
    """
    Find the molecules from the desired database.
    """

    mols = []

    # For each database
    for d in database:
        # For each receptor
        for r in receptor:
            # For each kind
            for k in kind:
                # Find its molecules
                mols += [os.path.join(ocdb_path, d, r, "compounds", k, t, "payload.pkl") for t in glob(os.path.join(ocdb_path, d, r, "compounds", k, "*"))]
    
    return mols


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
        expand(os.path.join(ocdb_path, "PDBbind", "{pdbbind_target}", "receptor.pdb"),
            pdbbind_target = pdbbind_targets,
        ),

rule db_dudez:
    """
    Set up the DUDEz database.
    """
    input:
        expand(os.path.join(ocdb_path, "DUDEz", "{dudez_target}", "receptor.pdb"),
            dudez_target = dudez_targets,
        ),

rule run_rescoring:
    """
    Run the rescoring of the poses from all docking software.
    """
    params:
        joblib_backend = config["joblib_backend"],
    input:
        plants_output = os.path.join(
            ocdb_path, "{database}", "{receptor}", "compounds", "{kind}",
            "{target}", "plantsFiles", "run", "prepared_ligand_entry_00001_conf_01.mol2"
        ),
        vina_output = os.path.join(
            ocdb_path, "{database}", "{receptor}", "compounds", "{kind}",
            "{target}", "vinaFiles", "{target}_split_1.pdbqt"
        ),
    output:
        touch(os.path.join(
            ocdb_path, "{database}", "{receptor}", "compounds",
            "{kind}", "{target}", "payload.pkl"
        )),
    threads: 1
    run:
        plants_output = input.plants_output
        vina_output = input.vina_output

        # Test if the output files exists
        if os.path.isfile(plants_output) and os.path.isfile(vina_output):
            # To fix Loky backend issue
            from joblib import parallel_backend

            # Import the libraries
            import OCDocker.Docking.PLANTS as ocplants
            import OCDocker.Docking.Smina as ocsmina
            import OCDocker.Docking.Vina as ocvina
            #import OCDocker.Rescoring.ODDT as ocoddt
            import OCDocker.Toolbox.Conversion as occonversion
            import OCDocker.Toolbox.FilesFolders as ocff
            import OCDocker.Toolbox.MoleculeProcessing as ocmolproc
            import OCDocker.Processing.Preprocessing.RmsdClustering as ocrmsdclust
            from OCDocker.DB.Models.Complexes import Complexes
            from OCDocker.DB.Models.Ligands import Ligands
            from OCDocker.DB.Models.Receptors import Receptors

            box_path = os.path.join(
                ocdb_path, wildcards.database, wildcards.receptor, "compounds",
                wildcards.kind, wildcards.target, "boxes", "box0.pdb"
            )
            bindingSiteCenter, bindingSiteRadius = ocplants.get_binding_site(box_path)

            # Get the base directory for the output files
            plants_base_dir = os.path.dirname(plants_output)
            vina_base_dir = os.path.dirname(vina_output)

            # Set the base path for the ligand
            basePath = os.path.dirname(vina_base_dir)

            # Make the smina base dir from the base path (it is similar to the vina base dir, but with sminaFiles instead of vinaFiles)
            smina_base_dir = os.path.join(basePath, "sminaFiles")

            # Create the base smina dir if it does not exists
            ocff.safe_create_dir(smina_base_dir)

            # Find the poses contained in this directory
            plants_poses = sorted(ocplants.get_docked_poses(plants_base_dir))
            vina_poses = sorted(ocvina.get_docked_poses(vina_base_dir))

            if not plants_poses or not vina_poses:
                print(f"No docking poses found for {wildcards.receptor}/{wildcards.target}.")
                return

            # Concatenate the poses lists from vina and plants into a single list
            poses_list = vina_poses + plants_poses

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
                    occonversion.convert_mols(medoid, outfile, overwrite=overwrite)
                    # Prepare the mol2 file for PLANTS
                    ocplants.run_prepare_ligand(outfile, preparedOutfile, overwrite=overwrite)
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
                    ocvina.run_prepare_ligand(medoid, preparedOutfile, overwrite=overwrite)
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
                ocplants.run_rescore(confFile, poseListPath, outPath, preparedReceptor_plants, sf, bindingSiteCenter[0], bindingSiteCenter[1], bindingSiteCenter[2], bindingSiteRadius, logFile = "", overwrite = True) # type: ignore

            #########################
            ## Run ODDT rescoring   #
            #########################

            # Use the threading backend context manager instead of the Loky backend
            #with parallel_backend(params.joblib_backend, n_jobs = available_cores):
            #    df = ocoddt.run_oddt(preparedReceptor, medoids, wildcards.target, f"{basePath}/oddt") # type: ignore

            ##############################
            ## Get the rescoring results #
            ##############################

            # Find the receptor in the database
            receptor = Receptors.find_first(wildcards.receptor)

            # Find the ligand in the database
            ligand = Ligands.find_first(wildcards.receptor + "_" + wildcards.target)

            # Create the payload
            payload = { "receptor": receptor, "ligand": ligand, "name": wildcards.receptor + "-" + wildcards.target }

            ## Vina
            #########
            vinaRescoringResult = ocvina.read_rescore_logs(ocvina.get_rescore_log_paths(vina_base_dir))

            # Get the vina SFs
            vinaSFs = list(vinaRescoringResult.keys())

            # Reverse each string in the list
            reversed_vinaSFs = [s[::-1] for s in vinaSFs]

            # Find the common prefix of reversed strings
            common_prefix = os.path.commonprefix(reversed_vinaSFs)

            # Reverse the common prefix to get the common suffix
            common_suffix = common_prefix[::-1]

            for key, item in vinaRescoringResult.items():
                # Remove the rescoring_ and the common suffix from the key
                newKey = "VINA_" + key.lower().replace("rescoring_", "").replace(common_suffix, "").upper()
                payload[newKey] = item
            
            ## Smina
            ##########
            sminaRescoringResult = ocsmina.read_rescore_logs(ocsmina.get_rescore_log_paths(smina_base_dir))

            # Get the smina SFs
            sminaSFs = list(sminaRescoringResult.keys())

            # Reverse each string in the list
            reversed_sminaSFs = [s[::-1] for s in sminaSFs]

            # Find the common prefix of reversed strings
            common_prefix = os.path.commonprefix(reversed_sminaSFs)

            # Reverse the common prefix to get the common suffix
            common_suffix = common_prefix[::-1]

            for key, item in sminaRescoringResult.items():
                # Remove the rescoring_ and the common suffix from the key
                newKey = "SMINA_" + key.lower().replace("rescoring_", "").replace(common_suffix, "").upper()
                payload[newKey] = item

            ## PLANTS
            ###########
            plantsRescoringResult = {}

            # For each scoring function
            for sf in plants_scoring_functions:
                # Read the rescoring results and save it in the dictionary
                plantsRescoringResult[sf] = ocplants.read_rescore_logs(f"{plants_parent_base_dir}/run_{sf}/ranking.csv")

            for key, item in plantsRescoringResult.items():
                newKey = "PLANTS_" + key.lower().replace("rescoring_", "").upper()
                payload[newKey] = item[1]["PLANTS_TOTAL_SCORE"]

            # Create the Complexes entry
            complexes = Complexes.insert(payload, ignorePresence = True)

            # Save the payload in a pickle file
            ocff.to_pickle(output[0], payload)
        else:
            print(f"One of the input files does not exists: '{plants_output}' or '{vina_output}'")
rule all:
    """
    Prepare the input files for PLANTS docking software.

    Usage:
        snakemake all --cores 20 --use-conda --conda-frontend mamba --keep-going --wms-monitor http://127.0.0.1:5000
    """
    input:
        #allkinds = expand(find_mols(["DUDEz"], dudez_targets, ["ligands", "decoys", "compounds"])),
        allkinds = expand(find_mols(["PDBbind"], pdbbind_targets, ["ligands", "decoys", "compounds"])),
        #allkinds = expand(
        #    "tmp/{database}!-!{receptor}!-!{kind}",
        #    database = ["PDBbind"], 
        #    receptor = pdbbind_targets, 
        #    kind = ["ligands", "decoys", "compounds"],
        #),
    run:
        print("All done!")
