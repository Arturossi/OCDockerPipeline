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
from OCDocker.Initialise import *

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
        #pdbbindPath = directory(os.path.join(pdbbind_archive, "refined-set")),
        sentinel = touch("tmp/untar_complete.sentinel")
    #conda:
    #    "../../envs/ocdocker.yaml"
    threads: 1
    run:
        import OCDocker.Toolbox.FilesFolders as ocff
        import OCDocker.Toolbox.MoleculeProcessing as ocmolproc
        import OCDocker.DB.baseDB as ocbdb
        
        # If the output directory does not exist or the user wants to overwrite it
        if not os.path.isdir(pdbbind_archive) or overwrite:
            # Untar the file
            ocff.untar(input.pdbbindTarGzPath, pdbbind_archive)

rule processPdbBind:
    """
    Processes the PDBbind dataset.

    Outputs:
        (file): Receptor file.
        (file): Ligand file.
    """
    input:
        #pdbbindPath = os.path.join(pdbbind_archive, "refined-set"), # Needed for the PDBBind database
        sentinel = "tmp/untar_complete.sentinel"
    output:
        receptor = pdbbind_archive + "/{target}/receptor.pdb",
        ligand = pdbbind_archive + "/{target}/compounds/ligands/ligand/ligand.smi",
    run:
        import OCDocker.Toolbox.Conversion as occonversion
        import OCDocker.Toolbox.FilesFolders as ocff
        import OCDocker.Toolbox.Printing as ocprint
        import OCDocker.Ligand as ocl

        # Check if there is a refined-set folder
        if os.path.isdir(os.path.join(pdbbind_archive, "refined-set")):
            # Parameterize the source and destination path
            destPath = os.path.join(pdbbind_archive, wildcards.target)
            sourcePath = os.path.join(pdbbind_archive, "refined-set", wildcards.target)
            
            # If the source path does not exist
            if not os.path.isdir(sourcePath):
                # Print an error
                return ocerror.Error.dir_not_exist(f"The folder '{sourcePath}' does not exist. Please, check if the dataset is correct and try again.", level = ocerror.ReportLevel.ERROR)
            
            # If the user wants to overwrite the folder
            if overwrite:
                # Remove the existing folder
                shutil.rmtree(destPath)
                # Create the destination folder
                _ = ocff.safe_create_dir(destPath)
                
            # Move the contents of sourcePath directly into destPath
            for item in os.listdir(sourcePath):
                print("Moving", item)
                shutil.move(os.path.join(sourcePath, item), destPath)

            # Create the compounds folder inside the protein folder
            _ = ocff.safe_create_dir(f"{destPath}/compounds")
            # Create the ligands folder inside the compounds folder (PDBbind only has one ligand per protein)
            _ = ocff.safe_create_dir(f"{destPath}/compounds/ligands")
            # Create the ligand folder inside the ligands folder (yes, generic name until I find a better one)
            _ = ocff.safe_create_dir(f"{destPath}/compounds/ligands/ligand")
            # Create the boxes folder inside the ligand folder
            _ = ocff.safe_create_dir(f"{destPath}/compounds/ligands/ligand/boxes")

            # Make a copy of the ligands to serve as reference and then move the ligand files to the ligands folder (mol2 and sdf)
            shutil.copy(os.path.join(destPath, wildcards.target + "_ligand.mol2"), f"{destPath}/reference_ligand.mol2")
            shutil.copy(os.path.join(destPath, wildcards.target + "_ligand.sdf"), f"{destPath}/reference_ligand.sdf")
            shutil.move(os.path.join(destPath, wildcards.target + "_ligand.mol2"), f"{destPath}/compounds/ligands/ligand/ligand.mol2")
            shutil.move(os.path.join(destPath, wildcards.target + "_ligand.sdf"), f"{destPath}/compounds/ligands/ligand/ligand.sdf")

            # Rename the protein file
            shutil.move(os.path.join(destPath, wildcards.target + "_protein.pdb"), f"{destPath}/receptor.pdb")

            # Remove all the unwanted files
            unwanteds = [("pocket", "pdb")]
            for unwanted in unwanteds:
                # If the file exists
                if os.path.isfile(f"{destPath}/{wildcards.target}_{unwanted[0]}.{unwanted[1]}"):
                    # Remove it
                    os.remove(f"{destPath}/{wildcards.target}_{unwanted[0]}.{unwanted[1]}")

            # Convert the ligand to smiles and save it in the ligand folder
            _ = occonversion.convertMols(f"{destPath}/reference_ligand.mol2", output.ligand)

            # Parameterize the reference ligand extensions in a list (in order of preference)
            ref_ligand_exts = ["mol2", "sdf", "pdb"]

            # Set the target centroid to None
            targetCentroid = None

            # For each extension in the list
            for ref_ligand_ext in ref_ligand_exts:
                # Parameterize the reference ligand path
                ref_ligand = os.path.join(destPath, f"reference_ligand.{ref_ligand_ext}")

                # Check if the reference ligand does not exist (extensions in order: pdb, mol2)
                if os.path.isfile(ref_ligand):
                    try:
                        try:
                            # Set the target centroid as the centroid of the ligand from the mol2 file
                            targetCentroid = ocl.get_centroid(ref_ligand, sanitize = True)
                        except:
                            # Set the target centroid as the centroid of the ligand from the mol2 file
                            targetCentroid = ocl.get_centroid(ref_ligand, sanitize = False)
                        
                        # Check if the target centroid is None
                        if not targetCentroid:
                            # Print a warning
                            ocprint.print_warning(message = f"WARNING: The centroid of the reference ligand in path '{path}' could not be calculated. The centroid of the receptor will be used instead.")
                            # Force the next iteration
                            continue

                        # Reference ligand found and read, break the loop
                        break
                    except Exception as e:
                        # Print the error
                        ocprint.print_error(f"Problems parsing the reference ligand file: {ref_ligand}. Error: {e}")
            
            # Check if the target centroid is still None
            if targetCentroid is None:
                return ocerror.Error.file_not_exist(f"Could not find the file '{' or '.join([os.path.join(destPath, f'reference_ligand.{ref_ligand_ext}') for ref_ligand_ext in ref_ligand_exts])}' for the molecule '{destPath}' or the provided files are not valid and a target centroid has not been provided. This molecule will not be processed.", level = ocerror.ReportLevel.ERROR)
            
            try:
                # Create the ligand object
                m = ocl.Ligand(output.ligand, wildcards.target, sanitize = True)
            except:
                # Create the ligand object (without sanitizing)
                m = ocl.Ligand(output.ligand, wildcards.target, sanitize = False)

            # Test if the Radius of Gyration is None
            if not m.RadiusOfGyration: # type: ignore
                # Append the molecule name to the list of molecules that will not be processed in the file
                with open(config["ignored_pdb_database_index"], "a") as f:
                    f.write(f"{wildcards.target}\n")
                # Print a warning
                return ocerror.Error.malformed_molecule("The Radius of Gyration of the ligand is None. The ligand will not be processed.", level = ocerror.ReportLevel.ERROR)

            # Create a box around the ligand
            m.create_box(centroid = targetCentroid, overwrite = overwrite)

            return ocerror.Error.ok()
        return ocerror.Error.dir_not_exist(f"The folder 'refined-set' does not exist in the PDBbind folder ('{pdbbind_archive}'). Please, check if the dataset is correct and try again.", level = ocerror.ReportLevel.ERROR)

