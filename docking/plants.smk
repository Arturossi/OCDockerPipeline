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

rule GenerateMutationImages:
    """
    Generate images of the mutated protein structures and color them according to the results of the program.

    Inputs:
        (file): Input pdb file.
        (file): Input list of mutations.
    Outputs:
        (file): Images of the protein mutations for multiple angles.
    """
    params:
        n_rotations = config['n_rotations'],
        generateGif = config['generateGif'],
        total_duration = config['total_duration'],
        protein_color = config['protein_color'],
        missing_data_color = config['missing_data_color'],
        multiple_mutation_color_min = config['multiple_mutation_color_min'],
        multiple_mutation_color_max = config['multiple_mutation_color_max'],
        location = config['location']
    input:
        "{outdir}/{filename}/{filename}.pdb",
        "{outdir}/{filename}/{program}/{filename}.csv",
    output:
        "{outdir}/{filename}/images/{program}/{filename}_{axis}.png"
    resources:
        pymol=1
    run:
        # Read the csv file with the program results according to the program
        if(wildcards.program == "SDM"):
            df = get_sdm_csv_for_imaging(input[1])
        elif(wildcards.program == "FoldX"):
            df = get_foldx_csv_for_imaging(input[1])
        elif(wildcards.program == "SIFT"):
            df = get_sift_csv_for_imaging(input[1])
        elif(wildcards.program == "ESM"):
            df = get_esm_csv_for_imaging(input[1])
        
        # Check if there is a wildcard for the suffix
        try:
            suffix = wildcards.suffix
        except:
            suffix = ""
        
        # If the df type is the type of None
        if type(df) == type(None):
            # TODO: Add log here
            # Patrameterize the resource path
            source_file = os.path.join(os.path.dirname(os.path.realpath(__name__)), "resources", "noimg.png")
            # Copy the noimg.png to the destiny
            mutafs.cp(source_file, output[0])
            # Finish this rule execution
            return None
        
        # Create the images
        generate_pdb_image(
                input[0],
                wildcards.filename,
                wildcards.program,
                df,
                wildcards.outdir + "/" + wildcards.filename + "/images",
                suffix = suffix,
                n_rotations = params.n_rotations,
                generateGif = params.generateGif,
                total_duration = params.total_duration,
                protein_color = params.protein_color,
                missing_data_color = params.missing_data_color,
                multiple_mutation_color_min = params.multiple_mutation_color_min,
                multiple_mutation_color_max = params.multiple_mutation_color_max,
                location = params.location
            )
