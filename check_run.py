# aaa

# sudo mysql -u ocdocker -p -e "USE tcpaqr; SELECT ligands.name FROM ligands JOIN complexes ON ligands.id = complexes.ligand_id INTO OUTFILE '/var/lib/mysql-files/ligands.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';" 
# @Kp3sRv9t@
# sudo mv /var/lib/mysql-files/ligands.txt ./
# sudo chmod 777 ligands.txt

from pprint import pprint

# Read the ligands.txt file and extract the ligand number
files = []
with open("ligands.txt") as f:
    for line in f:
        a = line.split('"')[1].split("_")[-1]
        files.append(a)

# Sort a in crescent order
files.sort()

# Find which numbers are missing from 0 to 99999
missing = []
for i in range(100000):
    if str(i) not in files:
        missing.append(i)

# Print the missing numbers
pprint(missing)