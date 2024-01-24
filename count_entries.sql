SELECT
    (SELECT COUNT(*) FROM complexes) AS N_Complexes,
    (SELECT COUNT(*) FROM receptors) AS N_Receptors,
    (SELECT COUNT(*) FROM ligands) AS N_Ligands,
    (SELECT COUNT(DISTINCT `name`) FROM complexes) as Unique_Complexes;