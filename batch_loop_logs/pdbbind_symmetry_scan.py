import sys, time
from pathlib import Path
import rustworkx as rx
from rdkit import Chem, RDLogger
RDLogger.DisableLog("rdApp.*")
LIMIT = 100_000
import os
done = {tuple(l.split("\t")[:2]) for l in open(sys.argv[1])} if os.path.exists(sys.argv[1]) else set()
out = open(sys.argv[1], "a")
if not done: out.write("queue\treceptor\theavy_atoms\tisomorphisms\tseconds\tsmiles\n")
for tf in sys.argv[2:]:
    queue = Path(tf).stem.replace("pdbbind_", "").replace("_targets", "")
    for line in open(tf):
        p = Path(line.strip())
        if not p.parts: continue
        lig = p.parent; rec = lig.parts[-4]
        if (queue, rec) in done: continue
        smi_file = lig / "ligand.smi"
        try:
            smi = smi_file.read_text().split()[0]
            mol = Chem.MolFromSmiles(smi, sanitize=False)
            g = rx.PyGraph(); g.add_nodes_from([a.GetAtomicNum() for a in mol.GetAtoms() if a.GetAtomicNum() > 1])
            idx = {a.GetIdx(): i for i, a in enumerate(a for a in mol.GetAtoms() if a.GetAtomicNum() > 1)}
            g.add_edges_from_no_data([(idx[b.GetBeginAtomIdx()], idx[b.GetEndAtomIdx()]) for b in mol.GetBonds()
                                      if b.GetBeginAtomIdx() in idx and b.GetEndAtomIdx() in idx])
            t = time.time(); n = 0
            for _ in rx.vf2_mapping(g, g, node_matcher=lambda a, b: a == b):
                n += 1
                if n > LIMIT: break
            out.write(f"{queue}\t{rec}\t{len(idx)}\t{n}\t{time.time()-t:.2f}\t{smi}\n")
        except Exception as e:
            out.write(f"{queue}\t{rec}\t\tERROR {type(e).__name__}\t\t\n")
        out.flush()
