"""Run the screening notebook's build+run+stop cells (skip the multi-config sweep).

Intended to be executed inside the in-cluster driver pod:
    cd /work && python3 run_nb_cells.py
"""
import json, os

os.chdir(os.path.dirname(os.path.abspath(__file__)))
nb = json.load(open("run_screening.ipynb", encoding="utf-8"))
code = ["".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"]
selected = [c for c in code if "for cfg_name" not in c]  # skip the sweep cell

ns = {}
for i, src in enumerate(selected):
    print(f"\n########## CELL {i} ##########", flush=True)
    exec(compile(src, f"<cell {i}>", "exec"), ns)
