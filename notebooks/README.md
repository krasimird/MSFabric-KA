# GenDWH Documentation Engine — Fabric Notebooks

## Cell Convention

Each `.py` file uses a cell-marker convention so it can be converted to `.ipynb`:

```
# CELL <N> ── <Title> ───
```

Where `<N>` is the zero-based cell index and `<Title>` describes the cell.
Everything between two markers (or between a marker and end-of-file) becomes one notebook code cell.

## Notebook Structure

| Notebook | Purpose |
|---|---|
| `GenDWH_Documentation_LIB.py` | Shared library — config, helpers, all phase functions |
| `Phase_0_Discovery.py` | Discovers warehouse objects |
| `Phase_1_Profiling.py` | Profiles tables and views |
| `Phase_2_AI_Documentation.py` | Generates AI documentation via Claude |
| `Phase_3_Relationship_Mapping.py` | Maps relationships between objects |
| `Phase_4_Publishing.py` | Publishes artefacts to Lakehouse |
| `Phase_5_Versioning.py` | Creates versioned snapshots |

## Building .ipynb Files

From the repository root:

```bash
python scripts/py_to_ipynb.py
```

This reads every `.py` in `notebooks/` and writes Fabric-compatible `.ipynb` files into `build/`.

## Importing into Microsoft Fabric

1. Run the converter to produce `.ipynb` files in `build/`.
2. Open your Fabric workspace in the browser.
3. Click **New → Import notebook**.
4. Upload **GenDWH_Documentation_LIB.ipynb** first (the phase notebooks depend on it).
5. Upload the six phase notebooks.
6. Ensure the default Lakehouse (**GenDWH_Documentation_LH**) is attached to each notebook.
7. Run phase notebooks in order (0 → 5), or use the `run_all()` function in the LIB notebook.

