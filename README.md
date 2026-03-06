# GenDWH Knowledge Assistant

A system that **(A)** auto-documents a Microsoft Fabric Data Warehouse using AI, and **(B)** provides a RAG-powered Q&A web app for the team.

## Repository Structure

```
notebooks/   — Fabric Notebooks (Documentation Engine)
webapp/      — RAG Web App (Azure Static Web Apps + Claude)
scripts/     — Build & utility scripts
docs/        — Specification documents
```

## Notebooks — Documentation Engine

The `notebooks/` folder contains Python source files that are converted to Fabric-compatible `.ipynb` notebooks.

### Build Notebooks

```bash
python scripts/py_to_ipynb.py
```

Output goes to `build/`. See [notebooks/README.md](notebooks/README.md) for the cell-marker convention and Fabric import instructions.

## Web App — RAG Q&A

The `webapp/` folder contains a static web app with an Azure Function backend that proxies requests to Claude.

### Run Locally

```bash
cd webapp
npm install
npm run dev
```

Requires a `.env` file — see `webapp/.env.example` for required secrets.

### Deploy

Pushes to `main` that touch `webapp/` trigger automatic deployment via GitHub Actions.

## Documentation

Spec documents live in `docs/` and will be added manually.

