# DataVeritas

A Streamlit dashboard app for data orchestration, reconciliation, and exploration.

## Setup

1. Create a virtual environment (optional but recommended):
   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Run

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`.

## Features

- **Orchestrator** – Connect to databases, load Excel from Network folder, SharePoint, or upload. Run SQL queries.
- **Recon** – View your list, metrics, and charts.
- **DMC** – Add and manage items.
- **Data Explorer** – Generate sample data or upload CSV files.

## Config

Place `config.json` in the project folder with database credentials, network paths, and SharePoint settings. See `config_sample.json` for the expected format.
