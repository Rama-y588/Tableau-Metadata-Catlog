# Tableau Metadata Explorer

A production-ready Streamlit application for exploring Tableau metadata, workbooks, datasources, and analytics.

---

## Features
- Explore Tableau workbooks, datasources, and analytics in a modern UI
- All configuration via `config.json` (no hardcoded values)
- Robust logging to file and console
- Modular, maintainable codebase
- Ready for deployment

---

## Setup

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd <your-repo-directory>
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure the App
- Edit `src/config/config.json` to set paths, UI, cache, and logging options.
- Place your Tableau CSV exports in the directory specified in `config.json` (default: `data/csv_exports`).

### 4. Run the App
```bash
cd src
streamlit run app.py
```

---

## Project Structure
```
src/
  app.py                # Main Streamlit app
  config/
    config.json         # All app configuration
    config_manager.py   # Singleton config loader
  operations/           # Data loading, analytics, caching (implement as needed)
  ...
```

---

## Configuration
- All settings (UI, data, cache, logging) are in `src/config/config.json`.
- No hardcoded values in the codebase.

---

## Logging
- Logs are written to the file specified in `config.json` (default: `tableau_explorer.log`).
- All major actions and errors are logged.

---

## Customization
- Add your data loading, analytics, and caching logic in the `operations/` folder.
- Update UI and navigation in `config.json` and `app.py` as needed.

---

## Contributing
- Please use clear docstrings and comments.
- Follow PEP8 and modular design.

---

## License
MIT License 