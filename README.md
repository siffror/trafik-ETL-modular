# 🚦 Trafik-ETL Modular

[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.x-FF4B4B.svg)](https://streamlit.io/)
[![SQLite](https://img.shields.io/badge/Database-SQLite-blue)](https://www.sqlite.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**Trafik-ETL Modular** is a complete ETL pipeline and interactive dashboard that collects road incidents from the **Swedish Transport Administration (Trafikverket) API**, stores them in a **SQLite database**, and visualizes them in a **Streamlit application**.  
The project is built in a modular way to ensure easy usage, maintenance, and future extension.

---

## 🌍 Demo

👉 [Open the Streamlit App](https://etl-trv.streamlit.app/?lang=en)

*(The dashboard is fully interactive – try filtering, exploring the map, and analyzing the charts directly online!)*

---

## ✨ Features
- 📡 **Data collection**: fetches ongoing and upcoming road incidents from Trafikverket’s API  
- 💾 **Storage**: automatically creates and updates a local SQLite database (`trafik.db`)  
- 📊 **Interactive dashboard** built with Streamlit:
  - Language support: English and Swedish  
  - Filters by status, county, date range, road number, and free text  
  - KPI metrics (Ongoing, Upcoming, Total)  
  - Clickable bar chart by county  
  - Map with points, heatmap, or combined mode  
  - Incident trend over time (per day)  
  - Distribution of incident types  
  - Table view of the latest incidents  

---

## 📦 Installation
```bash
git clone https://github.com/siffror/trafik-ETL-modular.git
cd trafik-ETL-modular
pip install -r requirements.txt
```

🚀 Usage
Step 1 – Run the ETL to fetch and store data

This will automatically create or update the trafik.db file with the latest incidents: python src/trv/load_sqlite.py

Step 2 – Start the Streamlit dashboard locally streamlit run src/app/streamlit_app.py

📂 Project Structure
```
.github/workflows/tv-etl.yml    # GitHub Actions workflow
src/
 ├── app/
 │    ├── __init__.py
 │    ├── etl_runner.py
 │    └── streamlit_app.py      # Streamlit dashboard
 ├── trv/
 │    ├── __init__.py
 │    ├── client.py             # API client for Trafikverket
 │    ├── config.py             # Settings & API keys
 │    ├── endpoints.py          # API queries
 │    ├── load_sqlite.py        # Load data into SQLite
 │    ├── transform.py          # Data transformations
 │    └── utils.py              # Helper functions
 ├── utils/
 │    ├── error_handler.py      # Error handling
 │    ├── notifier.py           # Notifications (e.g., webhook/Slack)
 │    └── __init__.py
 ├── cli.py                     # Command-line interface
 └── logger.py                  # Logging
.env
.gitignore
requirements.txt
structure.txt
trafik.db
```

📷 Dashboard Example




📜 License

MIT License – free to use, modify and share.


👨‍💻 Developed as part of the Advanced Python Programming course.
