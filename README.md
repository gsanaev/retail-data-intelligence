# 📘 Retail Data Intelligence
## *Macro-Economic Drivers of Retail Activity in the Euro Area (EA7)*

## 🔎 Project Overview

**Retail Data Intelligence** is a macro-economic analytics project that builds a **clean, balanced monthly panel dataset** for core euro-area economies and uses it to study the relationship between:
- Retail activity
- Inflation and consumption
- Consumer confidence
- Global financial conditions

The project focuses on **structural macro relationships** rather than firm-level or transactional data, making it suitable for **economic analysis, policy-relevant insights, and time-series research**.

## 🎯 Research Motivation

Retail activity is a key transmission channel through which:
- inflation affects households,
- confidence drives demand,
- global monetary tightening spills into domestic economies.

This project asks:

> **How do inflation, confidence, and global financial conditions shape retail trade dynamics across core euro-area countries?**

Rather than analyzing a single country, the project uses a **balanced cross-country panel** to enable:
- comparison across economies,
- identification of common cycles,
- analysis of asymmetric responses to shocks.

## 🌍 Countries & Time Coverage

### Countries (EA7):
- Germany (DE)
- France (FR)
- Italy (IT)
- Spain (ES)
- Netherlands (NL)
- Austria (AT)
- Finland (FI)

### Time span:
- January 2010 → December 2023
- Monthly frequency
- 168 observations per country
- 1,176 observations total

The result is a **fully balanced panel,** suitable for econometric and time-series analysis.

## 📊 Data Sources
### 1. Eurostat (Official European Statistics)

Data are downloaded using the **Eurostat SDMX-CSV 2.1 API,** the officially supported bulk access method.

**Indicators:**

| **Indicator** | **Description** |
| Retail Trade Index (RTI) | Volume index of retail sales |
| HICP (CP00) | Headline consumer price inflation |
| HFCE | Household final consumption expenditure |
| Consumer Confidence Indicator (CCI) | Survey-based demand sentiment |

All series are transformed to **monthly frequency** and harmonized across countries.

### 2. FRED (Global Financial Conditions)

Global macro-financial indicators are obtained from the **Federal Reserve Economic Data (FRED)** using direct CSV endpoints.

**Indicators:**

| **Variable** | **Description** |
| US CPI | Global inflation proxy |
| US Unemployment Rate | Global labor market conditions |
| Federal Funds Rate | Global monetary stance |
| US Industrial Production | Global business cycle proxy |

These variables capture **external shocks and global cycles** that affect euro-area economies.

## 🧱 Final Dataset Structure

The final output is a **single clean panel dataset:**
```bash
data/processed/macro_panel.csv
```

### Variables
```text
country                   # Euro-area country code
month                     # YYYY-MM
rti_index                 # Retail Trade Index (volume)
hicp_index                # Inflation (HICP, CP00)
hfce                      # Household consumption expenditure
cci                       # Consumer confidence indicator
us_cpi                    # Global inflation proxy
us_unemployment           # Global labor market proxy
us_fed_funds              # Global monetary policy stance
us_industrial_prod        # Global business cycle proxy
```

## 🔄 Data Pipeline

The project follows a **simple, transparent ETL workflow:**
```bash
1. Download Eurostat macro data
   uv run python -m src.download.eurostat_macro

2. Download global financial indicators (FRED)
   uv run python -m src.download.fred_financial

3. Build the macro panel dataset
   uv run python -m src.download.macro_etl
```

Each step is **idempotent**, reproducible, and documented.

## 📁 Repository Structure (Final)
```bash
retail-data-intelligence/
│
├── data/
│   ├── raw/                  # Raw Eurostat & FRED data
│   └── processed/
│       └── macro_panel.csv   # Final balanced panel
│
├── src/
│   └── download/
│       ├── eurostat_macro.py
│       ├── fred_financial.py
│       └── macro_etl.py
│
├── notebooks/
│   └── 01-notebook.ipynb     # Exploratory analysis
│
├── README.md
├── pyproject.toml
└── uv.lock
```

## 📈 Analytical Direction

The dataset is designed to support:
- Cross-country retail cycle comparison
- Inflation–retail dynamics
- Confidence as a leading indicator
- Spillovers from global monetary tightening
- Structural break analysis (COVID-19, 2022 inflation shock)

All analysis is conducted in **Jupyter notebooks** to ensure transparency and interpretability.

## 🧠 Methodological Philosophy

- Data discipline over data volume
- Balanced panels over ad-hoc samples
- Interpretability over black-box models
- Economic reasoning before forecasting

The project deliberately avoids unnecessary complexity (e.g. excessive APIs, dashboards, or micro-data) to maintain clarity and analytical focus.

## 🛠️ Tools & Technologies

- Python (pandas, NumPy)
- Eurostat SDMX-CSV API
- FRED CSV API
- Jupyter Notebooks
- uv for environment management

## ✨ Project Outcome

By the end of the pipeline, the project delivers:
- A reproducible macro-economic panel dataset
- A clean foundation for time-series and panel analysis
- A portfolio-ready applied macro project
- A structure suitable for academic extensions or forecasting work

---

## 📬 Contact

**Golib Sanaev**  
Data Scientist (Applied ML & Business Analytics)  

GitHub: https://github.com/gsanaev  
LinkedIn: https://linkedin.com/in/golib-sanaev  