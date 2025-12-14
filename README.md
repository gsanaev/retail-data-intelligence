# 📘 Retail Data Intelligence  
## *Macro-Economic Drivers of Retail Activity in the Euro Area (EA7)*

---

## 🔎 Project Overview

**Retail Data Intelligence** is a macro-economic analytics project that constructs a **clean, balanced monthly panel dataset** for core euro-area economies and uses it to analyze how retail activity responds to:

- inflation and household consumption,
- consumer confidence,
- global financial and monetary conditions.

The project focuses on **structural macroeconomic relationships**, rather than firm-level or transactional data. It is designed for **economic analysis, policy-relevant insights, and applied time-series research**.

---

## 🎯 Research Motivation

Retail activity represents a key transmission channel through which:

- inflation affects household purchasing power,
- confidence shapes consumption decisions,
- global monetary tightening spills into domestic demand.

This project addresses the central research question:

> **How do inflation, consumer confidence, and global financial conditions shape retail trade dynamics across core euro-area economies?**

Instead of focusing on a single country, the analysis uses a **balanced cross-country panel** to enable:

- comparison across economies,
- identification of common retail cycles,
- assessment of asymmetric responses to macroeconomic shocks.

---

## 🌍 Countries & Time Coverage

### Countries (EA7)
- Germany (DE)
- France (FR)
- Italy (IT)
- Spain (ES)
- Netherlands (NL)
- Austria (AT)
- Finland (FI)

### Time span
- January 2010 → December 2023  
- Monthly frequency  
- 168 observations per country  
- **1,176 observations total**

The result is a **fully balanced panel**, suitable for panel econometrics and macro-time-series analysis.

---

## 📊 Data Sources

### 1. Eurostat — Official European Statistics

Data are retrieved using the **Eurostat SDMX-CSV 2.1 API**, the officially supported bulk-download method.

**Indicators**

| Indicator | Description |
|---------|-------------|
| Retail Trade Index (RTI) | Volume index of retail sales |
| HICP (CP00) | Headline consumer price inflation |
| HFCE | Household final consumption expenditure |
| Consumer Confidence Indicator (CCI) | Survey-based demand sentiment |

All indicators are harmonized to **monthly frequency** and standardized across countries.

---

### 2. FRED — Global Financial Conditions

Global macro-financial indicators are sourced from **Federal Reserve Economic Data (FRED)** using direct CSV endpoints.

**Indicators**

| Variable | Description |
|--------|-------------|
| US CPI | Global inflation proxy |
| US Unemployment Rate | Global labor market conditions |
| Federal Funds Rate | Global monetary policy stance |
| US Industrial Production | Global business cycle proxy |

These variables capture **external financial and monetary shocks** relevant for euro-area retail dynamics.

---

## 🧱 Final Dataset

The final output is a **single, clean macro-panel dataset**:

```bash
data/processed/macro_panel.csv
```

**Variables**
```text
country                Euro-area country code
month                  YYYY-MM
rti_index              Retail Trade Index (volume)
hicp_index             Inflation (HICP, CP00)
hfce                   Household consumption expenditure
cci                    Consumer confidence indicator
us_cpi                 Global inflation proxy
us_unemployment         Global labor market proxy
us_fed_funds            Global monetary policy stance
us_industrial_prod      Global business cycle proxy
```

## 🔄 Data Pipeline

The project follows a **simple, transparent, and reproducible ETL workflow**:
```bash
1. Download Eurostat macro data
   uv run python -m src.download.eurostat_macro

2. Download global financial indicators (FRED)
   uv run python -m src.download.fred_financial

3. Build the balanced macro panel
   uv run python -m src.download.macro_etl
```

Each step is **idempotent**, documented, and environment-stable.


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
│   ├── 01_data_overview.ipynb
│   ├── 02_retail_dynamics.ipynb
│   ├── 03_inflation_consumption_link.ipynb
│   ├── 04_confidence_as_leading_indicator.ipynb
│   ├── 05_global_financial_spillovers.ipynb
│   ├── 06_shocks_and_structural_breaks.ipynb
│   └── 07_machine_learning_perspectives.ipynb
│
├── README.md
├── pyproject.toml
└── uv.lock
```

## 📈 Analytical Roadmap (Notebooks)

The analysis is organized into a **coherent narrative sequence**:

**1. Data overview & quality checks**
**2. Retail dynamics and growth patterns**
**3. Inflation–consumption–retail linkages**
**4. Consumer confidence as a leading indicator**
**5. Global financial spillovers**
**6. Shocks and structural breaks (COVID-19, inflation surge)**
**7. Machine learning perspectives (forecasting & regimes)**

All results are presented in **Jupyter notebooks** to ensure transparency and interpretability.


## 🧠 Methodological Philosophy

- Data discipline over data volume
- Balanced panels over ad-hoc samples
- Interpretability over black-box models
- Economic reasoning before prediction

The project deliberately avoids unnecessary complexity (e.g. dashboards, excessive APIs, or micro-data) to preserve **analytical clarity and economic coherence**.


## 🛠️ Tools & Technologies

- Python (pandas, NumPy, matplotlib, seaborn)
- Eurostat SDMX-CSV API
- FRED CSV API
- Jupyter Notebooks
- uv (environment management)


## ✨ Project Outcome

This project delivers:

- A reproducible, balanced macro-economic panel dataset
- A structured empirical analysis of retail dynamics in the euro area
- A portfolio-ready applied macro project
- A foundation suitable for econometric extensions or forecasting research

---

## 📬 Contact

**Golib Sanaev**  
Data Scientist (Applied ML & Business Analytics)  

GitHub: https://github.com/gsanaev  
LinkedIn: https://linkedin.com/in/golib-sanaev  