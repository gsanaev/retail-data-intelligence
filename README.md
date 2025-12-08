# 📘 Retail Data Intelligence  
### *End-to-End Retail Analytics & Forecasting with Macro–Micro Data Integration*

---

## 🌍 Project Overview

**Retail Data Intelligence** is an end-to-end analytics and applied machine learning project that integrates:

- **Macro-level European retail indicators** (Eurostat API)  
- **Micro-level transactional sales data** (Online Retail II dataset)  
- **SQL data modeling in DuckDB**  
- **Python pipelines for ETL, feature engineering, clustering & forecasting**  
- **Interactive Power BI dashboards for business insights**

The project demonstrates how real retail companies combine **external market signals** with **internal sales data** to understand demand, pricing, seasonality, and category performance — and how those insights support forecasting and business decision-making.

---

## 🎯 Business Context

A European online retailer wants to understand:

- How **macro-economic retail trends** (consumer demand, price inflation, retail trade activity) influence their sales  
- Which **categories and products** drive performance over time  
- How to **forecast demand** for inventory planning and promotions  
- How to segment products using purchasing patterns  

To support strategic and operational decisions, this project builds a **Retail Data Intelligence System** that integrates macro and micro data into a unified analytical workflow.

---

## 📊 Data Sources

---

### **1. Macro Data — Eurostat API (Real Data)**  

Eurostat provides official European statistical indicators.  
This project retrieves data using the **SDMX JSON API**, including:

#### **Retail Trade Index (RTI)**  
**Dataset:** `STS_RT_M`  
- Measures retail activity (volume indices)  
- By country, sector, seasonal adjustment  
- Monthly frequency  
- Used to track demand cycles & economic shocks  

#### **Consumer Price Index (HICP)**  
**Dataset:** `prc_hicp_midx`  
- Price inflation across product categories  
- Monthly  
- Used to analyze pricing pressure and elasticity  

#### **Household Consumption Expenditure (HFCE)**  
**Dataset:** `nama_10_co3_p3`  
- Consumer spending trends  
- Used to understand consumption-level macro context  

**Key Variables Extracted:**  
- `geo` — country  
- `TIME_PERIOD` — month  
- `value` — index value  
- `unit` — type of index  
- `s_adj` — seasonal adjustment  
- `sts_activity` — retail sector code  

---

### **2. Micro Data — Online Retail II (Real Transaction Data)**

Source: UCI Machine Learning Repository / Kaggle  
A real dataset containing UK-based transactional sales from 2010–2011.

**Variables include:**
- `InvoiceNo` — transaction ID  
- `InvoiceDate` — timestamp  
- `CustomerID` — customer identifier  
- `StockCode` — product code  
- `Description` — product name  
- `Quantity` — units sold  
- `UnitPrice` — price per unit  
- `Country` — customer location  

This dataset is used to compute:
- Revenue & demand metrics  
- Customer activity profiles  
- Category-level performance  
- Product clustering  
- Forecasting targets  

---

## 🔗 Macro–Micro Integration

One of the unique aspects of this project is the integration of **external macroeconomic indicators** with **internal retail sales**.

### **Why integrate macro and micro data?**
- Understand whether internal trends follow macro retail cycles  
- Measure inflation-adjusted growth  
- Improve forecasting with macro features  
- Detect divergence between company performance and the broader market  

### **How the integration works**

1. **Micro data** (Online Retail II) → aggregated into **monthly KPIs**  
2. **Macro data** (Eurostat) → already monthly  
3. Datasets joined on:
   - `period` (year-month)
   - `geo` = `"UK"` for macro indicators  

Example integrated row:

| Month   | RTI (macro) | CPI (macro) | Revenue (micro) | Units Sold | Top Categories |
|---------|--------------|-------------|------------------|------------|----------------|
| 2010-01 | 98.2         | 104.5       | 320,000          | 42,000     | Gifts, Home    |

This enables analyses such as:
- Correlation between macro retail activity and internal sales  
- Impact of inflation on demand  
- Seasonal alignment of macro vs micro trends  
- Forecasting performance with economic features  

---

## 🏗️ Project Architecture

```scss
                    ┌────────────────────────┐
                    │       Eurostat API     │
                    │   (macro indicators)   │
                    └────────────┬───────────┘
                                 │ JSON (SDMX)
                                 ▼
                         Python ETL Pipeline
                                 │
                                 ▼
 ┌────────────┐  CSV  ┌───────────────────────┐
 │ Online     │ ───►  │ Micro ETL + Cleaning  │
 │ Retail II  │       └───────────────────────┘
 └────────────┘        │                        
                        ▼                        
                   DuckDB SQL Model             
           (facts, dimensions, metrics, joins)  
                        │               
                        ▼               
                Feature Engineering Layer    
         (seasonality, lag features, macro features)
                        │               
                        ▼               
           ML Models: Clustering + Forecasting
                        │
                        ▼
                Power BI Dashboard
```

---

## 🧠 Analytics & Machine Learning

### **1. KPI & Trend Analysis**
- Monthly revenue, demand, returns  
- Category & product performance  
- Macro indicators overlay  
- YoY & MoM growth  
- Seasonality decomposition  

---

### **2. Product Clustering**
Clustering using:
- Sales velocity  
- Seasonality patterns  
- Price characteristics  
- Revenue contribution  

Used for category management and product strategy.

---

### **3. Demand Forecasting**
Models used:
- **SARIMA** — classical time-series approach  
- **Prophet** — holiday-aware modeling  
- **XGBoost** — feature-based forecasting with macro indicators  

Outputs:
- Monthly forecast  
- Confidence intervals  
- Feature importance  
- Impact of macro variables on demand  

---

## 📊 Power BI Dashboard

Includes pages for:
- Retail KPIs  
- Macro vs Micro trend comparison  
- Category performance  
- Clustering visualizations  
- Forecast vs actuals  
- Seasonality insights  

Designed for executives, analysts, and planners.

---

## 🛠️ Tools & Technologies

**Languages & Libraries**
- Python (pandas, NumPy, scikit-learn, statsmodels)
- DuckDB SQL
- Power BI  
- uv environment management  

**Engineering**
- Modular OOP pipeline design  
- API integration  
- SQL data modeling  
- Automated transformations  
- Data quality checks  
- Caching + retry logic for stable API requests  


---

## 📂 Repository Structure (Planned)
```bash
retail-data-intelligence/
│
├── src/
│ ├── data/ # API clients, loaders
│ ├── pipelines/ # ETL orchestration
│ ├── features/ # feature engineering
│ ├── models/ # ML components
│ └── utils/ # helpers (logging, config)
│
├── sql/ # DuckDB schema & transformations
├── notebooks/ # EDA, feature exploration, forecasting
├── dashboard/ # Power BI report
├── tests/ # Testing suite
│
├── pyproject.toml
└── README.md
```


---

## ✨ Key Project Outcomes

By the end of the project, we deliver:

- A **reproducible data system** integrating macro and micro data  
- A DuckDB-based **analytical model**  
- A **macro–micro enriched dataset** ready for ML  
- **Product clusters** and **demand forecasts**  
- A business-ready **Power BI dashboard**  
- A polished, portfolio-quality project structure  

---

## 📬 Contact

**Golib Sanaev**  
Data Scientist (Applied ML & Business Analytics)  

GitHub: https://github.com/gsanaev  
LinkedIn: https://linkedin.com/in/golib-sanaev  