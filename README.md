![Python](https://img.shields.io/badge/Python-3.10-blue)
![PySpark](https://img.shields.io/badge/PySpark-Enabled-orange)
![Delta](https://img.shields.io/badge/Delta-Lake-green)

# Databricks Notebook Sales ETL

A notebook-based ETL project built with PySpark and Delta Lake.
This project simulates a real-world data pipeline using a step-by-step notebook workflow similar to Databricks.

---

## 🚀 Project Motivation

This project was built to practice:

- PySpark transformations
- Delta Lake usage
- Notebook-based ETL development
- Preparing for Azure Databricks environments

---

## ⚙️ Tech Stack

- Python
- PySpark
- Delta Lake

---

## 📊 Architecture

```text
        ┌──────────────┐
        │   Raw Data   │
        │  CSV Files   │
        └──────┬───────┘
               │
        ┌──────▼───────┐
        │   Transform  │
        │ Notebook ETL │
        └──────┬───────┘
               │
        ┌──────▼───────┐
        │    Delta     │
        │   Outputs    │
        └──────────────┘
```

---

## 🔄 Data Flow

1. Read raw CSV files
2. Clean and transform data
3. Filter completed orders
4. Calculate order totals
5. Join datasets
6. Generate business KPIs
7. Write outputs as Delta tables
8. Read back Delta for verification

---

## Notebook Version

This project includes both:

- `.py` version (Databricks-compatible)
- `.ipynb` version (for interactive exploration)

## 📁 Project Structure

```text
databricks-notebook-sales-project/
├── data/
│   ├── customers.csv
│   ├── orders.csv
│   ├── order_items.csv
│   └── products.csv
├── notebooks/
│   └── sales_etl_notebook.py
│   └── sales_etl_notebook.ipynb
├── README.md
└── requirements.txt
```

---

## 📌 Key Features

- PySpark DataFrame transformations
- Notebook-style ETL pipeline
- Aggregations and KPI calculations
- Delta Lake write & read
- Databricks-compatible notebook format

---

## ▶️ Quick Start

```bash
git clone https://github.com/YOUR_USERNAME/databricks-notebook-sales-project.git
cd databricks-notebook-sales-project

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python notebooks/sales_etl_notebook.py
```

---

## 📈 Example Output

### Sales Per Customer

| customer_id | name    | country | total_sales |
| ----------- | ------- | ------- | ----------- |
| 1           | Alice   | Sweden  | 520         |
| 3           | Charlie | Sweden  | 360         |

---

## 💾 Delta Output

The notebook writes outputs to:

```text
output/
├── sales_per_customer_delta/
├── sales_per_country_delta/
├── product_sales_delta/
└── order_summary_delta/
```

Each folder contains:

- `_delta_log/`
- Parquet data files

---

## 🧠 Use Case

This project simulates a retail analytics pipeline where:

- Raw transactional data is processed
- Business KPIs are calculated
- Data is stored in Delta Lake format

This design reflects real-world workflows in Databricks environments.

---

## 🔮 Future Improvements

- Run inside Databricks environment
- Integrate with Azure Data Lake
- Add incremental (merge) logic
- Convert to production pipeline
