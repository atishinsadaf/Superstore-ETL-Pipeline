# Superstore Sales ETL Pipeline & Analysis

## Overview
A data engineering project that builds an ETL pipeline to clean and transform raw retail sales data, loads it into a database, and analyzes it using SQL.

## Tools & Technologies
- **Python** — ETL pipeline (pandas, SQLAlchemy, SQLite)
- **SQL** — Business analysis queries
- **DBeaver** — SQL querying and result visualization
- **Libraries** — pandas, sqlalchemy, sqlite3
            
## ETL Pipeline
- **Extract** — Loads raw CSV data into a pandas DataFrame
- **Transform** — Cleans column names, parses dates, fixes data types, removes whitespace, adds calculated columns (days_to_ship, order_month, order_day)
- **Validate** — Asserts data quality rules before loading (no negative sales, no null dates, row count check)
- **Load** — Writes cleaned data into a SQLite database

## Findings
- The **West region** generated the highest revenue ($710K) while the **South** was the lowest at $389K
- **Canon imageCLASS 2200 Copier** was the top-selling product at $61,600 — nearly double the second place product
- **Technology** was the highest grossing category at $827K, followed closely by Furniture ($728K) and Office Supplies ($705K)
- **Consumer segment** drove 51% of total revenue at $1.14M — more than Corporate and Home Office combined
- **Standard Class** shipping averages 5 days while **Same Day** averages 0 days

## How to Run
1. Clone the repo
2. Install dependencies:
pip install pandas sqlalchemy
3. Place train.csv in the project root
4. Run the pipeline:
python etl_pipeline.py
5. Open superstore.db in DBeaver to run analysis queries
