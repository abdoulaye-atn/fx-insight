# FX Insight — End-to-End Data Engineering Project

A complete data pipeline project that simulates financial transactions, enriches them with FX rates, stores data in a Bronze / Silver / Gold architecture, and exposes insights through an interactive dashboard.

## Overview

This project demonstrates a data engineering workflow:

- Generate synthetic customers and financial transactions
- Fetch real foreign exchange rates from an external API
- Build Bronze / Silver / Gold data lake layers
- Validate and quarantine invalid data
- Store datasets in partitioned Parquet format
- Upload data to AWS S3
- Query datasets with AWS Athena
- Visualize business metrics with a Streamlit dashboard

## Architecture

- FX API and synthetic data generation
- Bronze layer: raw CSV data
- Silver layer: cleaned and enriched Parquet data
- Gold layer: validated business-ready Parquet data
- AWS S3 as data lake storage
- AWS Glue / Athena compatibility
- Streamlit dashboard for exploration

## Project Structure

    fx-insight/
    ├── config/
    │   └── config.yaml
    ├── data/                  # generated pipeline outputs (ignored by git)
    │   ├── bronze/
    │   ├── silver/
    │   ├── gold/
    │   └── quarantine/
    ├── dashboard/
    │   └── app.py
    ├── demo_data/
    │   └── gold_sample.parquet
    ├── pipelines/
    │   └── pipeline.py
    ├── src/
    │   ├── ingestion/
    │   │   ├── fx_api_client.py
    │   │   └── generate_sample_data.py
    │   ├── processing/
    │   │   └── normalize_transactions.py
    │   ├── quality/
    │   │   └── data_checks.py
    │   └── utils/
    │       ├── config_loader.py
    │       ├── logger.py
    │       └── s3_client.py
    ├── generate_demo_data.py
    ├── requirements.txt
    └── README.md

## Data Pipeline Layers

### Bronze
Raw data ingested from:
- synthetic transactions
- synthetic customers
- real FX API rates

Stored as CSV files.

### Silver
Normalized and enriched datasets:
- transactions joined with FX rates
- transactions joined with customer data
- `amount_cad` calculated
- partitioned by date (`dt=YYYY-MM-DD`)

Stored as Parquet files.

### Gold
Validated business-ready datasets:
- invalid rows removed
- clean records preserved
- invalid records stored in quarantine

Stored as Parquet files.

## Features

- Synthetic customer generation with Faker
- Financial transaction simulation
- Real FX rate ingestion from Frankfurter API
- Currency normalization to CAD
- Data quality validation
- Bronze / Silver / Gold architecture
- Partitioned Parquet datasets
- AWS S3 integration
- AWS Athena querying
- Interactive Streamlit dashboard

## Demo Dashboard

This repository includes a public demo dataset so the dashboard can run immediately, without AWS and without running the pipeline first.

Run locally:

    pip install -r requirements.txt
    streamlit run dashboard/app.py

The dashboard uses:

    demo_data/gold_sample.parquet

## Note

The project also includes a complete local/AWS data pipeline used to generate Bronze / Silver / Gold datasets, but the public demo is the recommended way to explore the project quickly.

## Streamlit Dashboard Features

The dashboard includes:
- Date range filter
- Currency filter
- Customer filter
- Transaction type filter
- KPIs:
  - Total spent
  - Number of transactions
  - Number of customers
  - Average ticket
- Visuals:
  - Top customers by spending
  - Daily spending trend
  - Spending by transaction type
  - Currency distribution

## Tech Stack

- Python
- Pandas
- Faker
- Plotly
- Streamlit
- Parquet
- AWS S3
- AWS Athena
- AWS Glue

## Notes

- `data/` is ignored by Git because it contains generated pipeline outputs
- `demo_data/` is included so anyone can run the dashboard locally
- The AWS version and the public demo version are intentionally separated


## Author

Abdoulaye Niane  
Software Engineering Student — Data Engineering Focus