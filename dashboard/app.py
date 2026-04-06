from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st

# CONFIG
st.set_page_config(
    page_title="FX Insight Dashboard",
    layout="wide",
)

st.title("FX Insight Dashboard")
st.caption("Public demo using a static sample extracted from the pipeline output")


@st.cache_data
def load_data():
    demo_file = Path("demo_data/gold_sample.parquet")

    if not demo_file.exists():
        return pd.DataFrame()

    df = pd.read_parquet(demo_file)

    # Clean data
    df["amount_cad"] = pd.to_numeric(df["amount_cad"], errors="coerce").fillna(0)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    return df


df = load_data()

if df.empty:
    st.error("No demo data found in demo_data/gold_sample.parquet.")
    st.stop()

# SIDEBAR FILTERS
st.sidebar.header("Filters")

# Date filter
min_date = df["transaction_date"].min()
max_date = df["transaction_date"].max()

date_range = st.sidebar.date_input(
    "Date range",
    [min_date, max_date]
)

# Currency
currency_options = sorted(df["currency"].dropna().unique())
selected_currencies = st.sidebar.multiselect(
    "Currency",
    currency_options,
    default=currency_options
)

# Transaction type
type_options = sorted(df["transaction_type"].dropna().unique())
selected_types = st.sidebar.multiselect(
    "Transaction type",
    type_options,
    default=type_options
)

# Customers
customer_options = sorted(df["customer_id"].dropna().unique())
selected_customers = st.sidebar.multiselect(
    "Customers",
    customer_options,
    default=customer_options
)

# APPLY FILTERS
filtered_df = df.copy()

if len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = filtered_df[
        (filtered_df["transaction_date"] >= pd.to_datetime(start_date)) &
        (filtered_df["transaction_date"] <= pd.to_datetime(end_date))
    ]

filtered_df = filtered_df[
    filtered_df["currency"].isin(selected_currencies)
]

filtered_df = filtered_df[
    filtered_df["transaction_type"].isin(selected_types)
]

filtered_df = filtered_df[
    filtered_df["customer_id"].isin(selected_customers)
]

if filtered_df.empty:
    st.warning("No data after applying filters.")
    st.stop()

# KPIs
total_spent = filtered_df["amount_cad"].sum()
nb_transactions = len(filtered_df)
nb_clients = filtered_df["customer_id"].nunique()
avg_ticket = filtered_df["amount_cad"].mean()

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Spent (CAD)", f"{total_spent:,.2f}")
col2.metric("Transactions", f"{nb_transactions}")
col3.metric("Customers", f"{nb_clients}")
col4.metric("Average Ticket", f"{avg_ticket:,.2f}")

st.divider()

# TOP CUSTOMERS
top_clients = (
    filtered_df.groupby(["customer_id", "first_name", "last_name"])["amount_cad"]
    .sum()
    .reset_index()
    .sort_values("amount_cad", ascending=True)
    .tail(10)
)

top_clients["label"] = (
    top_clients["customer_id"].astype(str)
    + " - "
    + top_clients["first_name"].fillna("")
    + " "
    + top_clients["last_name"].fillna("")
)

fig_top = px.bar(
    top_clients,
    x="amount_cad",
    y="label",
    orientation="h",
    title="Top 10 Customers by Spending",
)

st.plotly_chart(fig_top, use_container_width=True)

# DAILY SPENDING
daily = (
    filtered_df.groupby("transaction_date")["amount_cad"]
    .sum()
    .reset_index()
)

fig_daily = px.line(
    daily,
    x="transaction_date",
    y="amount_cad",
    title="Daily Spending",
)

st.plotly_chart(fig_daily, use_container_width=True)

# BY TYPE & CURRENCY
col_left, col_right = st.columns(2)

# By type
by_type = (
    filtered_df.groupby("transaction_type")["amount_cad"]
    .sum()
    .reset_index()
)

fig_type = px.bar(
    by_type,
    x="transaction_type",
    y="amount_cad",
    title="Spending by Transaction Type",
)

# By currency
by_currency = (
    filtered_df.groupby("currency")["amount_cad"]
    .sum()
    .reset_index()
)

fig_currency = px.pie(
    by_currency,
    names="currency",
    values="amount_cad",
    title="Currency Distribution",
)

with col_left:
    st.plotly_chart(fig_type, use_container_width=True)

with col_right:
    st.plotly_chart(fig_currency, use_container_width=True)

st.divider()

# DATA TABLE
st.subheader("Data")

cols = [
    "transaction_id",
    "customer_id",
    "first_name",
    "last_name",
    "transaction_date",
    "amount",
    "currency",
    "transaction_type",
    "rate_to_cad",
    "amount_cad",
]

cols = [c for c in cols if c in filtered_df.columns]

st.dataframe(
    filtered_df[cols].sort_values("transaction_date", ascending=False),
    use_container_width=True,
    hide_index=True,
)