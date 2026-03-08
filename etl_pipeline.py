import pandas as pd
from sqlalchemy import create_engine
import sqlite3

# Extract
def extract(filepath):
    df = pd.read_csv(filepath)
    return df

# Transform
def transform(df):
    # Change columns to snake case
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')

    # Parse order date and ship date
    df['order_date'] = pd.to_datetime(df['order_date'], format='%d/%m/%Y')
    df['ship_date'] = pd.to_datetime(df['ship_date'], format='%d/%m/%Y')

    # Remove whitespace from all string columns
    df = df.apply(lambda col: col.str.strip() if col.dtype == 'object' else col)

    # Change postal code from float to string, remove the decimal
    df['postal_code'] = df['postal_code'].fillna(0).astype(int).astype(str)
    df['postal_code'] = df['postal_code'].replace('0', 'unknown')

    # Calculate
    df['days_to_ship'] = (df['ship_date'] - df['order_date']).dt.days
    df['order_month'] = df['order_date'].dt.month.astype(int)
    df['order_day'] = df['order_date'].dt.day.astype(int)

    return df

# Validate before loading
def validate(df):
    assert df['sales'].min() > 0, "Negative sales found"
    assert df['order_date'].isnull().sum() == 0, "Null dates found"
    assert len(df) > 9000, "Too many rows dropped"
    print('Validation passed')

# Load
def load(df):
    conn = sqlite3.connect('superstore.db')
    df.to_sql('orders', conn, if_exists='replace', index=False)
    conn.close()
    print("Data loaded into superstore.db")

def run_pipeline():
    df = extract('train.csv')
    df = transform(df)
    validate(df)
    load(df)
    print("Pipeline complete")

run_pipeline()