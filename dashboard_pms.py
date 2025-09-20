import streamlit as st
import pandas as pd
from fredapi import Fred
import psycopg2
from io import StringIO

st.title("Nonfarm Payrolls ETL Dashboard")

# --- Step 1: Extract ---
st.header("Step 1: Extracting data from FRED")
fred = Fred(api_key='c4bd4bf0eff060c3dfce5b96f4b0e44e')
series_id = 'PAYEMS'
df = fred.get_series(series_id)
jobs_df = pd.DataFrame(df, columns=['value']).reset_index().rename(columns={'index': 'date'})
st.write("Sample extracted data:")
st.dataframe(jobs_df.head())

# --- Step 2: Transform ---
st.header("Step 2: Transforming data (calculating MoM change)")
jobs_df['change_pct'] = (jobs_df['value'].pct_change() * 100).round(2)
jobs_df['change_abs'] = jobs_df['value'].diff()
jobs_df.dropna(inplace=True)
jobs_df = jobs_df.rename(columns={
    'value': 'total_nonfarm',
    'change_abs': 'mom_change_abs',
    'change_pct': 'mom_change_pct'
})
st.write("Transformed data sample:")
st.dataframe(jobs_df.head())

# --- Step 3: Load ---
st.header("Step 3: Loading transformed data into PostgreSQL")
DB_NAME = 'etl'
DB_USER = 'postgres'
DB_PASSWORD = 'Ayushi1@2&'
DB_HOST = 'localhost'
DB_PORT = '5432'

load_status = ""
try:
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nonfarm_payrolls (
            date DATE PRIMARY KEY,
            total_nonfarm DECIMAL,
            mom_change_abs DECIMAL,
            mom_change_pct DECIMAL
        );
    """)
    conn.commit()

    buffer = StringIO()
    jobs_df.to_csv(buffer, index=False, header=False, sep='\t')
    buffer.seek(0)
    cursor.copy_from(buffer, 'nonfarm_payrolls', columns=('date', 'total_nonfarm', 'mom_change_abs', 'mom_change_pct'), sep='\t')
    conn.commit()

    load_status = "Data successfully loaded!"
except Exception as e:
    load_status = f"An error occurred: {e}"
finally:
    if conn:
        cursor.close()
        conn.close()

st.success(load_status)
st.write("--- ETL Pipeline complete! ---")
