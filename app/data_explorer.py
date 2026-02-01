"""
Data Explorer page - Sample data and CSV upload
"""
import streamlit as st
import pandas as pd
import numpy as np


def render():
    """Render the Data Explorer page content."""
    st.title("Data Explorer")
    st.markdown("Upload or generate sample data to explore")

    tab1, tab2 = st.tabs(["Sample Data", "Upload CSV"])

    with tab1:
        n_rows = st.slider("Number of rows", 5, 100, 20)
        sample_df = pd.DataFrame({
            "ID": range(1, n_rows + 1),
            "Value": np.random.randn(n_rows).cumsum(),
            "Category": np.random.choice(["A", "B", "C"], n_rows),
        })
        st.dataframe(sample_df, use_container_width=True, hide_index=True)
        st.line_chart(sample_df.set_index("ID")["Value"])

    with tab2:
        uploaded = st.file_uploader("Upload a CSV file", type="csv")
        if uploaded:
            df = pd.read_csv(uploaded)
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.success(f"Loaded {len(df)} rows")
