"""
Read me page - Documentation
"""
import streamlit as st
from pathlib import Path


def render():
    """Render the Read me page content."""
    st.title("Read me")
    readme_path = Path(__file__).parent / "README.md"
    if readme_path.exists():
        st.markdown(readme_path.read_text(encoding="utf-8"))
    else:
        st.info("README.md not found.")
