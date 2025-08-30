
from datetime import date
import streamlit as st

def render_header(title: str):
    st.markdown(f"### {title}")
    st.divider()

def make_date_filters(default_months_back: int = 12):
    col1, col2 = st.columns(2)
    with col1:
        desde = st.date_input("Desde", value=date.today().replace(day=1))
    with col2:
        hasta = st.date_input("Hasta (inclusive)", value=date.today())
    return desde, hasta
