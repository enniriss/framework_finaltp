import os
import requests
import pandas as pd
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")
USERNAME = os.getenv("API_USER", "admin")
PASSWORD = os.getenv("API_PASS", "admin")

st.set_page_config(page_title="Datamart Dashboard", layout="wide")
st.title("Datamart Dashboard - Predictive Returns")

@st.cache_data(show_spinner=False)
def get_token():
    r = requests.post(f"{API_URL}/token", json={"username": USERNAME, "password": PASSWORD})
    r.raise_for_status()
    return r.json()["access_token"]

@st.cache_data(show_spinner=False)
def get_data(page=1, page_size=500):
    token = get_token()
    params = {"Authorization": f"Bearer {token}", "page": page, "page_size": page_size}
    r = requests.get(f"{API_URL}/datamarts/predictive_returns", params=params)
    r.raise_for_status()
    return r.json()

page_size = st.sidebar.slider("Page size", 50, 500, 200, 50)
page = st.sidebar.number_input("Page", min_value=1, value=1, step=1)

data = get_data(page=page, page_size=page_size)
df = pd.DataFrame(data["items"])

st.subheader("Aperçu")
st.dataframe(df, use_container_width=True)

if df.empty:
    st.warning("Aucune donnée.")
    st.stop()

# Graphique 1: Histogramme âge
st.subheader("1) Distribution de l'âge")
st.bar_chart(df["age"].value_counts().sort_index())

# Graphique 2: Revenu moyen par tranche d'âge
st.subheader("2) Revenu moyen par tranche d'âge")
df["age_bucket"] = (df["age"] // 10) * 10
st.line_chart(df.groupby("age_bucket")["income_level"].mean())

# Graphique 3: Return rate moyen par tranche d’âge
st.subheader("3) Taux de retour moyen par tranche d'âge")
st.line_chart(df.groupby("age_bucket")["return_rate"].mean())