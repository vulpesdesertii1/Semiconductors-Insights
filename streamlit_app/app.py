import os
import pandas as pd
import altair as alt
import streamlit as st

st.set_page_config(page_title="Semiconductors Insights", layout="wide")

st.title("🧠 Semiconductors Insights — Blog interactivo")

# Ruta al CSV
DATA_PATH = os.environ.get("SI_DATA", "../scraper/data/scraped.csv")

@st.cache_data
def load_data(path: str):
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

df = load_data(DATA_PATH)

# Sidebar con filtros
with st.sidebar:
    st.subheader("Filtros")
    sources = sorted(df["source"].dropna().unique().tolist())
    pick_sources = st.multiselect("Fuente", sources, default=sources)
    q = st.text_input("Buscar en títulos/resúmenes", "")

# Filtrado
fdf = df.copy()
if pick_sources:
    fdf = fdf[fdf["source"].isin(pick_sources)]
if q:
    ql = q.lower()
    fdf = fdf[fdf["title"].str.lower().str.contains(ql) | fdf["summary"].str.lower().str.contains(ql)]

st.subheader("Resultados")
st.write(f"Se encontraron **{len(fdf)}** artículos")

# Tabla
st.dataframe(fdf[["source","date","title","summary","url"]].sort_values("date", ascending=False), use_container_width=True)

# Gráfico simple: número de artículos por día
if "date" in fdf:
    by_day = fdf.dropna(subset=["date"]).copy()
    by_day["day"] = by_day["date"].dt.date
    agg = by_day.groupby("day").size().reset_index(name="artículos")
    chart = alt.Chart(agg).mark_line().encode(x="day:T", y="artículos:Q", tooltip=["day:T","artículos:Q"])
    st.altair_chart(chart, use_container_width=True)
