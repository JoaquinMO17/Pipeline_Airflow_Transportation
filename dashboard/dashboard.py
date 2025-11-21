import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

st.set_page_config(page_title="Mexico Transport Dashboard", layout="wide")

st.title("📊 Mexican Public Transport Dashboard")
st.write("Using cleaned ETL data from Airflow → Postgres")

# Connect to Postgres inside the docker network
engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

df = pd.read_sql("SELECT * FROM transport_clean", engine)
states = pd.read_csv("/app/data/tc_entidad.csv")

df = df.merge(states, on="ID_ENTIDAD", how="left")

# ---------------------------------------
# KPI — Total Transport Activity
# ---------------------------------------
total_valor = df["VALOR"].sum()
st.metric("🚍 Total Transport Activity (sum VALOR)", f"{total_valor:,.0f}")

# -------- Chart 1: Transport usage by year --------
st.subheader("Transport Activity Over Time")
fig1, ax1 = plt.subplots()
df.groupby("ANIO")["VALOR"].sum().plot(ax=ax1)
ax1.set_ylabel("Total VALOR")
ax1.set_title("Transport activity by year")
st.pyplot(fig1)

# -------------------------------------------------------
# CHART 2: Growth Over Time for Top 3 Transport Modes
# -------------------------------------------------------
st.subheader("📈 Growth of Top Transport Modes Over Time")

# Determine top 3 based on total activity
top_transports = (
    df.groupby("TRANSPORTE")["VALOR"]
    .sum()
    .sort_values(ascending=False)
    .head(5)
    .index
)

df_top = df[df["TRANSPORTE"].isin(top_transports)]

fig1, ax1 = plt.subplots(figsize=(10, 5))

for t in top_transports:
    df_t = df_top[df_top["TRANSPORTE"] == t]
    yearly = df_t.groupby("ANIO")["VALOR"].sum()
    ax1.plot(yearly.index, yearly.values, marker="o", label=t)

ax1.set_title("Growth of Top Transport Modes Over Time")
ax1.set_xlabel("Year")
ax1.set_ylabel("Total Transport Activity (VALOR)")
ax1.legend()
ax1.grid(True)

st.pyplot(fig1)

# -------------------------------------------------------
# CHART 2: Horizontal bar chart — Top 10 States
# -------------------------------------------------------
st.subheader("🏙️ Top 10 States by Total Transport Activity")

df_states = (
    df.groupby("NOM_ENTIDAD")["VALOR"]
    .sum()
    .sort_values(ascending=True)  # ascending for horizontal bar
    .tail(10)
)

fig2, ax2 = plt.subplots(figsize=(10, 6))
df_states.plot(kind="barh", ax=ax2)

ax2.set_title("Top 10 States by Transport Activity")
ax2.set_xlabel("Total VALOR")
ax2.set_ylabel("State (NOM_ENTIDAD)")

st.pyplot(fig2)

# -------------------------------------------------------
# BONUS: Top Transport in Each State (Table)
# -------------------------------------------------------
st.subheader("🏆 Most Used Transport Mode per State")

df_state_transport = (
    df.groupby(["NOM_ENTIDAD", "TRANSPORTE"])["VALOR"]
    .sum()
    .reset_index()
)

# find top mode per state
top_state_modes = (
    df_state_transport.sort_values("VALOR", ascending=False)
    .groupby("NOM_ENTIDAD")
    .first()
    .reset_index()
)

st.dataframe(top_state_modes)
