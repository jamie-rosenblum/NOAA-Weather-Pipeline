import streamlit as st
import duckdb

st.title("National Oceanic and Atmospheric Administration (NOAA) Data Dashboard")

con = duckdb.connect("weather.duckdb")

st.subheader("Monthly Temperature Trends")
df = con.execute("select month, avg_max_temp, avg_min_temp from fct_weather_monthly").df()
st.line_chart(df.set_index('month')[['avg_max_temp', 'avg_min_temp']])

st.subheader("Precipitation Trends")
prcp = con.execute("select month, total_precipitation from fct_weather_monthly").df()
st.bar_chart(prcp.set_index("month"))