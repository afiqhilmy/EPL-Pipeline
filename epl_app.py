import os

import streamlit as st
import pandas as pd
import pymysql
import plotly.express as px

# 1. Page Configuration (Dark theme aesthetic)
st.set_page_config(
    page_title="EPL Data Engineering Dashboard",
    page_icon="⚽",
    layout="wide"
)

st.title("⚽ Premier League (EPL) ETL Analytics Dashboard")
st.markdown("Powered by Apache Airflow, PySpark, MySQL, and Streamlit.")

# 2. Load Data from MySQL
@st.cache_data(ttl=600)
def load_data():
    password = os.environ.get("MYSQL_PASSWORD")
    if not password:
        raise RuntimeError("Set the MYSQL_PASSWORD environment variable before starting the dashboard.")

    conn = pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "localhost"),
        user=os.environ.get("MYSQL_USER", "airflow_user"),
        password=password,
        database=os.environ.get("MYSQL_DATABASE", "football_db"),
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        def read_table(sql):
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return pd.DataFrame(cursor.fetchall())

        teams_df = read_table("SELECT * FROM epl_team_summary")
        overall_df = read_table("SELECT * FROM epl_overall_agg")
        matches_df = read_table("SELECT * FROM epl_matches")
    finally:
        conn.close()

    return teams_df, overall_df, matches_df

try:
    teams_df, overall_df, matches_df = load_data()
except (pymysql.MySQLError, RuntimeError) as exc:
    st.error(f"Could not load dashboard data from MySQL: {exc}")
    st.stop()

if teams_df.empty:
    st.warning("No team statistics are available yet. Run the EPL Airflow pipeline, then refresh this page.")
    st.stop()

# 3. Sidebar Filters
st.sidebar.header("Filter Options")
selected_season = st.sidebar.selectbox("Select Season", teams_df["season"].unique())

# Filter data based on selection
filtered_teams = teams_df[teams_df["season"] == selected_season]
filtered_matches = matches_df[matches_df["season"] == selected_season]

# 4. Top-Level Metrics (KPI Cards)
st.subheader(f"📊 Season Overview ({selected_season})")
col1, col2, col3 = st.columns(3)

season_overall = overall_df[overall_df["season"] == selected_season]
total_goals = season_overall["total_league_goals"].values[0] if not season_overall.empty else 0
avg_goals = season_overall["avg_goals_per_match"].values[0] if not season_overall.empty else 0

col1.metric("Total Matches Played", len(filtered_matches))
col2.metric("Total League Goals", int(total_goals))
col3.metric("Average Goals / Match", round(float(avg_goals), 2))

# 5. Visualizations using Plotly
st.markdown("---")
c1, c2 = st.columns(2)

with c1:
    st.subheader("🏆 League Standings (Points)")
    standings_sorted = filtered_teams.sort_values(by="points", ascending=False)
    fig_points = px.bar(
        standings_sorted, 
        x="team", 
        y="points", 
        color="points",
        title=f"Team Points - {selected_season}",
        text="points"
    )
    st.plotly_chart(fig_points, use_container_width=True)

with c2:
    st.subheader("⚽ Goal Difference vs Goals Scored")
    fig_gd = px.scatter(
        filtered_teams,
        x="total_goals_scored",
        y="goal_difference",
        text="team",
        size="points",
        color="wins",
        title="Team Performance Matrix"
    )
    fig_gd.update_traces(textposition='top center')
    st.plotly_chart(fig_gd, use_container_width=True)

# 6. Raw Data Table Viewer
st.markdown("---")
st.subheader("🔍 Detailed Team Statistics Table")
st.dataframe(filtered_teams.sort_values(by="points", ascending=False), use_container_width=True)
