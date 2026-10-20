import os
import json
import requests
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_date, lit, when, udf, 
    sum as spark_sum, avg as spark_avg, count as spark_count, regexp_replace
)
from pyspark.sql.types import ArrayType, IntegerType

# File paths for inter-task communication
RAW_JSON_2526 = "/tmp/epl_2025-26.json"
RAW_JSON_2627 = "/tmp/epl_2026-27.json"
TRANSFORMED_PARQUET = "/tmp/epl_transformed.parquet"
TEAM_AGG_PARQUET = "/tmp/epl_team_agg.parquet"
OVERALL_AGG_PARQUET = "/tmp/epl_overall_agg.parquet"
EXPORT_CSV_PATH = "/tmp/epl_matches_export.csv"

default_args = {
    'owner': 'Afiq Hilmy',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_task():
    """Extracts raw JSON files from openfootball and stages them locally."""
    seasons_urls = {
        "2025-26": "https://raw.githubusercontent.com/openfootball/football.json/master/2025-26/en.1.json",
        "2026-27": "https://raw.githubusercontent.com/openfootball/football.json/master/2026-27/en.1.json"
    }
    
    for season_name, url in seasons_urls.items():
        response = requests.get(url)
        response.raise_for_status()
        local_path = RAW_JSON_2526 if season_name == "2025-26" else RAW_JSON_2627
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(response.json(), f)
    print("Extraction completed successfully.")

def transform_task():
    """Cleans data and normalizes result strings."""
    spark = SparkSession.builder.appName("EPL_Transform").getOrCreate()
    
    seasons_files = [("2025-26", RAW_JSON_2526), ("2026-27", RAW_JSON_2627)]
    all_dfs = []
    
    for season_name, file_path in seasons_files:
        if os.path.exists(file_path):
            with open(file_path, encoding="utf-8") as season_file:
                season_data = json.load(season_file)
            if not season_data.get("matches"):
                print(f"No matches found for season {season_name}; skipping it.")
                continue

            df = spark.read.json(file_path, multiLine=True)
            matches_df = df.select(
                lit(season_name).alias("season"),
                explode(col("matches")).alias("match")
            )
            
            flat_df = matches_df.select(
                col("season"),
                col("match.round").alias("round_name"),
                to_date(col("match.date"), "yyyy-MM-dd").alias("match_date"),
                col("match.team1").alias("home_team"),
                col("match.team2").alias("away_team"),
                col("match.score").alias("raw_score")
            )
            all_dfs.append(flat_df)

    if not all_dfs:
        spark.stop()
        raise ValueError("No season files found for transformation.")

    master_df = all_dfs[0]
    for next_df in all_dfs[1:]:
        master_df = master_df.union(next_df)

    # UDF to parse mixed score formats
    @udf(returnType=ArrayType(IntegerType()))
    def parse_score(score_val):
        if score_val is None:
            return None
        if isinstance(score_val, str):
            try:
                score_val = json.loads(score_val)
            except json.JSONDecodeError:
                return None
        if isinstance(score_val, dict):
            ft = score_val.get('ft')
            if ft and len(ft) >= 2:
                score_val = ft
        if isinstance(score_val, (list, tuple)):
            if len(score_val) >= 2:
                try:
                    return [int(score_val[0]), int(score_val[1])]
                except (TypeError, ValueError):
                    return None
        return None

    scored_df = master_df.withColumn("parsed_score", parse_score(col("raw_score")))

    extracted_df = scored_df.select(
        col("season"),
        col("round_name"),
        col("match_date"),
        col("home_team"),
        col("away_team"),
        col("parsed_score")[0].alias("home_score"),
        col("parsed_score")[1].alias("away_score")
    )

    # Filter unplayed matches & handle missing values
    cleaned_df = extracted_df.filter(
        col("home_score").isNotNull() & col("away_score").isNotNull() &
        col("home_team").isNotNull() & col("away_team").isNotNull()
    )

    # Derive result and remove underscores (HOME_WIN -> HOME WIN)
    result_df = cleaned_df.withColumn(
        "raw_result",
        when(col("home_score") > col("away_score"), "HOME_WIN")
        .when(col("home_score") < col("away_score"), "AWAY_WIN")
        .otherwise("DRAW")
    ).withColumn(
        "result", regexp_replace(col("raw_result"), "_", " ")
    ).drop("raw_result")

    # Save clean Silver dataset
    result_df.write.mode("overwrite").parquet(TRANSFORMED_PARQUET)
    spark.stop()
    print("Transformation completed.")

def aggregate_team_stats_task():
    """Branch 1 & 3: Team-level metrics including goals against, goal difference, and points by season."""
    spark = SparkSession.builder.appName("EPL_Team_Agg").getOrCreate()
    df = spark.read.parquet(TRANSFORMED_PARQUET)

    # Home perspective
    home_perspective = df.select(
        col("season"),
        col("home_team").alias("team"),
        col("home_score").alias("goals_for"),
        col("away_score").alias("goals_against"),
        when(col("home_score") > col("away_score"), 1).otherwise(0).alias("win"),
        when(col("home_score") == col("away_score"), 1).otherwise(0).alias("draw"),
        when(col("home_score") < col("away_score"), 1).otherwise(0).alias("loss")
    )

    # Away perspective
    away_perspective = df.select(
        col("season"),
        col("away_team").alias("team"),
        col("away_score").alias("goals_for"),
        col("home_score").alias("goals_against"),
        when(col("away_score") > col("home_score"), 1).otherwise(0).alias("win"),
        when(col("away_score") == col("home_score"), 1).otherwise(0).alias("draw"),
        when(col("away_score") < col("home_score"), 1).otherwise(0).alias("loss")
    )

    team_union = home_perspective.union(away_perspective)

    # Aggregate base sums and counts
    team_summary_raw = team_union.groupBy("season", "team").agg(
        spark_count("team").alias("matches_played"),
        spark_sum("goals_for").alias("total_goals_scored"),
        spark_sum("goals_against").alias("total_goals_against"),
        spark_avg("goals_for").alias("avg_goals_scored"),
        spark_sum("win").alias("wins"),
        spark_sum("draw").alias("draws"),
        spark_sum("loss").alias("losses") 
    )

    # Derive Goal Difference and Points (Wins * 3 + Draws * 1)
    team_summary = team_summary_raw.withColumn(
        "goal_difference", col("total_goals_scored") - col("total_goals_against")
    ).withColumn(
        "points", (col("wins") * 3) + (col("draws") * 1)
    ).select(
        "season", "team", "matches_played", "total_goals_scored", 
        "total_goals_against", "goal_difference", "avg_goals_scored", 
        "wins", "draws", "losses", "points"
    )

    team_summary.write.mode("overwrite").parquet(TEAM_AGG_PARQUET)
    spark.stop()
    print("Team aggregations with points and goal difference completed.")

def aggregate_overall_stats_task():
    """Branch 2: Overall league total and average goals by season."""
    spark = SparkSession.builder.appName("EPL_Overall_Agg").getOrCreate()
    df = spark.read.parquet(TRANSFORMED_PARQUET)

    total_goals_df = df.withColumn("total_match_goals", col("home_score") + col("away_score"))
    
    overall_summary = total_goals_df.groupBy("season").agg(
        spark_sum("total_match_goals").alias("total_league_goals"),
        spark_avg("total_match_goals").alias("avg_goals_per_match")
    )

    overall_summary.write.mode("overwrite").parquet(OVERALL_AGG_PARQUET)
    spark.stop()
    print("Overall aggregations completed.")

def load_to_mysql_task():
    """Loads match records and all aggregated metrics into MySQL using batch executemany and exports matches to CSV."""
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    
    # Read data from parquet and convert to list of tuples
    matches_pd = pd.read_parquet(TRANSFORMED_PARQUET)
    matches_rows = [tuple(x) for x in matches_pd.to_numpy()]

    team_pd = pd.read_parquet(TEAM_AGG_PARQUET)
    team_rows = [tuple(x) for x in team_pd.to_numpy()]

    overall_pd = pd.read_parquet(OVERALL_AGG_PARQUET)
    overall_rows = [tuple(x) for x in overall_pd.to_numpy()]

    match_upsert_sql = """
    INSERT INTO epl_matches 
    (season, round_name, match_date, home_team, away_team, home_score, away_score, result)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        round_name = VALUES(round_name),
        home_score = VALUES(home_score),
        away_score = VALUES(away_score),
        result = VALUES(result);
    """

    team_upsert_sql = """
    INSERT INTO epl_team_summary
    (season, team, matches_played, total_goals_scored, total_goals_against, goal_difference, avg_goals_scored, wins, draws, losses, points)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        matches_played = VALUES(matches_played),
        total_goals_scored = VALUES(total_goals_scored),
        total_goals_against = VALUES(total_goals_against),
        goal_difference = VALUES(goal_difference),
        avg_goals_scored = VALUES(avg_goals_scored),
        wins = VALUES(wins),
        draws = VALUES(draws),
        losses = VALUES(losses),
        points = VALUES(points);
    """

    overall_upsert_sql = """
    INSERT INTO epl_overall_agg 
    (season, total_league_goals, avg_goals_per_match)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        total_league_goals = VALUES(total_league_goals),
        avg_goals_per_match = VALUES(avg_goals_per_match);
    """

    # Use native connection and cursor with batch executemany
    with mysql_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(match_upsert_sql, matches_rows)
            cursor.executemany(team_upsert_sql, team_rows)
            cursor.executemany(overall_upsert_sql, overall_rows)
        conn.commit()

    # Export Match Records to CSV
    matches_pd.to_csv(EXPORT_CSV_PATH, index=False)
    print(f"Successfully loaded all data layers to MySQL via batch executemany and exported CSV to {EXPORT_CSV_PATH}.")

with DAG(
    'epl_etl_pipeline',
    default_args=default_args,
    description='EPL Match ETL Pipeline with Spark Aggregations and MySQL Load',
    schedule ='0 6 * * 1',  # Runs every Monday at 6:00 AM
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract_match_data',
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id='transform_match_data',
        python_callable=transform_task
    )

    # Parallel Aggregation Branches matching your architecture diagram
    aggregate_team_goals_and_wins = PythonOperator(
        task_id='aggregate_team_stats',
        python_callable=aggregate_team_stats_task
    )

    aggregate_overall_goals = PythonOperator(
        task_id='aggregate_overall_stats',
        python_callable=aggregate_overall_stats_task
    )

    load_and_export = PythonOperator(
        task_id='load_to_mysql_and_export_csv',
        python_callable=load_to_mysql_task
    )

    # DAG Dependency Wiring
    extract >> transform
    transform >> aggregate_team_goals_and_wins
    transform >> aggregate_overall_goals
    [aggregate_team_goals_and_wins, aggregate_overall_goals] >> load_and_export