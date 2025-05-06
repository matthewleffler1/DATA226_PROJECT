from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import requests

# CONFIG
API_KEY = Variable.get("weather_api_key")
CITY = "San Jose"
HOURLY_TABLE = "GROUP6_DB.ANALYTICS.PREDICTED_SPORTS_HOURS"
DAILY_TABLE = "GROUP6_DB.ANALYTICS.SPORTS_DAILY_SUMMARY"

def is_sports_ok(row):
    return (
        15 <= row['temp_c'] <= 28 and
        row['humidity'] < 80 and
        row['wind_kph'] < 15 and
        row['cloud'] < 70 and
        row['uv'] < 8 and
        row['vis_km'] >= 8 and
        row['is_day'] == 1
    )

with DAG(
    dag_id="sports_forecast_prediction",
    start_date=datetime(2025, 5, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["forecast", "sports", "weather"],
) as dag:

    @task()
    def fetch_forecast():
        url = f"http://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q={CITY}&days=14&aqi=no&alerts=no"
        res = requests.get(url)
        res.raise_for_status()
        raw = res.json()

        forecast_data = []
        for day in raw['forecast']['forecastday']:
            for hour in day['hour']:
                forecast_data.append({
                    'forecast_time': hour['time'],
                    'temp_c': hour['temp_c'],
                    'humidity': hour['humidity'],
                    'wind_kph': hour['wind_kph'],
                    'cloud': hour['cloud'],
                    'uv': hour['uv'],
                    'vis_km': hour['vis_km'],
                    'is_day': hour['is_day']
                })

        return forecast_data

    @task()
    def evaluate_and_store(records):
        df = pd.DataFrame(records)

        df['is_sports_ok'] = df.apply(is_sports_ok, axis=1).astype(int)
        df['sports_status'] = df['is_sports_ok'].apply(lambda x: 'Yes' if x == 1 else 'No')
        df['forecast_time'] = pd.to_datetime(df['forecast_time'])

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {HOURLY_TABLE} (
                forecast_time TIMESTAMP PRIMARY KEY,
                is_sports_ok INTEGER,
                sports_status STRING
            );
        """)

        times = df['forecast_time'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
        time_strs = ",".join([f"TO_TIMESTAMP('{t}')" for t in times])

        if time_strs:
            cur.execute(f"""
                DELETE FROM {HOURLY_TABLE}
                WHERE forecast_time IN ({time_strs})
            """)

        insert_values = ",".join([
            f"(TO_TIMESTAMP('{row['forecast_time']}'), {row['is_sports_ok']}, '{row['sports_status']}')"
            for _, row in df.iterrows()
        ])

        if insert_values:
            cur.execute(f"""
                INSERT INTO {HOURLY_TABLE} (forecast_time, is_sports_ok, sports_status)
                VALUES {insert_values}
            """)

        cur.execute("COMMIT;")
        cur.close()

        # Convert forecast_time back to string for JSON serialization
        df['forecast_time'] = df['forecast_time'].astype(str)
        return df.to_dict(orient="records")

    @task()
    def summarize_sports_days(records):
        df = pd.DataFrame(records)
        df['forecast_time'] = pd.to_datetime(df['forecast_time'])
        df['date'] = df['forecast_time'].dt.date

        summary = df.groupby('date')['is_sports_ok'].sum().reset_index()
        summary.rename(columns={'is_sports_ok': 'ok_hours'}, inplace=True)

        max_ok = summary['ok_hours'].max()
        summary['best_day_flag'] = summary['ok_hours'].apply(lambda x: 'Yes' if x == max_ok else 'No')

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cur = conn.cursor()

# Respect idempotency by deleting existing records
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {DAILY_TABLE} (
                date DATE PRIMARY KEY,
                ok_hours INTEGER,
                best_day_flag STRING
            );
        """)

        for _, row in summary.iterrows():
            cur.execute(f"""
                DELETE FROM {DAILY_TABLE} WHERE date = '{row['date']}'
            """)
            cur.execute(f"""
                INSERT INTO {DAILY_TABLE}
                VALUES ('{row['date']}', {int(row['ok_hours'])}, '{row['best_day_flag']}')
            """)

        cur.execute("COMMIT;")
        cur.close()

    forecast = fetch_forecast()
    evaluated = evaluate_and_store(forecast)
    summarize_sports_days(evaluated)
