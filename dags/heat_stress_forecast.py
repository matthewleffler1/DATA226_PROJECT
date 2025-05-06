from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import requests
import pickle

# CONFIG
API_KEY = Variable.get("weather_api_key")
CITY = "San Jose"
MODEL_PATH = "/opt/airflow/dags/models/heat_model.pkl"
HOURLY_TABLE = "GROUP6_DB.ANALYTICS.HEAT_STRESS_FORECAST" # Snowflake table for hourly data
DAILY_TABLE = "GROUP6_DB.ANALYTICS.HEAT_STRESS_DAILY_RISK" # Snowflake table for daily data

# Updated thresholds
def assign_risk(feelslike):
    if feelslike < 24:
        return "Low"
    elif feelslike < 32:
        return "Moderate"
    else:
        return "High"

with DAG(
    dag_id="heat_stress_forecast",
    start_date=datetime(2025, 5, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["forecast", "heat", "weather"],
) as dag:

    @task()
    def fetch_weather():
        url = f"http://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q={CITY}&days=14&aqi=no&alerts=no"
        res = requests.get(url)
        res.raise_for_status()
        raw = res.json()

        forecast_data = []
        for day in raw['forecast']['forecastday']:
            for hour in day['hour']:
                forecast_data.append({
                    'time': hour['time'],
                    'temp_c': hour['temp_c'],
                    'humidity': hour['humidity'],
                    'wind_kph': hour['wind_kph'],
                    'cloud': hour['cloud'],
                    'uv': hour['uv'],
                    'gust_kph': hour['gust_kph'],
                    'is_day': hour['is_day'],
                })

        return forecast_data

    @task()
    def predict_heat_risk(data):
        df = pd.DataFrame(data)

        with open(MODEL_PATH, 'rb') as f:
            model = pickle.load(f)

        # IMPORTANT: Match training feature order exactly
        features = ['temp_c', 'humidity', 'wind_kph', 'cloud', 'uv', 'gust_kph', 'is_day']
        df['predicted_feelslike_c'] = model.predict(df[features])
        df['risk_level'] = df['predicted_feelslike_c'].apply(assign_risk)

        return df[['time', 'predicted_feelslike_c', 'risk_level']].to_dict(orient='records')

    @task()
    def write_to_snowflake(records):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {HOURLY_TABLE} (
                time TIMESTAMP,
                predicted_feelslike_c FLOAT,
                risk_level STRING
            );
        """)
# Respect idempotency by deleting existing records
        for row in records: 
            cur.execute(f"""
                DELETE FROM {HOURLY_TABLE} WHERE time = TO_TIMESTAMP('{row['time']}')
            """)
            cur.execute(f"""
                INSERT INTO {HOURLY_TABLE}
                VALUES (TO_TIMESTAMP('{row['time']}'), {row['predicted_feelslike_c']}, '{row['risk_level']}')
            """)

        cur.execute("COMMIT;")
        cur.close()

    @task()
    def summarize_daily_risk(records):
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['time']).dt.date
        daily = df.groupby('date').agg({
            'predicted_feelslike_c': 'max',
            'risk_level': lambda x: sorted(x, key=lambda r: ['Low', 'Moderate', 'High'].index(r))[-1]
        }).reset_index()

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {DAILY_TABLE} (
                date DATE,
                max_predicted_feelslike_c FLOAT,
                highest_risk_level STRING
            );
        """)

        for row in daily.to_dict(orient='records'):
            cur.execute(f"""
                DELETE FROM {DAILY_TABLE} WHERE date = '{row['date']}'
            """)
            cur.execute(f"""
                INSERT INTO {DAILY_TABLE}
                VALUES ('{row['date']}', {row['predicted_feelslike_c']}, '{row['risk_level']}')
            """)

        cur.execute("COMMIT;")
        cur.close()

    # DAG flow
    weather_data = fetch_weather()
    predictions = predict_heat_risk(weather_data)
    write_to_snowflake(predictions)
    summarize_daily_risk(predictions)
