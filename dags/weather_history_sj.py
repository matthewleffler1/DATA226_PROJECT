from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests


@task
def extract_sanjose_history(days_back=14):
    api_key = Variable.get("weather_api_key")
    base_url = "http://api.weatherapi.com/v1/history.json"
    city = "San Jose"
    today = datetime.utcnow().date()

    raw_data = []

    for i in range(1, days_back + 1):
        date = today - timedelta(days=i)
        url = f"{base_url}?key={api_key}&q={city}&dt={date}"
        response = requests.get(url)
        response.raise_for_status()
        raw_data.append(response.json())

    return raw_data


@task
def transform_weather_history(raw_data):
    weather_data = []

    for day_data in raw_data:
        location = day_data['location']
        for hour in day_data['forecast']['forecastday'][0]['hour']:
            weather_data.append({
                'location_name': location['name'],
                'region': location['region'],
                'country': location['country'],
                'lat': location['lat'],
                'lon': location['lon'],
                'localtime': hour['time'],
                'temp_c': hour['temp_c'],
                'temp_f': hour['temp_f'],
                'feelslike_c': hour['feelslike_c'],
                'feelslike_f': hour['feelslike_f'],
                'is_day': hour['is_day'],
                'condition': hour['condition']['text'],
                'wind_kph': hour['wind_kph'],
                'wind_mph': hour['wind_mph'],
                'wind_dir': hour['wind_dir'],
                'wind_degree': hour['wind_degree'],
                'pressure_mb': hour['pressure_mb'],
                'pressure_in': hour['pressure_in'],
                'precip_mm': hour['precip_mm'],
                'precip_in': hour['precip_in'],
                'humidity': hour['humidity'],
                'cloud': hour['cloud'],
                'windchill_c': hour.get('windchill_c'),
                'heatindex_c': hour.get('heatindex_c'),
                'dewpoint_c': hour.get('dewpoint_c'),
                'vis_km': hour['vis_km'],
                'uv': hour['uv'],
                'gust_kph': hour['gust_kph'],
            })

    return weather_data


@task
def load_weather_history(data, target_table):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    cur = hook.get_conn().cursor()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                location_name STRING,
                region STRING,
                country STRING,
                lat FLOAT,
                lon FLOAT,
                "localtime" TIMESTAMP,
                temp_c FLOAT,
                temp_f FLOAT,
                feelslike_c FLOAT,
                feelslike_f FLOAT,
                is_day INT,
                condition STRING,
                wind_kph FLOAT,
                wind_mph FLOAT,
                wind_dir STRING,
                wind_degree INT,
                pressure_mb FLOAT,
                pressure_in FLOAT,
                precip_mm FLOAT,
                precip_in FLOAT,
                humidity INT,
                cloud INT,
                windchill_c FLOAT,
                heatindex_c FLOAT,
                dewpoint_c FLOAT,
                vis_km FLOAT,
                uv FLOAT,
                gust_kph FLOAT
            );
        """)

        for row in data:
            cur.execute(f"""
                INSERT INTO {target_table} (
                    location_name, region, country, lat, lon,
                    "localtime", temp_c, temp_f, feelslike_c, feelslike_f,
                    is_day, condition, wind_kph, wind_mph,
                    wind_dir, wind_degree, pressure_mb, pressure_in,
                    precip_mm, precip_in, humidity, cloud,
                    windchill_c, heatindex_c, dewpoint_c,
                    vis_km, uv, gust_kph
                ) VALUES (
                    '{row['location_name']}',
                    '{row['region']}',
                    '{row['country']}',
                    {row['lat']},
                    {row['lon']},
                    TO_TIMESTAMP('{row['localtime']}'),
                    {row['temp_c']},
                    {row['temp_f']},
                    {row['feelslike_c']},
                    {row['feelslike_f']},
                    {row['is_day']},
                    '{row['condition']}',
                    {row['wind_kph']},
                    {row['wind_mph']},
                    '{row['wind_dir']}',
                    {row['wind_degree']},
                    {row['pressure_mb']},
                    {row['pressure_in']},
                    {row['precip_mm']},
                    {row['precip_in']},
                    {row['humidity']},
                    {row['cloud']},
                    {row['windchill_c']},
                    {row['heatindex_c']},
                    {row['dewpoint_c']},
                    {row['vis_km']},
                    {row['uv']},
                    {row['gust_kph']}
                )
            """)

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Load failed:", e)
        raise e
    finally:
        cur.close()


with DAG(
    dag_id='weather_history_sanjose',
    start_date=datetime(2025, 4, 20),
    catchup=True,
    schedule_interval='@daily',
    tags=['weather', 'history', 'sanjose']
) as dag:
    target_table = 'GROUP6_DB.RAW.WEATHER_HISTORY_SJ_14DAYS'
    extracted = extract_sanjose_history()
    transformed = transform_weather_history(extracted)
    load_weather_history(transformed, target_table)
