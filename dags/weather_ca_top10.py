from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests


@task
def extract_weather_batch():
    api_key = Variable.get("weather_api_key")
    zip_codes = [
        "96101",  # Alturas
        "95501",  # Eureka
        "96001",  # Redding
        "96160",  # Truckee
        "95132",  # San Jose
        "94103",  # San Francisco
        "93901",  # Salinas
        "93702",  # Fresno
        "95370",  # Sonora
        "93401",  # San Luis Obispo
        "93301",  # Bakersfield
        "92401",  # San Bernardino
        "90001",  # Los Angeles
        "92243",  # El Centro
        "92101",  # San Diego
        "93526",  # Independence
        "92309",  # Baker
    ]
    url_template = "http://api.weatherapi.com/v1/current.json?key={key}&q={city_zip_code}&aqi=no"

    results = []
    for code in zip_codes:
        url = url_template.format(key=api_key, city_zip_code=code)
        response = requests.get(url)
        response.raise_for_status()
        results.append(response.json())

    return results


@task
def transform_batch(data_list):
    results = []
    for data in data_list:
        location = data['location']
        current = data['current']

        results.append({
            'location_name': location['name'],
            'region': location['region'],
            'country': location['country'],
            'lat': location['lat'],
            'lon': location['lon'],
            'localtime': location['localtime'],
            'temp_c': current['temp_c'],
            'temp_f': current['temp_f'],
            'feelslike_c': current['feelslike_c'],
            'feelslike_f': current['feelslike_f'],
            'is_day': current['is_day'],
            'condition': current['condition']['text'],
            'wind_kph': current['wind_kph'],
            'wind_mph': current['wind_mph'],
            'wind_dir': current['wind_dir'],
            'wind_degree': current['wind_degree'],
            'pressure_mb': current['pressure_mb'],
            'pressure_in': current['pressure_in'],
            'precip_mm': current['precip_mm'],
            'precip_in': current['precip_in'],
            'humidity': current['humidity'],
            'cloud': current['cloud'],
            'windchill_c': current.get('windchill_c'),
            'heatindex_c': current.get('heatindex_c'),
            'dewpoint_c': current.get('dewpoint_c'),
            'vis_km': current['vis_km'],
            'uv': current['uv'],
            'gust_kph': current['gust_kph'],
        })
    return results


@task
def load_current_weather(data_list, target_table):
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

        for row in data_list:
            cur.execute(f"""
                DELETE FROM {target_table}
                WHERE location_name = '{row['location_name']}'
            """)

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
    dag_id='weather_ca_top10',
    start_date=datetime(2025, 4, 25),
    catchup=False,
    schedule='@hourly',
    tags=['weather', 'current', 'california']
) as dag:

    target_table = 'GROUP6_DB.RAW.CALIFORNIA_ZIP_CODE_WEATHER_DATA'
    raw = extract_weather_batch()
    transformed = transform_batch(raw)
    load_current_weather(transformed, target_table)
