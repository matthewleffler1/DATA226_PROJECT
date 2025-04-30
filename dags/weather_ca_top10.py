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
        "96101", "95501", "96001", "96160", "95132", "94103", "93901",
        "93702", "95370", "93401", "93301", "92401", "90001", "92243",
        "92101", "93526", "92309"
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
            'gust_kph': current['gust_kph']
        })
    return results


@task
def load_current_weather(data_list, target_table):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    cur = hook.get_conn().cursor()

    try:
        cur.execute("BEGIN;")

        # Create table if it doesn't exist
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

        # Insert one row at a time with DELETE+INSERT (idempotent)
        for row in data_list:
            # Delete existing record with same location_name and localtime
            cur.execute(f"""
                DELETE FROM {target_table}
                WHERE location_name = %(location_name)s AND "localtime" = TO_TIMESTAMP(%(localtime)s)
            """, row)

            # Insert new record
            cur.execute(f"""
                INSERT INTO {target_table} VALUES (
                    %(location_name)s, %(region)s, %(country)s, %(lat)s, %(lon)s,
                    TO_TIMESTAMP(%(localtime)s), %(temp_c)s, %(temp_f)s,
                    %(feelslike_c)s, %(feelslike_f)s, %(is_day)s, %(condition)s,
                    %(wind_kph)s, %(wind_mph)s, %(wind_dir)s, %(wind_degree)s,
                    %(pressure_mb)s, %(pressure_in)s, %(precip_mm)s, %(precip_in)s,
                    %(humidity)s, %(cloud)s, %(windchill_c)s, %(heatindex_c)s,
                    %(dewpoint_c)s, %(vis_km)s, %(uv)s, %(gust_kph)s
                )
            """, row)

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
