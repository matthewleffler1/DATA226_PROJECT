SELECT LOCATION_NAME,
       LAT,
       LON,
       CASE
            WHEN LAT < 35.689062 THEN 'Southern California'
            WHEN LAT < 38.843757 THEN 'Central California'
            ELSE 'Northern California'
       END AS REGION,
       "localtime" as TIMESTAMP,
       TEMP_F,
       WIND_MPH,
       WIND_DIR,
       PRECIP_IN,
       HUMIDITY,
       CLOUDINESS,
       CONDITION
       UV,
       GUST_KPH
FROM {{ ref("raw_zipcode_weather_data") }}