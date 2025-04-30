SELECT "localtime",
       TEMP_F,
       WIND_MPH,
       PRECIP_IN,
       HUMIDITY,
       CLOUD AS CLOUDINESS,
       UV
FROM {{ source('RAW', 'WEATHER_HISTORY_SJ_14DAYS') }}