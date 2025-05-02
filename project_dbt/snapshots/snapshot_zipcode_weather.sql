{% snapshot snapshot_zipcode_weather %}
{{
    config(
      target_schema='snapshots',
      unique_key=['LOCATION_NAME', '"localtime"'],
      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ ref('raw_zipcode_weather_data') }}
{% endsnapshot %}