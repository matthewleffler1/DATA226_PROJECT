{% snapshot snapshot_sj_weather %}
{{
    config(
      target_schema='"snapshots"',
      unique_key='"localtime"',
      strategy='check',
      check_cols="all",
      invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ ref('raw_sj_weather_data') }}
{% endsnapshot %}