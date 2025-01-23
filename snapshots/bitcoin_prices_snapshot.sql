{% snapshot bitcoin_prices_snapshot %}
{{ 
    config(
        target_schema='SNAPSHOTS',
        unique_key='date',
        strategy='check',
        check_cols=['open', 'high', 'low', 'close', 'volume']
    )
}}
SELECT *
FROM {{ source('bitcoin_data_db', 'bitcoin_prices') }}
{% endsnapshot %}
