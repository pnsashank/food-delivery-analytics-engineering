{% snapshot snap_restaurants %}
{{
    config(
        target_schema = 'snapshots',
        unique_key = 'restaurant_id',
        strategy = 'check',
        check_cols=[
            'brand_name',
            'brand_is_active',
            'outlet_name',
            'city',
            'delivery_zone',
            'address_line1',
            'postal_code',
            'is_active'
        ]
    )
}}
select *
from {{ ref('int_restaurants__snapshot_source') }}
{% endsnapshot %}

