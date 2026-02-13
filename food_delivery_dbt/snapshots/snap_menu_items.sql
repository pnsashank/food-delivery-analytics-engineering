{% snapshot snap_menu_items %}
{{
    config(
        target_schema = 'snapshots',
        unique_key = 'menu_item_id',
        strategy = 'check',
        check_cols = ['item_name', 'category']
    )
}}

select *
from {{ ref('int_menu_items__snapshot_source') }}
{% endsnapshot %}
