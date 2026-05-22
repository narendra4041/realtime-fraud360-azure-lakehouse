{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    transient=false
) }}

with source_data as (

    select *

    from {{ source('silver', 'TRANSACTIONS_CLEAN') }}

    {% if is_incremental() %}

        where event_ts >
        (
            select coalesce(
                max(last_event_ts),
                '1900-01-01'
            )
            from {{ this }}
        )

    {% endif %}

),

aggregated as (

    select

        customer_id,

        count(*) as transaction_count,

        round(sum(amount), 2) as total_amount,

        round(avg(amount), 2) as avg_amount,

        round(max(amount), 2) as max_amount,

        sum(
            case
                when is_suspicious_hint then 1
                else 0
            end
        ) as suspicious_transaction_count,

        count(distinct country) as countries_used,

        count(distinct merchant_category)
            as merchant_categories_used,

        max(event_ts) as last_event_ts,

        current_timestamp() as dbt_processed_ts

    from source_data

    group by customer_id

)

select *
from aggregated