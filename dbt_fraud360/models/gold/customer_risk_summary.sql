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

    count(distinct merchant_category) as merchant_categories_used,

    current_timestamp() as dbt_processed_ts

from {{ source('silver', 'TRANSACTIONS_CLEAN') }}

group by customer_id