select
    count(*) as unknown_date_count

from {{ ref('dim_date') }}

where date_key = 0

having count(*) <> 1