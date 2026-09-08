with normalized_companies as (

    select distinct
        nullif(trim(company), '') as company_name

    from {{ ref('stg_taxi_trips') }}

),

known_companies as (

    select
        md5(company_name) as company_key,
        company_name

    from normalized_companies
    where company_name is not null

),

unknown_company as (

    select
        md5('__UNKNOWN_COMPANY__') as company_key,
        'Unknown'::text as company_name

)

select *
from unknown_company

union all

select *
from known_companies