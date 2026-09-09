select
    count(*) as unknown_company_count

from {{ ref('dim_company') }}

where company_key = md5('__UNKNOWN_COMPANY__')
  and company_name = 'Unknown'

having count(*) <> 1