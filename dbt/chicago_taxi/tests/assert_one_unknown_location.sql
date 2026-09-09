select
    count(*) as unknown_location_count

from {{ ref('dim_location') }}

where location_key = md5('__UNKNOWN_LOCATION__')
  and community_area is null
  and census_tract is null
  and centroid_latitude is null
  and centroid_longitude is null

having count(*) <> 1