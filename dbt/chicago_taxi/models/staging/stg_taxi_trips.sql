with source as (

    select * 
    from {{source('raw', 'taxi_trips')}}

),

typed as (

     select
        trip_id,
        taxi_id,

        nullif(trim(trip_start_timestamp), '')::timestamp as trip_start_timestamp,
        nullif(trim(trip_end_timestamp), '')::timestamp as trip_end_timestamp,
        nullif(trim(trip_seconds), '')::integer as trip_seconds,
        nullif(trim(trip_miles), '')::numeric as trip_miles,

        nullif(trim(pickup_census_tract), '') as pickup_census_tract,
        nullif(trim(dropoff_census_tract), '') as dropoff_census_tract,
        nullif(trim(pickup_community_area), '')::integer as pickup_community_area,
        nullif(trim(dropoff_community_area), '')::integer as dropoff_community_area,

        nullif(trim(fare), '')::numeric as fare,
        nullif(trim(tips), '')::numeric as tips,
        nullif(trim(tolls), '')::numeric as tolls,
        nullif(trim(extras), '')::numeric as extras,
        nullif(trim(trip_total), '')::numeric as trip_total,

        payment_type,
        company,

        nullif(trim(pickup_centroid_latitude), '')::numeric as pickup_centroid_latitude,
        nullif(trim(pickup_centroid_longitude), '')::numeric as pickup_centroid_longitude,
        pickup_centroid_location,

        nullif(trim(dropoff_centroid_latitude), '')::numeric as dropoff_centroid_latitude,
        nullif(trim(dropoff_centroid_longitude), '')::numeric as dropoff_centroid_longitude,
        dropoff_centroid_location,

        _batch_id,
        _source_name,
        _ingested_at

    from source

)

select * from typed