{{
    config(
        materialized='table'
    )
}}

with fhv_trip as (
    select * from {{ ref ('stg_fhv_tripdata') }}
    where (pickup_locationid is not null and dropoff_locationid is not null)
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    fhv_trip.dispatching_base_num,
    fhv_trip.pickup_datetime,
    fhv_trip.dropoff_datetime,
    fhv_trip.pickup_locationid as pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_trip.dropoff_locationid as dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_trip.sr_flag,
    fhv_trip.affiliated_base_number

from fhv_trip 
inner join dim_zones as pickup_zone
on pickup_zone.locationid = fhv_trip.pickup_locationid
inner join dim_zones as dropoff_zone
on dropoff_zone.locationid = fhv_trip.dropoff_locationid
