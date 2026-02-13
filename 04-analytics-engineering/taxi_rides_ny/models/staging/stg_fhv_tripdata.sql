with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select *
    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where dispatching_base_num is NOT null
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}
