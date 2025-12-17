with base as (
    select * from {{ ref('fact_farm_production') }}
),

-- base metrics
metrics as (
    select
        crop_type,
        season,
        irrigation_type,

        sum(yield_tons) as total_yield_tons,
        sum(yield_tons) / nullif(sum(farm_area_acres), 0) as yield_per_acre,
        sum(yield_tons) / nullif(sum(fertilizer_used_tons), 0) as fertilizer_efficiency,
        sum(yield_tons) / nullif(sum(water_usage_cubic_meters), 0) as water_productivity
    from base
    group by crop_type, season, irrigation_type
),

-- top 3 crops by yield per acre
top_crops as (
    select
        crop_type,
        yield_per_acre,
        dense_rank() over (order by yield_per_acre desc) as crop_rank
    from (
        select
            crop_type,
            sum(yield_tons) / nullif(sum(farm_area_acres), 0) as yield_per_acre
        from base
        group by crop_type
    ) t
),

-- top 3 irrigation methods by avg yield
top_irrigation as (
    select
        irrigation_type,
        avg(yield_tons) as avg_yield,
        dense_rank() over (order by avg(yield_tons) desc) as irrigation_rank
    from base
    group by irrigation_type
)

select
    m.crop_type,
    m.season,
    m.irrigation_type,
    m.total_yield_tons,
    m.yield_per_acre,
    m.fertilizer_efficiency,
    m.water_productivity,

    tc.crop_rank,
    ti.irrigation_rank

from metrics m
left join top_crops tc
    on m.crop_type = tc.crop_type
left join top_irrigation ti
    on m.irrigation_type = ti.irrigation_type

where
    tc.crop_rank <= 3
    or ti.irrigation_rank <= 3
