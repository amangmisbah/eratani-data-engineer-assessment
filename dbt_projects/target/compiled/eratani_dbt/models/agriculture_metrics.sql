select
    crop_type,
    season,

    sum(yield_tons) as total_yield_tons,

    sum(yield_tons) / nullif(sum(farm_area_acres), 0)
        as yield_per_acre,

    sum(yield_tons) / nullif(sum(fertilizer_used_tons), 0)
        as fertilizer_efficiency,

    sum(yield_tons) / nullif(sum(water_usage_cubic_meters), 0)
        as water_productivity

from "airflow"."public"."fact_farm_production"
group by
    crop_type,
    season