select
    farm_id,
    crop_type,
    farm_area_acres,
    irrigation_type,
    fertilizer_used_tons,
    pesticide_used_kg,
    yield_tons,
    soil_type,
    season,
    water_usage_cubic_meters
from "airflow"."public"."stg_agriculture"