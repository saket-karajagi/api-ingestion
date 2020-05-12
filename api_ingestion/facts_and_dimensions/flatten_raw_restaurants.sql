CREATE TABLE public.restaurant_inspections AS
SELECT
CAST(data_blob->>'camis' as integer) as camis
,CAST(data_blob->>'dba' as varchar) as dba
,CAST(data_blob->>'boro' as varchar) as boro
,CAST(data_blob->>'building' as varchar) as building
,CAST(data_blob->>'street' as varchar) as street
,CAST(data_blob->>'zipcode' as varchar) as zipcode
,CAST(data_blob->>'phone' as varchar) as phone
,CAST(data_blob->>'cuisine_description' as varchar) as cuisine
,CAST(data_blob->>'inspection_date' as timestamp) as inspection_date
,CAST(data_blob->>'action' as varchar) as action
,CAST(data_blob->>'violation_code' as varchar) as violation_code 
,CAST(data_blob->>'violation_description' as varchar) as violation_description
,CAST(data_blob->>'critical_flag' as varchar) as critical_flag
,CAST(data_blob->>'score' as integer) as score
,CAST(data_blob->>'grade' as varchar) as grade
,CAST(data_blob->>'grade_date' as timestamp) as grade_date
,CAST(data_blob->>'record_date' as timestamp) as record_date
,CAST(data_blob->>'inspection_type' as varchar) as inspection_type
,CAST(data_blob->>'latitude' as double precision) as latitude
,CAST(data_blob->>'longitude' as double precision) as longitude
,CAST(data_blob->>'community_board' as integer) as community_board
,CAST(data_blob->>'council_district' as integer) as council_district
,CAST(data_blob->>'census_tract' as integer) as census_tract
,CAST(data_blob->>'bin' as integer) as bin
,CAST(data_blob->>'bbl' as varchar) as bbl
,CAST(data_blob->>'nta' as varchar) as nta
FROM public.raw_restaurant_inspections;
