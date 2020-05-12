
CREATE OR REPLACE TABLE public.stg_fact_latest_inspections
(restaurant_id varchar,
 cuisine_id varchar,
 address_id varchar,
 inspection_date date,
 inspection_type varchar,
 action varchar,
 violation_code varchar,
 violation_description varchar,
 critical_flag varchar,
 grade varchar);
/*
CREATE TABLE public.fact_latest_inspections
(restaurant_id varchar,
 cuisine_id varchar,
 address_id varchar,
 inspection_date date,
 inspection_type varchar,
 action varchar,
 violation_code varchar,
 violation_description varchar,
 critical_flag varchar,
 grade varchar);
*/
INSERT INTO public.stg_fact_latest_inspections (restaurant_id, cuisine_id, address_id, inspection_date, inspection_type, action, violation_code, violation_description, critical_flag, grade)
SELECT DISTINCT
       dr.restaurant_id,
       dc.cuisine_id,
       da.address_id,
       a.inspection_date,
       a.inspection_type,
       a.action,
       a.violation_code,
       a.violation_description,
       a.critical_flag,
       a.grade
from public.restaurant_inspections a
join (select camis, max(inspection_date) as inspection_date 
      from public.restaurant_inspections where grade is not null
      group by 1) as b on a.camis = b.camis and a.inspection_date = b.inspection_date
join dim_address da on (da.boro = a.boro AND da.building = a.building AND da.street = a.street AND da.zipcode = a.zipcode AND da.latitude = a.latitude AND da.longitude = a.longitude)
join dim_restaurant dr on dr.restaurant_id = a.camis::varchar
join dim_cuisine dc on dc.cuisine_desc = a.cuisine;

INSERT INTO public.fact_latest_inspections
SELECT DISTINCT *
FROM public.stg_fact_latest_inspections a
WHERE NOT EXISTS (SELECT * from public.fact_latest_inspections f1
                  WHERE COALESCE(f1.restaurant_id, 'NA') = COALESCE(a.restaurant_id, 'NA') AND
                        COALESCE(f1.cuisine_id, 'NA') = COALESCE(a.cuisine_id, 'NA') AND
                        COALESCE(f1.address_id, 'NA') = COALESCE(a.address_id, 'NA') AND
                        COALESCE(f1.inspection_date, '2018-08-13 00:00:00') = COALESCE(a.inspection_date, '2018-08-13 00:00:00') AND
                        COALESCE(f1.inspection_type, 'NA') = COALESCE(a.inspection_type, 'NA') AND
                        COALESCE(f1.action, 'NA') = COALESCE(a.action, 'NA') AND
                        COALESCE(f1.violation_code, 'NA') = COALESCE(a.violation_code, 'NA') AND
                        COALESCE(f1.violation_description, 'NA') = COALESCE(a.violation_description, 'NA') AND
                        COALESCE(f1.critical_flag, 'NA') = COALESCE(a.critical_flag, 'NA') AND
                        COALESCE(f1.grade, 'NA') = COALESCE(a.grade, 'NA'));
