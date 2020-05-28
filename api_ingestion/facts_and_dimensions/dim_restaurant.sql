/*
CREATE TABLE public.dim_restaurant
(restaurant_id integer primary key,
 restaurant_name varchar);
*/
INSERT INTO public.dim_restaurant(restaurant_id, restaurant_name)
SELECT
DISTINCT
camis,
dba
FROM public.restaurant_inspections
/*WHERE NOT EXISTS (SELECT * FROM public.dim_restaurant r1
                  WHERE r1.restaurant_id = camis) */
WHERE record_date between current_date and date_sub(current_date, interval -7 day)
ON CONFLICT (camis) 
DO
		UPDATE
	  SET dba = EXCLUDED.dba;
