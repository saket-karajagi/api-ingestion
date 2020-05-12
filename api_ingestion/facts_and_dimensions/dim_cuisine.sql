/*
CREATE TABLE public.cuisine(
cuisine_id SERIAL PRIMARY KEY,
cuisine_desc varchar);

CREATE SEQUENCE cuisine_sequence
  start 1
  increment 1;
*/
INSERT INTO public.dim_cuisine(cuisine_id, cuisine_desc)   
SELECT
nextval('cuisine_sequence'),
cuisine
FROM 
(SELECT distinct cuisine FROM public.restaurant_inspections) c
WHERE NOT EXISTS (SELECT * FROM public.cuisine c1
                  WHERE c1.cuisine_desc = c.cuisine);
