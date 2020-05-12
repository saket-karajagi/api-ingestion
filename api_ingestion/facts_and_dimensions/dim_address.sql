/*
CREATE TABLE public.dim_address
(address_id serial primary key,
 boro varchar(20),
 building varchar(300),
 street varchar(300),
 zipcode varchar(10),
 latitude double precision,
 longitude double precision);

CREATE SEQUENCE address_sequence
  start 1
  increment 1;
*/

INSERT INTO public.dim_address (address_id, boro, building, street, zipcode, latitude, longitude)
SELECT
nextval('address_sequence'),
a.boro,
a.building,
a.street,
a.zipcode,
a.latitude,
a.longitude
FROM 
(SELECT distinct boro, building, street, zipcode, latitude, longitude FROM public.restaurant_inspections) a
WHERE NOT EXISTS (SELECT * FROM public.dim_address a1
                  WHERE a1.boro = a.boro AND
                        a1.building = a.building AND
                        a1.street = a.street AND
                        a1.zipcode = a.zipcode AND
                        a1.latitude = a.latitude AND
                        a1.longitude = a.longitude);
