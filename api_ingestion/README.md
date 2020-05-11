# API Ingestion

## Overview:

The ingest_api_to_rds process is a fully automated, v1 production-ready ELT process that is generic (can work for any API endpoint with minimal setup), and uses a schema-on-read methodology to account for upstream schema changes. The data is ingested in Postgres RDS for analytical purposes.

### HOW-TO Execute (Python, Spark and RDS):

```
pip install sodapy 
pip install pandas
python ingest_api_to_rds.py -d restaurants
```

The argument peeks the config.py file for the following required inputs:

* API endpoint / URL
* API access key
* NYS dataset_id

### One-time Schema Inference

1. Spark Environment:

```
#File location and type
file_location = "/FileStore/tables/restaurants.csv"
file_type = "csv"

#CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

#The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

df.printSchema()
```

2. Paste the output in RDS and build the view to dedupe records on camis (unique restaurant identifier) for the latest inspection_date 

3. Execute ```v_restaurants.sql```

4. Verify Results (RDS):

```
postgres=> select * from public.v_restaurants limit 100;
```

## DQ Tests (RDS):


1. Records count match the csv file:
```
postgres=> select count(*) from public.v_restaurants;
-[ RECORD 1 ]-
count | 389447
```
```
389448 restaurants_2020-05-08.csv (includes header)
```
2. No duplicates found in the dataset
```
postgres=> select * from 
          (select camis, 
                  inspection_date, 
                  violation_code, 
                  score, 
                  grade, 
                  count(*) as count 
          from restaurants_staging 
          where violation_code is not null 
          group by 1, 2, 3, 4, 5) as a where count > 1;
(0 rows)
```

3. Some fields such as grade, grade_date, score and address can be NULL which indicates that they’re not yet graded
4. Caveat from NYS Open Data: Because this dataset is compiled from several large administrative data systems, it contains some illogical values that could be a result of data entry or transfer errors. Data may also be missing

## Key Features:
1. ingest_api_to_rds.py is generic and can work on any API endpoint with minimal setup and its modules can be easily replicated to work on other API endpoints and sources such as csv, relational databases etc
2. The structure of the data from the API can evolve overtime. The ELT process creates a staging table to load the source data, converts the attributes into a json blob which is inserted into the “destination” table.
3. A view is designed using schema inference to cast the data to its accurate data types and dedupe the latest restaurant records. Records that do not contain certain attributes present in future data extracts appear NULL in the view.
4. The process is robust, tested for errors and potential breaking points, user-friendly and provides a verbose output as it executes

## Challenges / Constraints:
1. The end-to-end work on the entire exercise (including development and documentation) was time-boxed to 3 hours to simulate a real-life scenario with deadlines, and the goal was to produce the most robust, reusable process within the time limit with future room for extensibility
2. My initial approach was to extract the data from the API in json and ingest the json into a blob data type in Postgres RDS. This was unsuccessful as I ran into unescapable characters such as ‘\t’ and ‘ô’ while bulk loading using the COPY command in Postgres
3. UPSERT / MERGE in Postgres isn’t as straightforward and I spent time figuring out the syntax. It didn’t end up working since there is no primary key for the dataset
4. Schema inference is not completely accurate in pandas/spark as it only infers off of a chunk of the entire dataset. Some manual trial-and-error had to be performed to get the final view correct.
5. Some more error logging and testing could’ve been done to improve the process with more time

## Data Model / BI Extract:

*Since the data extract came from an administrative system, it was unclear what the grain of the data was or what each row in the dataset represents.

*We created a Fact table on top of the dataset to show the latest inspection results by date for every restaurant.
```
postgres=> create view public.inspection_snapshot as
select a.*
from public.v_restaurants a 
join (select camis, max(inspection_date) as inspection_date 
      from public.v_restaurants where grade is not null 
      group by 1) as b on a.camis = b.camis and a.inspection_date = b.inspection_date;
```

## Sample analysis:

1. Inspection results aggregated by zipcode:
```
postgres=> select zipcode, grade, count(*) from public.inspection_snapshot group by 1, 2;
```

2. Violations performed by a particular restaurant on the latest inspection date
```
postgres=> select * from public.inspection_snapshot where dba = 'FELIDIA RESTAURANT';
```

3. Distribution of cuisines in NYC (American cuisine restaurants had the majority)

```
postgres=> select cuisine, count(*) from public.inspection_snapshot group by 1;
```

4. Find the top 100 restaurants with the most violations

```
postgres=> select dba, count(violation_code) from public.inspection_snapshot group by 1 order by 2 desc limit 100;
```

## Future Improvements:
1. A dimensional model more specific to business use case, including storing the history using Slowly Changing Dimensions and each dimension in separate tables with data backfilled for time, date, address, cuisines, restaurants and grade
2. It would’ve been interesting to perform more interesting analysis such as which/how many restaurants had their grades improve overtime, or which zipcodes had the worst violations in the city
3. UPSERT / MERGE into the destination table instead of TRUNCATE / INSERT to overwrite data that is updated, and perform no action if stays the same
4. Better orchestration using Airflow, logging and alerting for the process
5. Database credentials better secured in a conf file in Airflow
