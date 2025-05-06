-- cannot work
CREATE OR REPLACE EXTERNAL TABLE `even-acumen-450403-v4.zoomcamp.fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-*.parquet']
);


SELECT count(*) FROM `even-acumen-450403-v4.zoomcamp.fhv_tripdata`;


SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `even-acumen-450403-v4.zoomcamp.fhv_tripdata`;


CREATE OR REPLACE TABLE `even-acumen-450403-v4.zoomcamp.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `even-acumen-450403-v4.zoomcamp.fhv_tripdata`;

CREATE OR REPLACE TABLE `even-acumen-450403-v4.zoomcamp.fhv_partitioned_tripdata`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `even-acumen-450403-v4.zoomcamp.fhv_tripdata`
);

SELECT count(*) FROM  `even-acumen-450403-v4.zoomcamp.fhv_nonpartitioned_tripdata`
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');


SELECT count(*) FROM `even-acumen-450403-v4.zoomcamp.fhv_partitioned_tripdata`
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');