-- SELECT THE COLUMNS INTERESTED FOR YOU
SELECT passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, tolls_amount, tip_amount
FROM `even-acumen-450403-v4.zoomcamp.yellow_tripdata_partitioned` WHERE fare_amount != 0;

-- CREATE A ML TABLE WITH APPROPRIATE TYPE
CREATE OR REPLACE TABLE `even-acumen-450403-v4.zoomcamp.yellow_tripdata_ml` (
`passenger_count` INTEGER,
`trip_distance` FLOAT64,
`PULocationID` STRING,
`DOLocationID` STRING,
`payment_type` STRING,
`fare_amount` FLOAT64,
`tolls_amount` FLOAT64,
`tip_amount` FLOAT64
) AS (
SELECT passenger_count, trip_distance, cast(PULocationID AS STRING), CAST(DOLocationID AS STRING),
CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
FROM `even-acumen-450403-v4.zoomcamp.yellow_tripdata_partitioned` WHERE fare_amount != 0
);

-- CREATE MODEL WITH DEFAULT SETTING
CREATE OR REPLACE MODEL `even-acumen-450403-v4.zoomcamp.tip_boosted_model`
OPTIONS(
  model_type = 'boosted_tree_regressor',
  input_label_cols = ['tip_amount'],
  DATA_SPLIT_METHOD = 'AUTO_SPLIT'
) AS
SELECT
  passenger_count,
  trip_distance,
  CAST(PULocationID AS STRING) AS PULocationID,
  CAST(DOLocationID AS STRING) AS DOLocationID,
  CAST(payment_type AS STRING) AS payment_type,
  fare_amount,
  tolls_amount,
  tip_amount
FROM `even-acumen-450403-v4.zoomcamp.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;

-- CHECK FEATURES
SELECT * 
FROM ML.FEATURE_INFO(MODEL `even-acumen-450403-v4.zoomcamp.tip_boosted_model`);

-- EVALUATE THE MODEL
SELECT * 
FROM ML.EVALUATE(
  MODEL `even-acumen-450403-v4.zoomcamp.tip_boosted_model`,
  (
    SELECT *
    FROM `even-acumen-450403-v4.zoomcamp.yellow_tripdata_ml`
    WHERE tip_amount IS NOT NULL
  )
);

-- PREDICT THE MODEL
SELECT * 
FROM ML.PREDICT(
  MODEL `even-acumen-450403-v4.zoomcamp.tip_boosted_model`,
  (
    SELECT *
    FROM `even-acumen-450403-v4.zoomcamp.yellow_tripdata_ml`
    WHERE tip_amount IS NOT NULL
  )
);

-- PREDICT AND EXPLAIN
SELECT * 
FROM ML.EXPLAIN_PREDICT(
  MODEL `even-acumen-450403-v4.zoomcamp.tip_boosted_model`,
  (
    SELECT *
    FROM `even-acumen-450403-v4.zoomcamp.yellow_tripdata_ml`
    WHERE tip_amount IS NOT NULL
  ),
  STRUCT(3 AS top_k_features)
);

-- HYPER PARAM TUNNING
CREATE OR REPLACE MODEL `even-acumen-450403-v4.zoomcamp.tip_boosted_model_tuned`
OPTIONS(
  model_type = 'boosted_tree_regressor',
  input_label_cols = ['tip_amount'],
  DATA_SPLIT_METHOD = 'AUTO_SPLIT',
  num_trials = 10, 
  max_parallel_trials = 3, 
  max_tree_depth = HPARAM_RANGE(3, 10),
  min_split_loss = HPARAM_RANGE(0, 5),
  l1_reg = HPARAM_RANGE(0, 5),
  l2_reg = HPARAM_RANGE(0, 5),
  subsample = HPARAM_RANGE(0.6, 1.0),
  booster_type = 'GBTREE'  -- Gradient Boosted Trees
) AS
SELECT
  passenger_count,
  trip_distance,
  CAST(PULocationID AS STRING) AS PULocationID,
  CAST(DOLocationID AS STRING) AS DOLocationID,
  CAST(payment_type AS STRING) AS payment_type,
  fare_amount,
  tolls_amount,
  tip_amount
FROM `even-acumen-450403-v4.zoomcamp.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;