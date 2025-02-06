# Homework 3

## Question 1
```SELECT count(*) FROM `swift-delight-448019-p9.taxi_data.external_table`;```

## Question 2
```SELECT COUNT(DISTINCT PULocationID) AS nr_of_unique_locations FROM `swift-delight-448019-p9.taxi_data.external_table`;```

```SELECT COUNT(DISTINCT PULocationID) AS nr_of_unique_locations FROM `swift-delight-448019-p9.taxi_data.regular_table`;```

## Question 3
```SELECT PULocationID FROM `swift-delight-448019-p9.taxi_data.regular_table`;```

```SELECT PULocationID, DOLocationID FROM `swift-delight-448019-p9.taxi_data.regular_table`;```

## Question 4
```SELECT count(*) FROM `swift-delight-448019-p9.taxi_data.regular_table` WHERE fare_amount=0;```

## Question 5
```CREATE TABLE `swift-delight-448019-p9.taxi_data.partitioned_table` PARTITION BY DATE(tpep_dropoff_datetime) CLUSTER BY VendorID AS SELECT * FROM `swift-delight-448019-p9.taxi_data.regular_table`;```

## Question 6 
```SELECT DISTINCT VendorID FROM `swift-delight-448019-p9.taxi_data.regular_table` WHERE tpep_dropoff_datetime BETWEEN TIMESTAMP('2024-03-01') AND TIMESTAMP('2024-03-15 23:59:59');```

```SELECT DISTINCT VendorID FROM `swift-delight-448019-p9.taxi_data.partitioned_table` WHERE tpep_dropoff_datetime BETWEEN TIMESTAMP('2024-03-01') AND TIMESTAMP('2024-03-15 23:59:59');```