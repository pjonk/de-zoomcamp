# HW2 Queries 

Question 3

`SELECT COUNT(*) FROM `swift-delight-448019-p9.taxi_data.yellow_taxi_data` WHERE FORMAT_DATE('%Y', source_year_month) = '2020'`

Question 4

`SELECT COUNT(*) FROM `swift-delight-448019-p9.taxi_data.green_taxi_data` WHERE FORMAT_DATE('%Y', source_year_month) = '2020'`

Question 5

`SELECT COUNT(*) FROM `swift-delight-448019-p9.taxi_data.yellow_taxi_data` WHERE FORMAT_DATE('%Y-%m', source_year_month) = '2021-03'`