-- Quesion 3

SELECT count(1) FROM `dtc-de-course-412502.dbt_hly.fact_fhv_trips`;


-- Question 4
-- What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table?

SELECT 
  service_type,
  COUNT(tripid) as number_of_trips
FROM dtc-de-course-412502.dbt_hly.fact_trips
WHERE EXTRACT(month from pickup_datetime) = 7 AND EXTRACT(year from pickup_datetime) = 2019
GROUP BY 1,

-- Yellow -- 3232249
-- Green -- 397615

SELECT COUNT(1)
FROM dtc-de-course-412502.dbt_hly.fact_fhv_trips
WHERE EXTRACT(month FROM pickup_datetime ) = 7;
-- 290680
