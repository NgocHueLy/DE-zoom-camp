-- question 3
SELECT 
	CAST(lpep_pickup_datetime AS DATE) as start_date,
	CAST(lpep_dropoff_datetime AS DATE) as end_date,
	COUNT (1)
FROM public.green_taxi_trips
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-09-18'
	AND CAST(lpep_dropoff_datetime AS DATE) = '2019-09-18'
GROUP BY
	1,2;
	

--question 4
SELECT 
	CAST(lpep_pickup_datetime AS DATE) as date,
	max(trip_distance) as largest_trip_distance
FROM public.green_taxi_trips
GROUP BY
	1
ORDER BY
	largest_trip_distance DESC;


-- question 5
SELECT 
	CAST(lpep_pickup_datetime AS DATE) as date,
	z."Borough",
	sum(total_amount)
FROM public.green_taxi_trips t JOIN zones z
ON t."PULocationID" = z."LocationID" 
WHERE 
	CAST(lpep_pickup_datetime AS DATE) = '2019-09-18' AND
	z."Borough" != 'Unknown'
GROUP BY 1, 2
ORDER BY 3 DESC;
	
-- Question 6:
SELECT
	zpu."Zone" as pick_up_zone,
	zdo."Zone" as drop_off_zone,
	tip_amount 	
FROM public.green_taxi_trips t 
JOIN zones zpu ON t."PULocationID" = zpu."LocationID" 
JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE 
	zpu."Zone" = 'Astoria'
ORDER BY 3 DESC;

