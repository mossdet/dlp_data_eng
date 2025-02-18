###########################
# Question 2
# Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
# What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

# Materialized Table
SELECT 
  'PULocationID' AS pickup_location,
  Count(DISTINCT PULocationID) as cntr
FROM 
  kestra-dlp-sandbox.de_zoomcamp.03_datawarehouse_hw_dlp_reg;

# External Table
SELECT 
  'PULocationID' AS pickup_location,
  Count(DISTINCT PULocationID)
FROM 
  kestra-dlp-sandbox.de_zoomcamp.03_datawarehouse_hw_dlp_ext;

###########################
# Question 3
# Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. 
# Why are the estimated number of Bytes different?

# Materialized Table
SELECT 
  'PULocationID'
FROM 
  kestra-dlp-sandbox.de_zoomcamp.03_datawarehouse_hw_dlp_reg;

# Materialized Table
SELECT 
  'PULocationID', 'DOLocationID'
FROM 
  kestra-dlp-sandbox.de_zoomcamp.03_datawarehouse_hw_dlp_reg;

###########################
# Question 4
# How many records have a fare_amount of 0?
SELECT 
  COUNT(1)
FROM 
  kestra-dlp-sandbox.de_zoomcamp.03_datawarehouse_hw_dlp_reg
WHERE 
  fare_amount = 0;

###########################
# Question 5
# What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID 
# (Create a new table with this strategy)
CREATE OR REPLACE TABLE kestra-dlp-sandbox.de_zoomcamp.03_datawarehouse_hw_dlp_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY 
  VendorID AS
    SELECT * FROM kestra-dlp-sandbox.de_zoomcamp.03_datawarehouse_hw_dlp_ext;


###########################
# Question 6
# Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
# Use the materialized table you created earlier in your from clause and note the estimated bytes. 
# Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? 
SELECT 
  DISTINCT('VendorID')
FROM 
  kestra-dlp-sandbox.de_zoomcamp.03_datawarehouse_hw_dlp_reg
WHERE
  tpep_dropoff_datetime >= '2024-03-01'
  and tpep_dropoff_datetime <= '2024-03-15';

SELECT 
  DISTINCT('VendorID')
FROM 
  kestra-dlp-sandbox.de_zoomcamp.03_datawarehouse_hw_dlp_partitioned_clustered
WHERE
  tpep_dropoff_datetime >= '2024-03-01'
  and tpep_dropoff_datetime <= '2024-03-15';
