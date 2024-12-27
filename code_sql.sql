-- Databricks notebook source
-- MAGIC
-- MAGIC %python
-- MAGIC #creating a user defined function
-- MAGIC def year_variable(year):
-- MAGIC     # perform complex calculation
-- MAGIC     result = year
-- MAGIC     return result

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Registering the User Defined Function
-- MAGIC from pyspark.sql.functions import udf
-- MAGIC
-- MAGIC spark.udf.register("year_variable", year_variable)

-- COMMAND ----------

--Creating a temporary view by selecting the function
CREATE OR REPLACE TEMPORARY VIEW year AS
SELECT year_variable('2021') AS year_value

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Retrieving the year value from temporary view.
-- MAGIC year_value = spark.sql("SELECT * FROM year").collect()[0][0]
-- MAGIC
-- MAGIC # Using the year value in the python code
-- MAGIC basefile = "clinicaltrial_" + year_value

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC #assigning basefile as the environment variable
-- MAGIC import os
-- MAGIC os.environ ['basefile'] = basefile

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial = spark.read.options(delimiter ="|").csv("/FileStore/tables/" + basefile +  ".csv/", header= "True", inferSchema=True)
-- MAGIC
-- MAGIC pharma = spark.read.csv('/FileStore/tables/pharma', header=True, sep=',', inferSchema = True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #creating temporary views of the data
-- MAGIC clinicaltrial.createOrReplaceTempView('clinicaltrial')
-- MAGIC
-- MAGIC pharma.createOrReplaceTempView('pharma')

-- COMMAND ----------

select * from clinicaltrial

-- COMMAND ----------

select * from pharma

-- COMMAND ----------



-- COMMAND ----------

--creating a table for  clinicaltrial in the default database
create or replace table default.clinicaltrial as select * from clinicaltrial

-- COMMAND ----------

--creating a table for  pharma in the default database
create or replace table default.pharma as select * from pharma

-- COMMAND ----------

show tables

-- COMMAND ----------

------count of all the studies using ID column
select 'Total number of studies: ', count('id') as Count_of_Studies from default.clinicaltrial

-- COMMAND ----------

----count of all the types

SELECT Type, COUNT(*) as count
FROM default.clinicaltrial
GROUP BY Type
ORDER BY count DESC

-- COMMAND ----------

--Question3: Count of Conditions and Frequency

-- COMMAND ----------

SELECT condition, COUNT(*) AS Frequency
FROM (
    SELECT explode(split(Conditions, ',')) AS condition
    FROM default.clinicaltrial
    WHERE Conditions IS NOT NULL
)
GROUP BY condition
ORDER BY Frequency DESC
LIMIT 5;

-- COMMAND ----------

SELECT default.clinicaltrial.Sponsor, COUNT(*) AS Count_of_Sponsor
FROM default.clinicaltrial
LEFT JOIN default.pharma ON default.clinicaltrial.Sponsor = default.pharma.Parent_Company
WHERE default.pharma.Parent_Company IS NULL AND default.clinicaltrial.Sponsor IS NOT NULL
GROUP BY default.clinicaltrial.Sponsor
ORDER BY Count_of_Sponsor DESC
LIMIT 10

-- COMMAND ----------

SELECT a.Sponsor, COUNT(*) AS Count_of_Sponsor
FROM default.clinicaltrial a
LEFT JOIN default.pharma b ON a.Sponsor = b.Parent_Company
WHERE b.Parent_Company IS NULL AND a.Sponsor IS NOT NULL
GROUP BY a.Sponsor
ORDER BY Count_of_Sponsor DESC
LIMIT 10

-- COMMAND ----------

----doing a count of each month ordered by the calendar months

 CREATE OR REPLACE TEMPORARY VIEW completed_studies_by_month AS
SELECT DATE_FORMAT(completion_date, "MMM") AS Month, COUNT(*) AS Number_of_Completed_Studies
FROM
  (SELECT id, completion, CAST(TO_DATE(completion, 'MMM yyyy') AS DATE) AS completion_date
   FROM default.clinicaltrial
   WHERE completion LIKE CONCAT('%',(select * from year), '%') AND Status ='Completed')
GROUP BY Month
order by case Month 
when 'Jan' then 1
when 'Feb' then 2
when 'Mar' then 3
when 'Apr'  then 4
when 'May' then 5
when 'Jun' then 6
when 'Jul' then 7
when 'Aug' then 8
when 'Sep' then 9
when 'Oct'  then 10
when 'Nov' then 11
when 'Dec' then 12
end

-- COMMAND ----------

select * from completed_studies_by_month

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #transforming the table to a dataframe
-- MAGIC
-- MAGIC df_completed_studies = spark.table("completed_studies_by_month")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #converting the dataframe to a panda and ploting a bar chart using matplotlib
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC df_pandas = df_completed_studies.toPandas()
-- MAGIC df_pandas.plot(kind="bar", x="Month", y="Number_of_Completed_Studies", color = 'red')
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC FURTHER ANALYSIS

-- COMMAND ----------

--create a view of all completed studies within a period
CREATE OR REPLACE TEMP VIEW Study_Period AS

SELECT Id, Start, Completion,  

       CAST(TO_DATE(Start, 'MMM yyyy') AS DATE) AS start_date, 

       CAST(TO_DATE(Completion, 'MMM yyyy') AS DATE) AS completion_date, 

       DATEDIFF(MONTH, CAST(TO_DATE(Start, 'MMM yyyy') AS DATE), CAST(TO_DATE(Completion, 'MMM yyyy') AS DATE)) AS months_diff 

FROM default.clinicaltrial


-- COMMAND ----------

select * from Study_Period

-- COMMAND ----------

--counting all the studies completed in 12 months or less
select count(*) from study_period where months_diff <= 12

-- COMMAND ----------

--viewing all the studies completed in 12 months
select * from study_period where months_diff <= 12

-- COMMAND ----------


