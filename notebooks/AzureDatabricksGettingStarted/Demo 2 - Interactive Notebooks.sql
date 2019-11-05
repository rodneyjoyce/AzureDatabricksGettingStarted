-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Interactive SQL Notebook - Cells, Shortcuts, Markdown

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### The next SQL command creates a Spark data table from a Databricks demo dataset

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds
  USING csv
  OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", inferSchema="true", header "true")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### We can now see that we have a SQL Table mounted on our cluster under Data

-- COMMAND ----------

SELECT COUNT(*) FROM diamonds

-- COMMAND ----------

SELECT * FROM diamonds LIMIT 5

-- COMMAND ----------

SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY color

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Magic Commands % - A bit of unix if you're into that sort of thing

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC curl https://github.com/

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Repeat the same operations using Python DataFrame API. 
-- MAGIC This is a SQL notebook; by default command statements are passed to a SQL interpreter. To pass command statements to a Python interpreter, include the `%python` magic command.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("Spark version", sc.version, spark.sparkContext.version, spark.version)
-- MAGIC print("Python version", sc.pythonVer)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/databricks-datasets/Rdatasets/data-001/csv/ggplot2/"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Creates a DataFrame from a Databricks dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamondsDF = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamondsDF.count()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Manipulates the data and displays the results

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import avg
-- MAGIC display(diamondsDF.select("color","price").groupBy("color").agg(avg("price")).sort("color"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Display the data in a chart and leave a comment for the next data science

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import avg
-- MAGIC 
-- MAGIC display(diamondsDF.select("color","price").groupBy("color").agg(avg("price")).sort("color"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Now let's pin it to a dashboard

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### What happens if we made a mistake? Revisions store a checkpoint for each change

-- COMMAND ----------

