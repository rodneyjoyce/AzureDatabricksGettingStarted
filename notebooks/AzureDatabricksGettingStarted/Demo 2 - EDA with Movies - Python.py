# Databricks notebook source
# MAGIC %md
# MAGIC #### Interactive Notebooks - Menu, Clusters, Cells, Shortcuts, Markdown

# COMMAND ----------

# This is a comment (PySpark)
display(dbutils.fs.ls("dbfs:/databricks-datasets/Rdatasets/data-001/csv/ggplot2/"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a DataFrame from the Databricks Movies CSV file

# COMMAND ----------

moviesDF = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/movies.csv", header="true", inferSchema="true")

# COMMAND ----------

# MAGIC %md ##### What are the 2 problems with the Dataframe above?

# COMMAND ----------

# MAGIC %md
# MAGIC #### How many records are there?

# COMMAND ----------

moviesDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What does the data and schema look like?

# COMMAND ----------

display(moviesDF.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Let's keep exploring - Show a few columns from the first 5 records and order them by Rating

# COMMAND ----------

display(moviesDF.select("year", "title", "rating").orderBy("rating").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimised Execution

# COMMAND ----------

display(moviesDF.orderBy("rating").select("year", "title", "rating").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark Dataframes are immutable

# COMMAND ----------

moviesDF.orderBy("rating").select("year", "title", "rating").limit(5)
display(moviesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### It's better not to overwrite data so that you can always trace the lineage for debugging... do this instead 

# COMMAND ----------

top5MoviesDf = moviesDF.orderBy("rating").select("year", "title", "rating").limit(5)
display(top5MoviesDf)

# OR 
#display(moviesDF.orderBy("rating").select("year", "title", "rating").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the average rating of all records?

# COMMAND ----------

from pyspark.sql.functions import avg, col

averageRatingDF = moviesDF.agg(avg("rating"))
display(averageRatingDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the average rating by year?

# COMMAND ----------

display(moviesDF.groupBy("year").agg(avg("rating")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Show the average rating for the last 100 year datapoints, sorted in descending order

# COMMAND ----------

display(
    moviesDF
      .groupBy(col("year"))
      .agg(avg(col("rating")).alias("average rating"))
      .sort(col("year").desc())
      .take(100)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Same data... but so much easier to spot possible insights visually - this proves that movies have just got worse over time
# MAGIC ##### It's also easy to analyse in Excel

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Prefer SQL to Python? The next SQL command creates a Spark data table from a Databricks demo CSV File

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS moviesSqlDF;
# MAGIC 
# MAGIC CREATE TABLE moviesSqlDF
# MAGIC   USING csv
# MAGIC   OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/movies.csv", header "true")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Count the records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM moviesSqlDF

# COMMAND ----------

# MAGIC %md 
# MAGIC #### And do the same but with SQL. Show the average rating for the last 100 years, sorted in descending order

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, avg(rating) AS aveRating FROM moviesSqlDF GROUP BY year ORDER BY year DESC LIMIT 100 
# MAGIC 
# MAGIC 
# MAGIC -- PySpark Version
# MAGIC -- display(
# MAGIC --     moviesDF
# MAGIC --       .groupBy(col("year"))
# MAGIC --       .agg(avg(col("rating")).alias("aveRating"))
# MAGIC --       .sort(col("year").desc())
# MAGIC --       .take(100)
# MAGIC -- )