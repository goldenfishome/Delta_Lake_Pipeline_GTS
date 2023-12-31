# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,map_keys,col,split,to_timestamp
from pyspark.sql.functions import from_utc_timestamp,unix_timestamp
#from pyspark.sql.types import *

# COMMAND ----------

# create a SparkSession
spark = SparkSession.builder.appName("ReadJSON").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read All data from DBFS

# COMMAND ----------

# MAGIC %md
# MAGIC ### DF 1: yelp_academic_dataset_business.json

# COMMAND ----------

spark = SparkSession.builder.appName("LoadAndSplitJSON").getOrCreate()

business_df = spark.read.json("/FileStore/tables/yelp_data/yelp_academic_dataset_business.json")


# Extract all columns from the nested structure using list comprehension
nested_columns = [col("attributes" + "." + c).alias("attributes" + "_" + c) for c in business_df.select("attributes" + ".*").columns]

business_df = business_df.select("*", *nested_columns).drop('attributes')

# Show the resulting DataFrame
#business_df.show()


# COMMAND ----------

# Create a view or table

temp_business = "yelp_academic_dataset_business_json"

business_df.createOrReplaceTempView(temp_business)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DF 2: yelp_academic_dataset_tip.json

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/yelp_data/yelp_academic_dataset_tip.json"
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
tip_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#display(tip_df)

# COMMAND ----------

# Create a view or table

tip_temp = "yelp_academic_dataset_tip_json"

tip_df.createOrReplaceTempView(tip_temp)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DF 3: yelp_academic_dataset_checkin.json

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/yelp_data/yelp_academic_dataset_checkin.json"

# The applied options are for CSV files. For other file types, these will be ignored.
checkin_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#display(checkin_df)

# split the column 
checkin_df_temp = checkin_df.select("business_id",split(col("date"),",").alias("DateArray")).drop("date")
checkin_df = checkin_df_temp.select(checkin_df_temp.business_id,explode(checkin_df_temp.DateArray))

checkin_df.show(10)

# COMMAND ----------

# Create a view or table

checkin_temp = "yelp_academic_dataset_checkin_json"

checkin_df.createOrReplaceTempView(checkin_temp)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DF 4: yelp_academic_dataset_user.json

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/split_1.json"

# The applied options are for CSV files. For other file types, these will be ignored.
user1_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)


# Create a view or table

user1_temp = "yelp_academic_dataset_user1_json"

user1_df.createOrReplaceTempView(user1_temp)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/split_2.json"

# The applied options are for CSV files. For other file types, these will be ignored.
user2_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Create a view or table

user2_temp = "yelp_academic_dataset_user2_json"

user2_df.createOrReplaceTempView(user2_temp)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/split_3.json"

# The applied options are for CSV files. For other file types, these will be ignored.
user3_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Create a view or table

user3_temp = "yelp_academic_dataset_user3_json"

user3_df.createOrReplaceTempView(user3_temp)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DF 5: yelp_academic_dataset_review.json

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/review_split_1.json"

# The applied options are for CSV files. For other file types, these will be ignored.
review1_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Create a view or table

review1_temp = "yelp_academic_dataset_review1_json"

review1_df.createOrReplaceTempView(review1_temp)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/review_split_2.json"

# The applied options are for CSV files. For other file types, these will be ignored.
review2_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Create a view or table

review2_temp = "yelp_academic_dataset_review2_json"

review2_df.createOrReplaceTempView(review2_temp)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/review_split_3.json"

# The applied options are for CSV files. For other file types, these will be ignored.
review3_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Create a view or table

review3_temp = "yelp_academic_dataset_review3_json"

review3_df.createOrReplaceTempView(review3_temp)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/review_split_4.json"

# The applied options are for CSV files. For other file types, these will be ignored.
review4_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Create a view or table

review4_temp = "yelp_academic_dataset_review4_json"

review4_df.createOrReplaceTempView(review4_temp)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 2: Merge the splited data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Merge yelp_academic_dataset_review.json data

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE TABLE yelp_academic_dataset_review_json AS
# MAGIC   SELECT *
# MAGIC   FROM `yelp_academic_dataset_review1_json`
# MAGIC   UNION
# MAGIC   SELECT *
# MAGIC   FROM `yelp_academic_dataset_review2_json`
# MAGIC   UNION
# MAGIC   SELECT *
# MAGIC   FROM `yelp_academic_dataset_review3_json`
# MAGIC   UNION
# MAGIC   SELECT *
# MAGIC   FROM `yelp_academic_dataset_review4_json`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Merge yelp_academic_dataset_user.json data

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE TABLE yelp_academic_dataset_user_json AS
# MAGIC   SELECT *
# MAGIC   FROM `yelp_academic_dataset_user1_json`
# MAGIC   UNION
# MAGIC   SELECT *
# MAGIC   FROM `yelp_academic_dataset_user2_json`
# MAGIC   UNION
# MAGIC   SELECT *
# MAGIC   FROM `yelp_academic_dataset_user3_json`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create unified table

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE OR REPLACE TABLE unified_tab AS
# MAGIC SELECT
# MAGIC   r.review_id,
# MAGIC   r.business_id,
# MAGIC   r.date,
# MAGIC   b.city,
# MAGIC   b.state,
# MAGIC   b.categories,
# MAGIC   b.review_count,
# MAGIC   '2023-12-31' AS startdate,
# MAGIC   'default' AS enddate
# MAGIC FROM 
# MAGIC   `yelp_academic_dataset_review_json` r
# MAGIC LEFT JOIN
# MAGIC   `yelp_academic_dataset_business_json` b
# MAGIC   ON r.business_id=b.business_id
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM 
# MAGIC     `unified_tab` 

# COMMAND ----------

unified_df = spark.sql(f'''SELECT
                            *
                            FROM 
                            `unified_tab`''')
#display(unified_df)

# COMMAND ----------

unified_df.write.mode("overwrite").saveAsTable("silver_unified_df")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q: How many reviews are there for each business?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   business_id,
# MAGIC   COUNT(business_id) AS num_reviews
# MAGIC FROM
# MAGIC   `unified_tab`
# MAGIC GROUP BY
# MAGIC   business_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q: How many businesses take place in each state, In each city? 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     state, 
# MAGIC     city,
# MAGIC     COUNT(DISTINCT business_id) AS business_num
# MAGIC FROM 
# MAGIC     `unified_tab`
# MAGIC GROUP BY
# MAGIC     state, city

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q:  What kind of business do they have the most in each state, in each city ?

# COMMAND ----------

business_df_temp = business_df.select('*',split(col('categories'),',').alias('categoryArray')).drop('categories')
business_df_temp = business_df_temp.select('*',explode(business_df_temp.categoryArray)).drop('categoryArray')
#business_df_temp = business_df_temp.select(['business_id','categories','city','state','col'])
#display(business_df_temp)

# COMMAND ----------

business_df_temp = business_df_temp.select(['business_id','city','state','col'])
#display(business_df_temp)

# COMMAND ----------

# Create a view or table

business_tab_temp = "business_tab_temp"

business_df_temp.createOrReplaceTempView(business_tab_temp)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC   state,
# MAGIC   city,
# MAGIC   col,
# MAGIC   category_num
# MAGIC FROM 
# MAGIC   (SELECT
# MAGIC     *,
# MAGIC     RANK() OVER(PARTITION BY state,city ORDER BY category_num DESC) AS ranking
# MAGIC   FROM 
# MAGIC     (SELECT
# MAGIC         state,
# MAGIC         city,
# MAGIC         col,
# MAGIC         COUNT(col) AS category_num
# MAGIC     FROM 
# MAGIC         `business_tab_temp`
# MAGIC     GROUP BY
# MAGIC         state,
# MAGIC         city,
# MAGIC         col) sub) sub2
# MAGIC WHERE
# MAGIC   ranking=1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Q: What time do people usually write reviews?

# COMMAND ----------

unified_df_temp = unified_df.select("*", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss").alias('date_time'))
unified_df_temp = unified_df_temp.withColumn('Hour', hour(unified_df_temp.date_time))

# Create a view or table

unified_tab_time_temp = "unified_tab_time_temp"

unified_df_temp.createOrReplaceTempView(unified_tab_time_temp)


#display(unified_df_temp)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   Hour,
# MAGIC   COUNT(Hour) AS hour_cnt
# MAGIC FROM `unified_tab_time_temp`
# MAGIC GROUP BY
# MAGIC   Hour
# MAGIC ORDER BY
# MAGIC   hour_cnt DESC

# COMMAND ----------

spark.stop()

# COMMAND ----------



# COMMAND ----------


