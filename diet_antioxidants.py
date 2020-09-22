# Databricks notebook source
from csv import reader
from pyspark.sql import Row 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import seaborn as sb
import matplotlib.pyplot as plt
import warnings
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, isnan, when, count, trim, desc, sum, asc
from pyspark.sql.functions import countDistinct, explode, split, concat_ws, collect_list
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date

import os
os.environ["PYSPARK_PYTHON"] = "python3"

# COMMAND ----------

spark = SparkSession \
    .builder \
    .appName("antioxidants analysis") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# COMMAND ----------

dSchema = R([
            Fld("Product",Str()),
            Fld("origin",Str()),
            Fld("Procured_in",Str()),
            Fld("antioxidant_content_mmol_100g", Dbl())
            ])

# COMMAND ----------

#read in tables
dffruits = spark.read.csv("dbfs:/FileStore/tables/Fruit.csv", header=True, schema=dSchema)
dfvegetables = spark.read.csv("dbfs:/FileStore/tables/Vegetables.csv", header = True, schema=dSchema)
dfnuts = spark.read.csv("dbfs:/FileStore/tables/Nuts_and_seeds.csv", header = True, schema=dSchema)

# COMMAND ----------

dffruits = dffruits.withColumn('category', lit('Fruit'))
dffruits.show(5, truncate = True)
dffruits.count()

# COMMAND ----------

dfvegetables = dfvegetables.withColumn('category', lit('Vegetables'))
dfvegetables.show(5, truncate = True)
dfvegetables.count()

# COMMAND ----------

dfnuts = dfnuts.withColumn('category', lit('Nuts and Seeds'))
dfnuts.show(5, truncate = True)
dfnuts.count()

# COMMAND ----------

#union 3 tables
All = dffruits.unionAll(dfvegetables).unionAll(dfnuts)
All.show(5, truncate = True)
All.count()

# COMMAND ----------

#simplify product name
All = All.withColumn('productArray', split(col("Product"),",")).drop('Product')
All = All.withColumn('product_name', col('productArray')[0]).drop('productArray')
All.show(5)

# COMMAND ----------

anti_category = All.groupBy("category").agg(F.avg("antioxidant_content_mmol_100g").alias('avg_antioxidant')).orderBy(desc('avg_antioxidant'))
anti_category.show(truncate = False)

# COMMAND ----------

anti_product = All.groupBy("product_name").agg(F.avg("antioxidant_content_mmol_100g").alias('avg_antioxidant')).orderBy(desc('avg_antioxidant'))
anti_product.show(5, truncate = False)

# COMMAND ----------

#using Spark Sql to analyze the data
All.createOrReplaceTempView("All")     

# COMMAND ----------

#top 20 fruits high in antioxidants
spark.sql("""select distinct
             product_name,
             avg(antioxidant_content_mmol_100g) as avg_antioxidants
             from All
             where category = 'Fruit' and Procured_in = "USA"
             group by product_name
             order by avg_antioxidants desc
             limit 10
""").show(truncate = False)

# COMMAND ----------

#top 20 vegetables high in antioxidants
spark.sql("""select distinct
             product_name,
             avg(antioxidant_content_mmol_100g) as avg_antioxidants
             from All
             where category = 'Vegetables' and Procured_in = "USA"
             group by product_name
             order by avg_antioxidants desc
             limit 10
""").show(truncate = False)

# COMMAND ----------

#top 20 nuts high in antioxidants
spark.sql("""select distinct
             product_name,
             avg(antioxidant_content_mmol_100g) as avg_antioxidants
             from All
             where category = 'Nuts and Seeds' and Procured_in = "USA"
             group by product_name
             order by avg_antioxidants desc
             limit 10
""").show(truncate = False)

# COMMAND ----------

#import txt file with 25 selected food and their prices
dfprices = spark.read.option("header", "true") \
    .option("delimiter", "|") \
    .option("inferSchema", "true") \
    .csv("dbfs:/FileStore/tables/30_products_w_prices-1.txt")

dfprices.show(10, truncate=False)

# COMMAND ----------

dfprices.columns

# COMMAND ----------

#using Spark Sql to analyze the data
dfprices.createOrReplaceTempView("dfprices")     

# COMMAND ----------

spark.sql("""select distinct
             product,
             avg_antioxidant_mmol_100g as avg_antioxidants,
             price_per_pound as price_per_pound,
             avg_antioxidant_mmol_100g/0.22/price_per_pound as mmol_one_dollar
             from dfprices
             order by mmol_one_dollar desc
""").show(truncate = False)

# COMMAND ----------


