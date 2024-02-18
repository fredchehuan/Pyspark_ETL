#!/usr/bin/env python
# coding: utf-8

# # Importing Libraries

# In[5]:


import findspark                                              #Import library to Search for Spark Installation  

findspark.init()                                              #Search Spark Installation

import pyspark                                                #Only run after findspark.init()

from pyspark.sql import SparkSession                          # Import of Spark Session
from pyspark import SparkContext as spark                     # Import the Regular Spark Contex 
from pyspark.sql import SQLContext                            # Import the SQL Spark Contex 
from pyspark.sql.window import Window                         # Import Window methoed
from pyspark.sql.functions import *                           # Import all Functions
from pyspark.sql.types import *                               # Import all Types
spark = SparkSession.builder.getOrCreate()                    # Creating Session
from datetime import date                                     # Import date methoed

import os
import pandas as pd
import boto3
import time
from botocore.client import ClientError

import pyarrow.parquet as pq
import s3fs
import calendar
import json

month_ref = date.today().month                                # Creating Current Month Variable
year_ref = date.today().year                                  # Creating Current Year Variable

sc = spark.sparkContext                                       #Initialize Spark 

spark.conf.set("spark.sql.broadcastTimeout",  36000)          #Changing Broadcast Timeout
#conf = pyspark.SparkConf().setMaster("yarn-client").setAppName("sparK-mer")
#conf.set("spark.executor.heartbeatInterval","3600s")


# -----------------------------

# # Creating Variables

# ## Database

# In[ ]:


ATHENA_SPG = os.environ['SPG_DATABASE']


# In[ ]:


ATHENA_HANA = os.environ['GERDAU_HANA_DATABASE']


# In[ ]:


ATHENA_SALES = os.environ['GERDAU_SALES_DATABASE']


# ## Buckets

# In[ ]:


SPG_MANUAL_INPUT_BUCKET = os.environ['MANUAL_INPUT_BUCKET']


# In[ ]:


SPG_INTEGRATION_INPUT_BUCKET = os.environ['INTEGRATION_INPUT_BUCKET']


# In[ ]:


SPG_QUERY_BUCKET = os.environ['QUERY_BUCKET']


# In[ ]:


GERDAU_BUCKET = os.environ['GERDAU_BUCKET']


# In[ ]:


SPG_OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']


# ## Input Paths

# In[ ]:


# SPG Regions
SPG_INTEGRATION_INPUT_BUCKET_REGION = "SPG_DIMENSIONS/SPG_REGIOES/SPG_RG_REGIOES/SPG_RG_REGIOES.parquet"


# In[ ]:


# SPG Products
SPG_INTEGRATION_INPUT_BUCKET_PRODUCTS = "SPG_DIMENSIONS/SPG_PRODUTOS/SPG_PD_PRODUTOS.parquet"


# In[ ]:


# Alçada
SPG_INTEGRATION_INPUT_BUCKET_VERGE = "SPG_GLOBAL/SUPPORT/tb_SPG_SUPPORT_ALCADA.parquet"


# In[ ]:


SPG_QUERY_BUCKET_ATHENA = "SPG_QUERY/SPG_ATHENA"


# In[ ]:


# SPG SUPPORT SALES OFFICE
SPG_MANUAL_INPUT_BUCKET_SALES_OFFICE = "SPG_GLOBAL/INPUT/tb_SPG_SUPPORT_SALES_OFFICE.csv"


# In[ ]:


# SPG SUPPORT SALES ORDER
SPG_MANUAL_INPUT_BUCKET_SALES_TYPE = "SPG_GLOBAL/INPUT/tb_SPG_SUPPORT_SALES_ORDER_TYPE.csv"


# In[ ]:


# Conditions
GERDAU_BUCKET_OPEN_ORDER_INPUT_PATH = "global/lsa/adm/o2c/backlogopensales_smartpricing/report_openorder_pricing"


# In[ ]:


QUERY_WALLET = "SELECT * FROM "+ATHENA_HANA+".tb_o2c_backlogopsales_rpt_en_full_parquet inner join (SELECT DISTINCT max(dt_load) AS max_date FROM "+ATHENA_HANA+".tb_o2c_backlogopsales_rpt_en_full_parquet WHERE CustCountry LIKE '%Brazil%') ON dt_load = max_date WHERE CustCountry LIKE '%Brazil%'"


# In[ ]:


QUERY_VBAP = "SELECT DISTINCT vbeln as SALES_ORDER_NUMBER_drop, posnr as SALES_ORDER_ITEM_drop, cast(NETWR as decimal(19,6)) as RLV, cast(ZZICMS_IPI_T as decimal(19,6)) as RBV,cast(MWSBP as decimal(19,6)) as TOTAL_TAXES, cast(KWMENG as decimal(19,6)) as KWMENG,cast(UMVKZ as decimal(19,6)) as UMVKZ,cast(UMVKN as decimal(19,6)) as UMVKN FROM "+ATHENA_SALES+".tb_global_vbap_parquet"


# ## Output Paths

# In[ ]:


SPG_OUTPUT_BUCKET_WALLET = "SPG_WALLET/tb_SPG_WT_WALLET.parquet"


# ## Boto3 Variable

# In[ ]:


#S3 Configuration
S3_ATHENA_INPUT =  's3://'+SPG_QUERY_BUCKET+'/'+SPG_QUERY_BUCKET_ATHENA


# In[ ]:


S3_ATHENA_OUTPUT = 's3://'+SPG_QUERY_BUCKET+'/'+SPG_QUERY_BUCKET_ATHENA


# In[ ]:


region_name = os.environ['AWS_REGION']


# In[ ]:


aws_access_key_id = os.environ['AWS_ACCESS_KEY']


# In[ ]:


aws_secret_access_key = os.environ['AWS_SECRET_KEY']


# ------------

# # Creating Defined Functions

# In[ ]:


# Run Query

def run_query(query, database, s3_output):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
            },
        ResultConfiguration={
            'OutputLocation': s3_output,
            }
        )
    return response


# In[ ]:


def get_aws_path(query,database,s3_output):
    response = run_query(query, database, s3_output)
    file_query = response['QueryExecutionId']
    file_metadata = response['QueryExecutionId'] + '.metadata'
    return file_query


# In[ ]:


# Wating for 300 seconds until the end of the upload

def wait_athena_load(Bucket, Key):
    time_to_wait = 300
    time_counter = 0

    while True:
        try:
            s3.meta.client.head_object(Bucket=Bucket,Key=Key)
        except ClientError:
            time.sleep(1)
            time_counter += 1
            if time_counter > time_to_wait:
                break
        else:
            break


# In[ ]:


def remove_some_accents(col_name):
    removed_array = [('Á', 'A'), ('Ã', 'A'),('À', 'A'),('Â', 'A'),('Ä', 'A'),
                    ('É', 'E'),('È', 'E'),('Ê', 'E'),('Ë', 'E'),
                    ('Í', 'I'),('Ì', 'I'),('Î', 'I'),('Ï', 'I'),
                    ('Ó', 'O'),('Õ', 'O'), ('Ò', 'O'),('Ô', 'O'),('Ö', 'O'),
                    ('Ú', 'U'),('Ù', 'U'),('Û', 'U'),('Ü', 'U'),
                    ('Ç', 'C')]
    r = col_name
    for a, b in removed_array:
        r = regexp_replace(r, a, b)
    return r


# In[ ]:


# Replacing hifens by /
def remove_some_hifen(col_name):
    removed_chars = ("-")
    regexp = "|".join('\{0}'.format(i) for i in removed_chars)
    return regexp_replace(col_name, regexp, "/")


# In[ ]:


# Replacing dots by /
def remove_some_dots(col_name):
    removed_chars = (".")
    regexp = "|".join('\{0}'.format(i) for i in removed_chars)
    return regexp_replace(col_name, regexp, "/")


# In[ ]:


# Removing hifens
def replace_some_hifen(col_name):
    removed_chars = ("-")
    regexp = "|".join('\{0}'.format(i) for i in removed_chars)
    return regexp_replace(col_name, regexp, "")


# In[ ]:


# Removing spaces
def replace_some_space(col_name):
    removed_chars = (" ")
    regexp = "|".join('\{0}'.format(i) for i in removed_chars)
    return regexp_replace(col_name, regexp, "")


# In[ ]:


# Removing apostrophes
def replace_some_apostrophe(col_name):
    removed_chars = ("'")
    regexp = "|".join('\{0}'.format(i) for i in removed_chars)
    return regexp_replace(col_name, regexp, "")


# -------

# # Configuring Boto3

# In[ ]:


#Athena Client Configuration

client = boto3.client('athena', 
    aws_access_key_id = aws_access_key_id, 
    aws_secret_access_key = aws_secret_access_key, 
    region_name = region_name )


# In[ ]:


#S3 Resource Configuration

s3 = boto3.resource('s3',
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    region_name = region_name)


# # Importing Tables

# In[6]:


# SPG Regions
df_region = spark.read.parquet("s3a://"+SPG_INTEGRATION_INPUT_BUCKET+"/"+SPG_INTEGRATION_INPUT_BUCKET_REGION)


# In[7]:


df_region.write.partitionBy(144);
df_region = df_region.repartition(144);
df_region.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[8]:


# SPG Products
df_products = spark.read.parquet("s3a://"+SPG_INTEGRATION_INPUT_BUCKET+"/"+SPG_INTEGRATION_INPUT_BUCKET_PRODUCTS)


# In[9]:


df_products.write.partitionBy(144);
df_products = df_products.repartition(144);
df_products.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[16]:


# Alçada
df_alcada = spark.read.parquet("s3a://"+SPG_INTEGRATION_INPUT_BUCKET+"/"+SPG_INTEGRATION_INPUT_BUCKET_VERGE)


# In[17]:


df_alcada.write.partitionBy(144);
df_alcada = df_alcada.repartition(144);
df_alcada.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[20]:


# SPG SUPPORT SALES OFFICE
df_hana_office = spark.read.format("csv").option("header","true").option("sep",";").option("encoding", "ISO-8859-1").load("s3a://"+SPG_MANUAL_INPUT_BUCKET+"/"+SPG_MANUAL_INPUT_BUCKET_SALES_OFFICE)


# In[21]:


df_hana_office.write.partitionBy(144);
df_hana_office = df_hana_office.repartition(144);
df_hana_office.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[22]:


# SPG SUPPORT SALES ORDER
df_hana_sales_doc = spark.read.format("csv").option("header","true").option("sep",";").option("encoding", "ISO-8859-1").load("s3a://"+SPG_MANUAL_INPUT_BUCKET+"/"+SPG_MANUAL_INPUT_BUCKET_SALES_TYPE)


# In[23]:


df_hana_sales_doc.write.partitionBy(144);
df_hana_sales_doc = df_hana_sales_doc.repartition(144);
df_hana_sales_doc.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[24]:


# Conditions
df_conditions = spark.read.parquet("s3a://"+GERDAU_BUCKET+"/"+GERDAU_BUCKET_OPEN_ORDER_INPUT_PATH)


# In[25]:


df_conditions.write.partitionBy(144);
df_conditions = df_conditions.repartition(144);
df_conditions.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[26]:


# Import CSV from View

athena_response = get_aws_path(QUERY_WALLET,ATHENA_SPG,S3_ATHENA_OUTPUT)

wait_athena_load(SPG_QUERY_BUCKET, SPG_QUERY_BUCKET_ATHENA+"/"+athena_response+".csv")


# In[27]:


df_wallet = spark.read.csv("s3a://"+SPG_QUERY_BUCKET+"/"+SPG_QUERY_BUCKET_ATHENA+"/"+athena_response+".csv", header = 'true')


# In[28]:


df_wallet.write.partitionBy(144);
df_wallet = df_wallet.repartition(144);
df_wallet.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[29]:


# Import CSV from View

athena_response = get_aws_path(QUERY_VBAP,ATHENA_SPG,S3_ATHENA_OUTPUT)

wait_athena_load(SPG_QUERY_BUCKET, SPG_QUERY_BUCKET_ATHENA+"/"+athena_response+".csv")


# In[ ]:


# Import CSV from View

df_vbap = spark.read.csv("s3a://"+SPG_QUERY_BUCKET+"/"+SPG_QUERY_BUCKET_ATHENA+"/"+athena_response+".csv", header = 'true')


# In[ ]:


df_vbap.write.partitionBy(144);
df_vbap = df_vbap.repartition(144);
df_vbap.persist(pyspark.StorageLevel.MEMORY_ONLY)


# ------------------

# # Preparing Conditions Table

# In[36]:


df_conditions=df_conditions.select(df_conditions.VBAK_VBELN.alias("SALES_ORDER_NUMBER_drop")
                                    ,df_conditions.VBAP_POSNR.alias("SALES_ORDER_ITEM_drop")
                                    ,df_conditions.BX23.alias("IPI").cast("float")
                                    ,df_conditions.ICMI.alias("ICMI").cast("float")
                                    ,df_conditions.ZD38.alias("ZD38").cast("float")
                                    ,df_conditions.ZEB1.alias("ZEB1").cast("float")
                                    ,df_conditions.BX41.alias("BX41").cast("float"))\
             .dropDuplicates()


# ------------

# # Preparing Vbap Table

# In[37]:


df_vbap=df_vbap.withColumn("REQUESTED_QUANTITY", ((col("KWMENG")*col("UMVKZ"))/col("UMVKN")).cast("float"))


# In[38]:


df_vbap=df_vbap.drop("KWMENG", "UMVKZ", "UMVKN")              .dropDuplicates()


# --------------

# ## Preparing Alcada Table

# In[39]:


df_alcada=df_alcada.select(df_alcada.sales_org_cod.alias("SALES_ORG_COD")
                          ,df_alcada.material_cod.alias("MATERIAL_COD")
                          ,df_alcada.ibge_uf_acronyms.alias("ISSUING_STATE")
                          ,df_alcada.ALCADA.cast("float"))\
                    .dropDuplicates()


# ------

# # Preparing Regions Table

# In[40]:


# Normalizing the column IBGE_SAP_CITY_NAME
df_region=df_region.withColumn("UPPER_NAME", remove_some_accents(upper(replace_some_apostrophe(replace_some_space(replace_some_hifen(df_region["IBGE_SAP_CITY_NAME"]))))))


# In[41]:


# Selecting Necessary Columns
df_region=df_region.select(df_region.UPPER_NAME
                          ,df_region.BRANCH
                          ,df_region.IBGE_UF_ACRONYMS)


# ------------------

# # Preparing Products Table

# In[42]:


# Selecting Necessary Columns
df_products=df_products.select(df_products.DESC_GPD
                               ,df_products.COD_GPD
                               ,df_products.DESC_GPM.alias("GPM_DESC")
                               ,df_products.DESC_GPM.alias("GPM_COD")
                               ,df_products.COD_MATERIAL.alias("COD_MATERIAL_drop")
                               ,df_products.DESC_MATERIAL.alias("MATERIAL_DESC")
                               ,df_products.SALES_ORG_COD.alias("SALES_ORG_COD_drop"))


# ------------------

# # Preparing Support Table

# In[43]:


df_hana_office=df_hana_office.select(df_hana_office["Escritório de vendas"].alias("SALES_OFFICE_COD_drop")
                                    ,df_hana_office["Denominação"].alias("SALES_OFFICE_DESC"))\
                              .distinct()


# In[44]:


df_hana_sales_doc=df_hana_sales_doc.select(df_hana_sales_doc["Ordem Venda (Tipo) COD"].alias("ORDER_TYPE_COD_drop")
                                           ,df_hana_sales_doc["Ordem Venda (Tipo) DESC"].alias("ORDER_TYPE_DESC"))\
                                  .distinct()


# # Preparing Wallet Table

# In[45]:


# Filtering GAB Sales Organizations
df_wallet=df_wallet.filter(df_wallet.salesorg.like('%BRIN%') |
                           df_wallet.salesorg.like('%BRIO%') |
                           df_wallet.salesorg.like('%BRDI%') |
                           df_wallet.salesorg.like('%BRDO%') |
                           df_wallet.salesorg.like('%BRCC%') |
                           df_wallet.salesorg.like('%BRCO%') |
                           df_wallet.salesorg.like('%BRCG%') |
                           df_wallet.salesorg.like('%BRGO%'))\


# In[46]:


# Normalizing Date columns
for col_name in ["dt_load", "ordercreationdate", "confirmeddate"]:
    df_wallet = df_wallet.withColumn(col_name, from_unixtime(unix_timestamp(remove_some_dots(col_name), 'yyyy/MM/dd')))


# In[47]:


df_wallet.write.partitionBy(144);
df_wallet = df_wallet.repartition(144);


# In[48]:


df_wallet.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[49]:


# Renaming and selecting the columns
df_wallet = df_wallet.select(col("salesorg").alias("SALES_ORG_COD"),
                             col("custregionship").alias("ISSUING_STATE"),
                             col("customer").alias("CUSTOMER_COD"),
                             col("custname").alias("CUSTOMER_DESC"),
                             col("corporategroupname").alias("CUSTOMER_GROUP"),
                             col("item").alias("SALES_ORDER_ITEM"),
                             col("salesoffice").alias("SALES_OFFICE_COD"),
                             col("custcitynameship").alias("ISSUING_CITY"),
                             col("material").cast("int").alias("MATERIAL_COD"),
                             col("deliveredquantity").alias("DELIVERED_QUANTITY"),
                             col("materialgroup1").alias("GPD"),
                             col("pricedate").alias("PRICE_DATE"),
                             col("salesgroup").alias("SALES_GROUP_COD"),
                             col("reqqttconv").alias("REQ_QTT_CONV"),
                             col("quantitywdelivery").alias("QUANTITY_WITH_DELIVERY"),
                             col("extraperc").alias("AUTO_ADDITION_P100"),
                             col("zd01").alias("ZD01_%"),
                             col("zd02").alias("ZD02_P100"),
                             col("zd03").alias("ZD03_P100"),
                             col("zd04").alias("ZD04_P100"),
                             col("zd05").alias("ZD05_P100"),
                             col("zd06").alias("ZD06_P100"),
                             col("zp01baseprice").alias("ZP01"),
                             col("zd13").alias("ZD13_P100"),
                             col("zd14").alias("ZD14"),
                             col("manualextra").alias("ZSU2_P100"),
                             col("brfinanceextra").alias("ZEAF_P100"),
                             col("confirmeddate").alias("CONFIRMED_ORDER_DATE"),
                             col("ordercreationdate").alias("SALES_ORDER_DATE"),
                             col("goodsIssueDate").alias("ISSUING_DATE"),
                             col("statusoverall").alias("ORDER_STATUS"),
                             col("order").alias("SALES_ORDER_NUMBER"),
                             col("salesgroupdesc").alias("SALES_GROUP_DESC"),
                             col("ordertype").alias("ORDER_TYPE_COD"))


# In[50]:


# Correcting "Ouro Branco" Sales Org 
    # BRIO -> BRIN
    # BRCO -> BRCC
    # BRDO -> BRDI
    # BRGO -> BRCG
df_wallet=df_wallet.withColumn("SALES_ORG_COD", when(df_wallet.SALES_ORG_COD.like("BRCO"), "BRCC")                                                    .otherwise(when(df_wallet.SALES_ORG_COD.like("BRGO"), "BRCG")                                                              .otherwise(when(df_wallet.SALES_ORG_COD.like("BRIO"), "BRIN")                                                                        .otherwise(when(df_wallet.SALES_ORG_COD.like("BRDO"), "BRDI")
                                                                                  .otherwise(df_wallet.SALES_ORG_COD)))))\


# In[51]:


# Correcting Practiced Price Field
df_wallet=df_wallet.withColumn("CUSTOMER_GROUP", when(df_wallet.CUSTOMER_GROUP.isNull()|
                                                  df_wallet.CUSTOMER_GROUP.like("GRUPO DE RENTABILIDADE GENÉRICO")
                                                  ,col("CUSTOMER_DESC")).otherwise(col("CUSTOMER_GROUP")))\
                    .withColumn("BILLING_DATE", col("SALES_ORDER_DATE"))\


# In[52]:


# Creating Conditions Columns
df_wallet = df_wallet.join(df_conditions, (df_wallet["SALES_ORDER_NUMBER"].cast("int")==df_conditions["SALES_ORDER_NUMBER_drop"].cast("int"))&
                                            (df_wallet["SALES_ORDER_ITEM"].cast("int")==df_conditions["SALES_ORDER_ITEM_drop"].cast("int")), how='left')\
                     .drop("SALES_ORDER_NUMBER_drop")\
                     .drop("SALES_ORDER_ITEM_drop")\


# In[53]:


df_wallet.write.partitionBy(144);
df_wallet = df_wallet.repartition(144);


# In[54]:


df_wallet.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[55]:


# Converting numeric(and text) columns to float 
for col_name in ["DELIVERED_QUANTITY"
                 ,"REQ_QTT_CONV"
                 ,"AUTO_ADDITION_P100"
                 ,"ZD01_%"
                 ,"ZD02_P100"
                 ,"ZD03_P100"
                 ,"ZD04_P100"
                 ,"ZD05_P100"
                 ,"ZD06_P100"
                 ,"ZP01"
                 ,"ZD13_P100"
                 ,"ZD14"
                 ,"ZSU2_P100"
                 ,"ZEAF_P100"]:
    df_wallet = df_wallet.withColumn(col_name, col(col_name).cast('float'))


# In[56]:


# Getting GPM_DESC Field
df_wallet=df_wallet.join(df_products, (trim(df_products.SALES_ORG_COD_drop)==trim(df_wallet.SALES_ORG_COD))&
                                  (df_products.COD_MATERIAL_drop.cast("int")==df_wallet.MATERIAL_COD.cast("int")), how="left")\
                   .withColumnRenamed("DESC_GPD", "GPD_DESC")\
                   .withColumnRenamed("COD_GPD", "GPD_COD")\
                   .drop("COD_MATERIAL_drop")\
                   .drop("SALES_ORG_COD_drop")\


# In[57]:


df_wallet=df_wallet.withColumn("GPD_COD", when(df_wallet.GPD_COD.isNull(), col("GPD"))                               .otherwise(df_wallet.GPD_COD))


# In[58]:


df_wallet=df_wallet.drop("GPD")


# In[59]:


df_wallet.write.partitionBy(144);
df_wallet = df_wallet.repartition(144);


# In[60]:


df_wallet.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[61]:


# Creating "RLV" and TOTAL_TAXES Columns
df_wallet = df_wallet.join(df_vbap, (df_wallet["SALES_ORDER_NUMBER"].cast("int")==df_vbap["SALES_ORDER_NUMBER_drop"].cast("int"))&
                                    (df_wallet["SALES_ORDER_ITEM"].cast("int")==df_vbap["SALES_ORDER_ITEM_drop"].cast("int")), how='left')\
                     .drop("SALES_ORDER_NUMBER_drop")\
                     .drop("SALES_ORDER_ITEM_drop")\


# In[62]:


df_wallet.write.partitionBy(144);
df_wallet = df_wallet.repartition(144);


# In[63]:


df_wallet.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[64]:


# Converting numeric(and text) columns to float 
for col_name in ["AUTO_ADDITION_P100"
                 ,"ZD01_%"
                 ,"ZD02_P100"
                 ,"ZD03_P100"
                 ,"ZD04_P100"
                 ,"ZD05_P100"
                 ,"ZD06_P100"
                 ,"ZD13_P100"
                 ,"ZSU2_P100"
                 ,"ZEAF_P100"]:
    df_wallet = df_wallet.withColumn(col_name, ((col(col_name).cast('float')*(abs(((col("REQUESTED_QUANTITY"))/col("REQ_QTT_CONV")))))/100).cast('float'))


# In[65]:


df_wallet=df_wallet.withColumn("QUANTITY_TON", col("REQUESTED_QUANTITY"))


# In[66]:


df_wallet=df_wallet.withColumn("IPI", when(df_wallet["IPI"].isNull(), lit(0)).otherwise(df_wallet["IPI"]))


# In[67]:


df_wallet=df_wallet.withColumn("BX41", when(df_wallet["BX41"].isNull(), lit(0)).otherwise(df_wallet["BX41"]))


# In[68]:


df_wallet=df_wallet.withColumn("RBV", col("RBV")-col("BX41")-col("IPI"))                   .drop("IPI")


# In[69]:


df_wallet=df_wallet.withColumn("PRACTICED_PRICE", col("RBV").cast("float"))


# In[70]:


# Creating List Price
df_wallet=df_wallet.withColumn("LIST_PRICE", col("ICMI"))


# In[71]:


# ZD01 $ Valeu
df_wallet=df_wallet.withColumn("ZD01", col("LIST_PRICE")*(col("ZD01_%")))
df_wallet=df_wallet.withColumn("LIST_PRICE_ZD01", col("LIST_PRICE")+col("ZD01"))


# In[72]:


# ZD02 $ Valeu
df_wallet=df_wallet.withColumn("ZD02", col("LIST_PRICE_ZD01")*(col("ZD02_P100")))
df_wallet=df_wallet.withColumn("LIST_PRICE_ZD02", col("LIST_PRICE_ZD01")+col("ZD02"))


# In[73]:


# ZD03 $ Valeu
df_wallet=df_wallet.withColumn("ZD03", col("LIST_PRICE_ZD02")*(col("ZD03_P100")))
df_wallet=df_wallet.withColumn("LIST_PRICE_ZD03", col("LIST_PRICE_ZD02")+col("ZD03"))


# In[74]:


# ZD04 $ Valeu
df_wallet=df_wallet.withColumn("ZD04", col("LIST_PRICE_ZD03")*(col("ZD04_P100")))
df_wallet=df_wallet.withColumn("LIST_PRICE_ZD04", col("LIST_PRICE_ZD03")+col("ZD04"))


# In[75]:


# ZD05 $ Valeu
df_wallet=df_wallet.withColumn("ZD05", col("LIST_PRICE_ZD04")*(col("ZD05_P100")))
df_wallet=df_wallet.withColumn("LIST_PRICE_ZD05", col("LIST_PRICE_ZD04")+col("ZD05"))


# In[76]:


# ZD06 $ Valeu
df_wallet=df_wallet.withColumn("ZD06", col("LIST_PRICE_ZD05")*(col("ZD06_P100")))
df_wallet=df_wallet.withColumn("LIST_PRICE_ZD06", col("LIST_PRICE_ZD05")+col("ZD06"))


# In[77]:


# ZD38 $ Valeu
df_wallet=df_wallet.withColumn("LIST_PRICE_ZD38", col("LIST_PRICE_ZD06")+col("ZD38"))


# In[78]:


# ZBE1 $ Valeu
#df_wallet=df_wallet.withColumn("LIST_PRICE_ZBE1", col("LIST_PRICE_ZD38")+col("ZBE1"))


# In[79]:


# ZD13 $ Valeu
df_wallet=df_wallet.withColumn("ZD13", col("LIST_PRICE_ZD38")*(col("ZD13_P100")))
df_wallet=df_wallet.withColumn("LIST_PRICE_ZD13", col("LIST_PRICE_ZD38")+col("ZD13"))


# In[80]:


# ZD14 $ Valeu
df_wallet=df_wallet.withColumn("ZD14_P100", (col("ZD14")/col("LIST_PRICE_ZD13")).cast("float"))
df_wallet=df_wallet.withColumn("LIST_PRICE_ZD14", col("LIST_PRICE_ZD13")+col("ZD14"))


# In[81]:


# ZSU2 $ Valeu
df_wallet=df_wallet.withColumn("ZSU2", col("LIST_PRICE_ZD14")*(col("ZSU2_P100")))
df_wallet=df_wallet.withColumn("LIST_PRICE_ZSU2", col("LIST_PRICE_ZD14")+col("ZSU2"))


# In[82]:


# ZEAF $ Valeu
df_wallet=df_wallet.withColumn("ZEAF", col("LIST_PRICE_ZSU2")*(col("ZEAF_P100")))
df_wallet=df_wallet.withColumn("LIST_PRICE_ZEAF", col("LIST_PRICE_ZSU2")+col("ZEAF"))


# In[83]:


# Creating "PO" column
df_wallet=df_wallet.withColumn("PO", col("RBV")
                                     -abs(col("ZSU2")) 
                                     +abs(col("ZD13"))
                                     +abs(col("ZD38"))
                                     +abs(col("ZD06"))
                                     +abs(col("ZD14")))\


# In[84]:


# Creating "ALCADA" column
df_wallet=df_wallet.withColumn("ALCADA", (col("LIST_PRICE_ZD05")*0.02).cast('float'))


# In[85]:


# Creating "DESVIO_POL" column
df_wallet = df_wallet.withColumn("DESVIO_POL", lit(0))


# In[86]:


# Creating "DESVIO_POL_REAIS" column
df_wallet = df_wallet.withColumn("DESVIO_POL_REAIS", lit(0))


# In[87]:


df_wallet = df_wallet.filter(df_wallet.GPD_DESC.like('%BR-PERFIS ESTRUTURAIS%') |
                        df_wallet.GPD_DESC.like('%BR-CA50%') |
                        df_wallet.GPD_DESC.like('%BR-CA60%') |
                        df_wallet.GPD_DESC.like('%BR-RECOZIDO%') |
                        df_wallet.GPD_DESC.like('%BR-TEL TUBO%') |
                        df_wallet.GPD_DESC.like('%BR-TELA P/ CONCRETO%') |
                        df_wallet.GPD_DESC.like('%BR-CORTE E DOBR%') |
                        df_wallet.GPD_DESC.like('%BR-CONSTRUCAO CIVIL%') |
                        df_wallet.GPD_DESC.like('%BR-CA25%') |
                        df_wallet.GPD_DESC.like('%BR-PERFIS COMERCIAIS%') |
                        df_wallet.GPD_DESC.like('%BR-BTG%') |
                        df_wallet.GPD_DESC.like('%BR-MALHA POP%') |
                        df_wallet.GPD_DESC.like('%BR-TELA COLUNA%') |
                        df_wallet.GPD_DESC.like('%BR-TRELIÇA%') |
                        df_wallet.GPD_DESC.like('%BR-B.TREFILADA%') |
                        df_wallet.GPD_DESC.like('%BR-OVALADO%') |
                        df_wallet.GPD_DESC.like('%BR-GALVANIZADO%') |
                        df_wallet.GPD_DESC.like('%BR-BARRAS COMERCIAIS%') |
                        df_wallet.GPD_DESC.like('%BR-CAIXOTARIA%') |
                        df_wallet.GPD_DESC.like('%BR-ARTEFATOS%') |
                        df_wallet.GPD_DESC.like('%BR-FARPADO%') |
                        df_wallet.GPD_DESC.like('%BR-ELETRODO%') |
                        df_wallet.GPD_DESC.like('%BR-SOLDA - MIG%') |
                        df_wallet.GPD_DESC.like('%BR-CANTON A572%') |
                        df_wallet.GPD_DESC.like('%BR-GGS%') |
                        df_wallet.GPD_DESC.like('%BR-ARMADO%') |
                        df_wallet.GPD_DESC.like('%BR-CORDOALHA%') |
                        df_wallet.GPD_DESC.like('%BR-ESTACA PRANCHA%') |
                        df_wallet.GPD_DESC.like('%BR-ARAME PREGO%') |
                        df_wallet.GPD_DESC.like('%BR-CABEÇA DUPLA%') |
                        df_wallet.GPD_DESC.like('%BR-CORDOALHA AGRO%') |
                        df_wallet.GPD_DESC.like('%BR-GRAMPO%') |
                        df_wallet.GPD_DESC.like('%BR-COBREADOS%') |
                        df_wallet.GPD_DESC.like('%BR-CHAPA LQ%') |
                        df_wallet.GPD_DESC.like('%BR-UDC%') |
                        df_wallet.GPD_DESC.like('%BR-CHAPA ZN%') |
                        df_wallet.GPD_DESC.like('%BR-TELHA AZ%') |
                        df_wallet.GPD_DESC.like('%BR-TUBO ZN%') |
                        df_wallet.GPD_DESC.like('%BR-MARCENARIA%') |
                        df_wallet.GPD_DESC.like('%BR-PREGOES%') |
                        df_wallet.GPD_DESC.like('%BR-TELHEIRO%') |
                        df_wallet.GPD_DESC.like('%BR-COLUNA%') |
                        df_wallet.GPD_DESC.like('%BR-ESTRIBO%') |
                        df_wallet.GPD_DESC.like('%BR-ACESSORIOS%') |
                        df_wallet.GPD_DESC.like('%BR-CHAPA LCG%') |
                        df_wallet.GPD_DESC.like('%BR-CHAPA LF%') |
                        df_wallet.GPD_DESC.like('%BR-TUBO LF%') |
                        df_wallet.GPD_DESC.like('%BR-CHAPA LQ PISO%') |
                        df_wallet.GPD_DESC.like('%BR-BOBININHA%') |
                        df_wallet.GPD_DESC.like('%BR-ESPECIAIS%') |
                        df_wallet.GPD_DESC.like('%BR-BOBINA LQ%') |
                        df_wallet.GPD_DESC.like('%BR-FITA LQ%') |
                        df_wallet.GPD_DESC.like('%BR-BOBINA AZ%') |
                        df_wallet.GPD_DESC.like('%BR-AÇOS ESPECIAIS%') |
                        df_wallet.GPD_DESC.like('%BR-PARAFUSOS%') |
                        df_wallet.GPD_DESC.like('%BR-CIMENTO%') |
                        df_wallet.GPD_DESC.like('%BR-TUBO LQ%') |
                        df_wallet.GPD_DESC.like('%BR-TELHA ZN%') |
                        df_wallet.GPD_DESC.like('%BR-BTC GLV CP%') |
                        df_wallet.GPD_DESC.like('%BR-BOBINA ZN%') |
                        df_wallet.GPD_DESC.like('%BR-S-BARRA LAMINADA MÉDIA%') |
                        df_wallet.GPD_DESC.like('%BR-S-BARRA LAMINADA PESADA%') |
                        df_wallet.GPD_DESC.like('%BR-FITA LF%') |
                        df_wallet.GPD_DESC.like('%BR-FITA AZ%') |
                        df_wallet.GPD_DESC.like('%BR-S-BARRA LAMINADA LEVE%') |
                        df_wallet.GPD_DESC.like('%BR-ALAMBRADO%') |
                        df_wallet.GPD_DESC.like('%BR-SAPATA%') |
                        df_wallet.GPD_DESC.like('%BR-MOURÃO%') |
                        df_wallet.GPD_DESC.like('%BR-ATC CLARO IND%') |
                        df_wallet.GPD_DESC.like('%BR-POLIDO%') |
                        df_wallet.GPD_DESC.like('%BR-PERFIL BENEFICIADO%') |
                        df_wallet.GPD_DESC.like('%BR-BOBINA LF%') |
                        df_wallet.GPD_DESC.like('%BR-S-BARRA CTT%') |
                        df_wallet.GPD_DESC.like('%BR-CHAPA AZ%') |
                        df_wallet.GPD_DESC.like('%BR-FITA ZN%') |
                        df_wallet.GPD_DESC.like('%BR-LDA%') |
                        df_wallet.GPD_DESC.like('%BR-PIATINA CLARA%') |
                        df_wallet.GPD_DESC.like('%BR-S-BARRA DESCASCADA%') |
                        df_wallet.GPD_DESC.like('%BR-MESH%') |
                        df_wallet.GPD_DESC.like('%BR-S-BARRA FORJADA FINA%') |
                        df_wallet.GPD_DESC.like('%BR-S-BARRA RETIFICADA%') |
                        df_wallet.GPD_DESC.like('%BR-S-BARRA TREFILADA%') |
                        df_wallet.GPD_DESC.like('%BR-PLACA%') |
                        df_wallet.GPD_DESC.like('%BR-COIL%') |
                        df_wallet.GPD_DESC.like('%BR-S-FERRAMENTA%') |
                        df_wallet.GPD_DESC.like('%BR-S-FIO MÁQUINA STT%') |
                        df_wallet.GPD_DESC.like('%BR-ATC CLARO MOL COL%') |
                        df_wallet.GPD_DESC.like('%BR-ATC GLV CP%') |
                        df_wallet.GPD_DESC.like('%BR-B.CHATA LONGARINA%') |
                        df_wallet.GPD_DESC.like('%BR-BT FORJARIA BLOCOS%') |
                        df_wallet.GPD_DESC.like('%BR-TARUGO%') |
                        df_wallet.GPD_DESC.like('%BR-DRAWING%') |
                        df_wallet.GPD_DESC.like('%BR-ATC CLARO ENF ALG%') |
                        df_wallet.GPD_DESC.like('%BR-PIATINA GLV%') |
                        df_wallet.GPD_DESC.like('%BR-PERFIL DORMENTE%') |
                        df_wallet.GPD_DESC.like('%BR-ATC%') |
                        df_wallet.GPD_DESC.like('%BR-CHQ%') |
                        df_wallet.GPD_DESC.like('%BR-ALMA DE ELETRODO%') |
                        df_wallet.GPD_DESC.like('%BR-FIO MAQUINA%') |
                        df_wallet.GPD_DESC.like('%BR-CHQ BORO%') |
                        df_wallet.GPD_DESC.like('%BR-PERFIL GUIA ELEV%'))


# In[88]:


df_wallet = df_wallet.filter(df_wallet.GPM_DESC.like('%PERFIS_ESTRUTURAIS%') |
                        df_wallet.GPM_DESC.like('%VERGALHAO%') |
                        df_wallet.GPM_DESC.like('%AMPLIADOS%') |
                        df_wallet.GPM_DESC.like('%CORTE_DOBRA%') |
                        df_wallet.GPM_DESC.like('%PREGOS%') |
                        df_wallet.GPM_DESC.like('%B&P%') |
                        df_wallet.GPM_DESC.like('%ARAMES_AGRO%') |
                        df_wallet.GPM_DESC.like('%ARAMES_IND%') |
                        df_wallet.GPM_DESC.like('%PLANOS_LQ%') |
                        df_wallet.GPM_DESC.like('%PLANOS_REVESTIDOS%') |
                        df_wallet.GPM_DESC.like('%PLANOS_LF%') |
                        df_wallet.GPM_DESC.like('%PLANOS_LCG%') |
                        df_wallet.GPM_DESC.like('%FIO_MAQUINA%') |
                        df_wallet.GPM_DESC.like('%BT_FORJARIA%') |
                        df_wallet.GPM_DESC.like('%PLACAS%') |
                        df_wallet.GPM_DESC.like('%TARUGO%') | 
                        df_wallet.GPM_DESC.isNull())


# In[90]:


# Creating "BRANCH" Column
df_wallet = df_wallet.join(df_region, (remove_some_accents(upper(replace_some_hifen(replace_some_space(replace_some_apostrophe(df_wallet["ISSUING_CITY"]))))) == replace_some_hifen(replace_some_space(replace_some_apostrophe(df_region["UPPER_NAME"])))) &
                                      (trim(df_wallet["ISSUING_STATE"]) == trim(df_region["IBGE_UF_ACRONYMS"])), how='left')\
                     .drop("IBGE_UF_ACRONYMS")\
                     .drop("UPPER_NAME")\


# In[91]:


df_wallet.write.partitionBy(144);
df_wallet = df_wallet.repartition(144);


# In[92]:


df_wallet.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[93]:


df_wallet = df_wallet.withColumn("BRANCH", when(df_wallet.BRANCH.isNull(), lit("Filial Não Encontrada"))                                            .otherwise(df_wallet.BRANCH))                     .withColumn("MANUFACTURE", lit("Gerdau Carteira"))                     .withColumn("APPROVED", lit("None"))


# In[94]:


df_wallet = df_wallet.withColumn("TYPE_PRICE", when(df_wallet["SALES_ORG_COD"].like("%CC%")|
                                                    df_wallet["SALES_ORG_COD"].like("%IN%"),"Usina-Mercado")\
                                            .otherwise(when(df_wallet["SALES_ORG_COD"].like("%CG%"),"Distribuição-Mercado")\
                                                      .otherwise("Usina-Distribuição")))\


# In[95]:


df_wallet = df_wallet.withColumn("TYPE_PRICE_ADJUST", when(df_wallet["TYPE_PRICE"].like("%Usina-Mercado%"),"Sell-in 1: Usina-Construtora/Industria")                                                  .otherwise(when(df_wallet["TYPE_PRICE"].like("%Usina-Distribuição%"),"Sell-in 1: Usina-Distribuição")                                                             .otherwise(when(df_wallet["TYPE_PRICE"].like("%Distribuição-Mercado%"),"Sell-in 2: Distribuição-Revenda")                                                                        .otherwise("Sell Out: Revenda-Consumidor"))))                    .drop("TYPE_PRICE")


# In[96]:


# Creating "SALES_OFFICE_DESC" Column
df_wallet = df_wallet.join(df_hana_office, trim(df_wallet["SALES_OFFICE_COD"]) == trim(df_hana_office["SALES_OFFICE_COD_drop"]), how='left')                     .drop("SALES_OFFICE_COD_drop")


# In[97]:


df_wallet.write.partitionBy(144);
df_wallet = df_wallet.repartition(144);


# In[98]:


df_wallet.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[99]:


df_wallet = df_wallet.withColumn("SALES_OFFICE_DESC", when(df_wallet.SALES_OFFICE_DESC.isNull(), lit("Descricao_Escritorio_Nao_Encontrado")).otherwise(df_wallet.SALES_OFFICE_DESC))


# In[100]:


# Creating "ORDER_TYPE_DESC" Column
df_wallet = df_wallet.join(df_hana_sales_doc, trim(df_wallet["ORDER_TYPE_COD"])== trim(df_hana_sales_doc["ORDER_TYPE_COD_drop"]), how='left')                     .drop("ORDER_TYPE_COD_drop")


# In[101]:


df_wallet.write.partitionBy(144);
df_wallet = df_wallet.repartition(144);


# In[102]:


df_wallet.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[103]:


df_wallet = df_wallet.withColumn("ORDER_TYPE_DESC", when(df_wallet.ORDER_TYPE_DESC.isNull(), lit("Descricao_Order_Nao_Encontrado")).otherwise(df_wallet.ORDER_TYPE_DESC))


# In[104]:


# Creating "TOTAL_WALLET" column
df_wallet=df_wallet.withColumn("TOTAL_WALLET", (abs(col("REQUESTED_QUANTITY")) - abs(((col("DELIVERED_QUANTITY")*col("REQUESTED_QUANTITY"))/col("REQ_QTT_CONV")))).cast("float"))


# In[105]:


df_wallet = df_wallet.withColumn("RLV", ((col("RLV")/col("REQUESTED_QUANTITY"))*col("TOTAL_WALLET")))


# In[106]:


df_wallet = df_wallet.withColumnRenamed("ZD13_P100", "ZD13_%")


# In[107]:


df_wallet = df_wallet.withColumn("ZD_TOTAL_P", col("ZD06_P100")+col("ZD13_%")+col("ZD14_P100"))                     .withColumn("ZD_TOTAL_D", col("ZD06")+col("ZD13")+col("ZD14"))


# In[108]:


# Converting numeric(and text) columns to float 
for col_name in ["AUTO_ADDITION_P100"
                ,"ZD02_P100"
                ,"ZD03_P100"
                ,"ZD04_P100"
                ,"ZD05_P100"
                ,"ZD06_P100"
                ,"ZP01"
                ,"ZD14_P100"
                ,"ZSU2_P100"
                ,"ZEAF_P100"]: 
    df_wallet = df_wallet.drop(col_name)


# In[109]:


df_wallet =df_wallet.withColumn("PRECO_PRAT_NORM", (when(df_wallet.SALES_ORG_COD.like("%CG%"),(col("RBV") + 
                                                                                              abs(col("ZD05")) - 
                                                                                              abs(col("ZEAF")) + 
                                                                                              abs(col("BX41"))))\
                                                    .otherwise(col("RBV") + 
                                                               abs(col("ZD05")) - 
                                                               abs(col("ZEAF")))).cast("float"))


# In[110]:


df_wallet =df_wallet.withColumn("PRECO_PRAT_NORM_KG", (col("PRECO_PRAT_NORM")/col("REQUESTED_QUANTITY")))


# In[111]:


df_wallet =df_wallet.withColumn("PRECO_POLITICA", when(df_wallet.SALES_ORG_COD.like("%CG%"),(col("LIST_PRICE") 
                                                                                            - abs(col("ZD01")) 
                                                                                            - abs(col("ZD02")) 
                                                                                            - abs(col("ZD03"))
                                                                                            + abs(col("BX41")))
                                                                                            - abs(col("ALCADA")))\
                                                    .otherwise((col("LIST_PRICE") 
                                                                - abs(col("ZD01")) 
                                                                - abs(col("ZD02")) 
                                                                - abs(col("ZD03")))
                                                                - abs(col("ALCADA"))))\


# In[112]:


df_wallet=df_wallet.drop("ALCADA")


# In[113]:


df_wallet=df_wallet.join(df_alcada.select(df_alcada.SALES_ORG_COD.alias("SALES_ORG_COD_drop")
                                     ,df_alcada.MATERIAL_COD.alias("MATERIAL_COD_drop")
                                     ,df_alcada.ISSUING_STATE.alias("ISSUING_STATE_drop")
                                     ,(df_alcada.ALCADA/100).alias("ALCADA"))
                         ,(trim(df_wallet.SALES_ORG_COD) == trim(col("SALES_ORG_COD_drop"))) &
                          (df_wallet.MATERIAL_COD.cast("int") == col("MATERIAL_COD_drop").cast("int")) &
                          (trim(df_wallet.ISSUING_STATE) == trim(col("ISSUING_STATE_drop"))), how="left")\
                    .drop("SALES_ORG_COD_drop"
                         ,"MATERIAL_COD_drop"
                         ,"ISSUING_STATE_drop")


# In[114]:


df_wallet = df_wallet.fillna({'ALCADA':0.04})


# In[115]:


df_wallet =df_wallet.withColumn("PRECO_POL_KG", col("PRECO_POLITICA")/col("REQUESTED_QUANTITY"))


# In[116]:


# Converting numeric(and text) columns to float 
for col_name in ["ZD01_%"
                ,"ZD13_%"]:
    df_wallet = df_wallet.withColumn(col_name, (col(col_name)*100).cast('float'))


# In[117]:


# Converting numeric(and text) columns to float 
for col_name in ["PRECO_PRAT_NORM_KG"
                ,"PRECO_PRAT_NORM"
                ,"PRECO_POLITICA"
                ,"PRECO_POL_KG"
                ,"QUANTITY_TON"
                ,"REQUESTED_QUANTITY"
                ,"DELIVERED_QUANTITY"
                ,"QUANTITY_WITH_DELIVERY"
                ,"ALCADA"
                ,"PRACTICED_PRICE"
                ,"RBV"
                ,"ZEB1"
                ,"ZD01_%"
                ,"LIST_PRICE"
                ,"ZD01"
                ,"LIST_PRICE_ZD01"
                ,"ZD02"
                ,"LIST_PRICE_ZD02"
                ,"ZD03"
                ,"LIST_PRICE_ZD03"
                ,"ZD04"
                ,"LIST_PRICE_ZD04"
                ,"ZD05"
                ,"LIST_PRICE_ZD05"
                ,"ZD06"
                ,"LIST_PRICE_ZD06"
                ,"ZD13_%"
                ,"ZD13"
                ,"LIST_PRICE_ZD13"
                ,"ZD14"
                ,"LIST_PRICE_ZD14"
                ,"ZSU2"
                ,"LIST_PRICE_ZSU2"
                ,"ZEAF"
                ,"BX41"
                ,"LIST_PRICE_ZEAF"
                ,"PO"
                ,"TOTAL_WALLET"
                ,"REQ_QTT_CONV"
                ,"DESVIO_POL"
                ,"DESVIO_POL_REAIS"
                ,"RLV"
                ,"TOTAL_TAXES"
                ,"ZD_TOTAL_D"
                ,"ZD_TOTAL_P"]:
    df_wallet = df_wallet.withColumn(col_name, abs(round(col(col_name),4)).cast('float'))


# In[119]:


#Rename Columns 
df_wallet = df_wallet.withColumnRenamed("PRECO_PRAT_NORM","NORM_PRACTICED_PRICE")                    .withColumnRenamed("PRECO_PRAT_NORM_KG","NORM_PRACTICED_PRICE_KG")                    .withColumnRenamed("PRECO_POLITICA","POLICY_PRICE")                    .withColumnRenamed("PRECO_POL_KG","POLICY_PRICE_KG")                    .withColumnRenamed("ALCADA","VERGE")                    .withColumnRenamed("DESVIO_POL","POL_DEVIATION")                    .withColumnRenamed("DESVIO_POL_REAIS","POL_DEVIATION_REAIS")


# In[121]:


df_wallet=df_wallet.dropDuplicates()


# In[122]:


df_wallet.write.partitionBy(144);
df_wallet = df_wallet.repartition(144);


# In[123]:


df_wallet.persist(pyspark.StorageLevel.DISK_ONLY)


# In[124]:


df_wallet.coalesce(144).write.parquet("s3a://"+SPG_OUTPUT_BUCKET+"/"+SPG_OUTPUT_BUCKET_WALLET, mode = "overwrite")


# In[ ]:




