#!/usr/bin/env python
# coding: utf-8

# In[548]:


import findspark                                              #Import library to Search for Spark Installation  

findspark.init()                                              #Search Spark Installation

import pyspark 
#Only run after findspark.init()

from pyspark.sql import SparkSession                          # Import of Spark Session
from pyspark import SparkContext as spark                     # Import the Regular Spark Contex 
from pyspark.sql import SQLContext                            # Import the SQL Spark Contex 
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import pyspark.sql.functions as F
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.types import IntegerType

import os

sc = spark.sparkContext                                       #Initialize Spark 

spark.conf.set("spark.sql.broadcastTimeout",  1200)
       
import gc                                                     #Importing Garbage Collecting Libraries


# -----------------

# # Creating Variables

# ## Buckets

# In[ ]:


EY_BUCKET = os.environ['EY_BUCKET']


# In[ ]:


GERDAU_BUCKET = os.environ['GERDAU_BUCKET']


# ## Input Paths

# In[ ]:


GERDAU_BUCKET_TSPAT_INPUT_PATH = "global/masterdata/tspat"


# In[ ]:


GERDAU_BUCKET_T179T_INPUT_PATH = "global/masterdata/t179t"


# In[ ]:


GERDAU_BUCKET_MARA_INPUT_PATH = "global/masterdata/tb_global_mara_parquet"


# In[ ]:


GERDAU_BUCKET_MAKT_INPUT_PATH = "global/masterdata/makt"


# In[ ]:


GERDAU_BUCKET_MARM_INPUT_PATH = "global/masterdata/marm"


# In[ ]:


GERDAU_BUCKET_MVKE_INPUT_PATH = "global/masterdata/mvke/"


# In[ ]:


GERDAU_BUCKET_TVM1T_INPUT_PATH = "global/sales/tvm1t/"


# In[ ]:


GERDAU_BUCKET_MATERIAL_CTBW_INPUT_PATH = "global/masterdata/material_ctbw/"


# ## Output Paths

# In[ ]:


EY_BUCKET_PRODUCTS_OUTPUT_PATH = "SPG_DIMENSIONS/SPG_PRODUTOS/SPG_PD_PRODUTOS.parquet"


# -------------

# # Creating Defined Funcitions

# In[ ]:


def normalizing_name(DESC_MATERIAL):
    removed_array = [('--', ''),('---', '')]
    r = 'DESC_MATERIAL'
    for a, b in removed_array:
        r = regexp_replace(r, a, b)
    return r 


# -------------

# # Importando Dados

# In[549]:


df_tspat = spark.read.parquet("s3a://"+GERDAU_BUCKET+"/"+GERDAU_BUCKET_TSPAT_INPUT_PATH)


# In[550]:


df_tspat.write.partitionBy(144)
df_tspat = df_tspat.repartition(144)
df_tspat.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[551]:


df_t179t = spark.read.parquet("s3a://"+GERDAU_BUCKET+"/"+GERDAU_BUCKET_T179T_INPUT_PATH)


# In[552]:


df_t179t.write.partitionBy(144)
df_t179t = df_t179t.repartition(144)
df_t179t.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[553]:


df_mara = spark.read.parquet("s3a://"+GERDAU_BUCKET+"/"+GERDAU_BUCKET_MARA_INPUT_PATH)


# In[554]:


df_mara.write.partitionBy(144)
df_mara = df_mara.repartition(144)
df_mara.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[555]:


df_makt = spark.read.parquet("s3a://"+GERDAU_BUCKET+"/"+GERDAU_BUCKET_MAKT_INPUT_PATH)


# In[556]:


df_makt.write.partitionBy(144)
df_makt = df_makt.repartition(144)
df_makt.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[557]:


df_marm = spark.read.parquet("s3a://"+GERDAU_BUCKET+"/"+GERDAU_BUCKET_MARM_INPUT_PATH)


# In[558]:


df_marm.write.partitionBy(144)
df_marm = df_marm.repartition(144)
df_marm.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[559]:


df_mvke = spark.read.parquet("s3a://"+GERDAU_BUCKET+"/"+GERDAU_BUCKET_MVKE_INPUT_PATH)


# In[560]:


df_mvke.write.partitionBy(144)
df_mvke = df_mvke.repartition(144)
df_mvke.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[561]:


df_tvm1t = spark.read.parquet("s3a://"+GERDAU_BUCKET+"/"+GERDAU_BUCKET_TVM1T_INPUT_PATH)


# In[562]:


df_tvm1t.write.partitionBy(144)
df_tvm1t = df_tvm1t.repartition(144)
df_tvm1t.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[563]:


df_material_ctbw = spark.read.parquet("s3a://"+GERDAU_BUCKET+"/"+GERDAU_BUCKET_MATERIAL_CTBW_INPUT_PATH)


# In[564]:


df_material_ctbw.write.partitionBy(144)
df_material_ctbw = df_material_ctbw.repartition(144)
df_material_ctbw.persist(pyspark.StorageLevel.MEMORY_ONLY)


# # Selecionando Colunas:
# 
# Selecionando apenas as colunas necessárias:
# 

# In[565]:


df_makt=df_makt.select('matnr','spras','maktx','maktg').orderBy('matnr')
df_tspat=df_tspat.select('spras','spart','vtext')
df_tvm1t=df_tvm1t.select('spras','mvgr1','bezei')
df_t179t=df_t179t.select('spras','prodh','vtext')
df_mara=df_mara.select('MATNR','spart','prdha','mtart','mstae').orderBy('MATNR')
df_marm=df_marm.select('matnr','meinh','umrez','umren').orderBy('matnr')
df_mvke=df_mvke.select('matnr','mvgr1', 'vkorg').orderBy('matnr')
df_material_ctbw=df_material_ctbw.orderBy('MATNR')


# # Filtrando dados
# 
# O filtro de linguagem de país serve para pegar os valores de produtos apenas com a Linguagem Português, ou seja, Brasil.

# In[566]:


df_makt=df_makt.filter(F.col('spras').like("%P%"))
df_tspat=df_tspat.filter(df_tspat.spras.like('%P%'))
df_tvm1t=df_tvm1t.filter(df_tvm1t.spras.like('%P%'))
df_t179t=df_t179t.filter(df_t179t.spras.like('%P%'))
df_mara=df_mara.filter(df_mara.mtart.like('%ZERT%')).withColumn("mtart", trim(col("mtart")))


# In[567]:


df_makt=df_makt.withColumn("maktx", trim(df_makt.maktx)).distinct()


# In[568]:


#Applying filter - Organization Sales = Brazil
df_mvke = df_mvke.where(df_mvke.vkorg.like('%BRIN%')| 
                        df_mvke.vkorg.like('%BRIO%')| 
                        df_mvke.vkorg.like('%BRDI%')| 
                        df_mvke.vkorg.like('%BRDO%')| 
                        df_mvke.vkorg.like('%BRCC%')|
                        df_mvke.vkorg.like('%BRCO%')|
                        df_mvke.vkorg.like('%BRCG%')|
                        df_mvke.vkorg.like('%BRGO%')|
                        df_mvke.vkorg.like('%BRTR%')|
                        df_mvke.vkorg.like('%BRTO%'))


# # Criando as Hierarquias
# 
# A tabela t179t possui a descrição das hierarquias, porém sem definição de tipo de hierarquia.
# 
# Para conseguir encaixar ele na base de produtos será necessário dividir esta tabela por hierarquia.

# In[569]:


df_t179t=df_t179t.withColumn('Num',F.length('prodh'))


# In[570]:


hierarquia1=df_t179t.where(col('Num')=="1").cache()    #Hierarquia 1
hierarquia2=df_t179t.where(col('Num')=="3").cache()    #Hierarquia 2
hierarquia3=df_t179t.where(col('Num')=="5").cache()    #Hierarquia 3
hierarquia4=df_t179t.where(col('Num')=="8").cache()    #Hierarquia 4
hierarquia5=df_t179t.where(col('Num')=="11").cache()   #Hierarquia 5
hierarquia6=df_t179t.where(col('Num')=="14").cache()   #Hierarquia 6
hierarquia7=df_t179t.where(col('Num')=="18").cache()   #Hierarquia 7


# In[571]:


hierarquia1=hierarquia1.selectExpr('prodh as prodh_1','vtext as Hierarquia_1_DESC').cache()
hierarquia2=hierarquia2.selectExpr('prodh as prodh_2','vtext as Hierarquia_2_DESC').cache()
hierarquia3=hierarquia3.selectExpr('prodh as prodh_3','vtext as Hierarquia_3_DESC').cache()
hierarquia4=hierarquia4.selectExpr('prodh as prodh_4','vtext as Hierarquia_4_DESC').cache()
hierarquia5=hierarquia5.selectExpr('prodh as prodh_5','vtext as Hierarquia_5_DESC').cache()
hierarquia6=hierarquia6.selectExpr('prodh as prodh_6','vtext as Hierarquia_6_DESC').cache()
hierarquia7=hierarquia7.selectExpr('prodh as prodh_7','vtext as Hierarquia_7_DESC').cache()


# In[572]:


HierarquiaFinal=df_mara.select('prdha').distinct().orderBy('prdha')


# In[573]:


HierarquiaFinal=HierarquiaFinal.withColumn("Hierarquia_1",substring(HierarquiaFinal["prdha"],0,1))
HierarquiaFinal=HierarquiaFinal.withColumn("Hierarquia_2",substring(HierarquiaFinal["prdha"],0,3))
HierarquiaFinal=HierarquiaFinal.withColumn("Hierarquia_3",substring(HierarquiaFinal["prdha"],0,5))
HierarquiaFinal=HierarquiaFinal.withColumn("Hierarquia_4",substring(HierarquiaFinal["prdha"],0,8))
HierarquiaFinal=HierarquiaFinal.withColumn("Hierarquia_5",substring(HierarquiaFinal["prdha"],0,11))
HierarquiaFinal=HierarquiaFinal.withColumn("Hierarquia_6",substring(HierarquiaFinal["prdha"],0,14))
HierarquiaFinal=HierarquiaFinal.withColumn("Hierarquia_7",substring(HierarquiaFinal["prdha"],0,18))


# In[574]:


Hierarquias=HierarquiaFinal.join(hierarquia1,HierarquiaFinal.Hierarquia_1==hierarquia1.prodh_1, how='left')


# In[575]:


Hierarquias=Hierarquias.join(hierarquia2,Hierarquias.Hierarquia_2==hierarquia2.prodh_2, how='left')


# In[576]:


Hierarquias=Hierarquias.join(hierarquia3,Hierarquias.Hierarquia_3==hierarquia3.prodh_3, how='left')


# In[577]:


Hierarquias=Hierarquias.join(hierarquia4,Hierarquias.Hierarquia_4==hierarquia4.prodh_4, how='left')


# In[578]:


Hierarquias=Hierarquias.join(hierarquia5,Hierarquias.Hierarquia_5==hierarquia5.prodh_5, how='left')


# In[579]:


Hierarquias=Hierarquias.join(hierarquia6,Hierarquias.Hierarquia_6==hierarquia6.prodh_6, how='left')


# In[580]:


Hierarquias=Hierarquias.join(hierarquia7,Hierarquias.Hierarquia_7==hierarquia7.prodh_7, how='left')


# In[581]:


Hierarquias=Hierarquias.selectExpr('prdha as prdha_hierarquia'
                                   ,'Hierarquia_1 as COD_HIERARCHY_1'
                                   ,'Hierarquia_1_DESC as DESC_HIERARCHY_1'
                                   ,'Hierarquia_2 as COD_HIERARCHY_2'
                                   ,'Hierarquia_2_DESC as DESC_HIERARCHY_2'
                                   ,'Hierarquia_3 as COD_HIERARCHY_3'
                                   ,'Hierarquia_3_DESC as DESC_HIERARCHY_3'
                                   ,'Hierarquia_4 as COD_HIERARCHY_4'
                                   ,'Hierarquia_4_DESC as DESC_HIERARCHY_4'
                                   ,'Hierarquia_5 as COD_HIERARCHY_5'
                                   ,'Hierarquia_5_DESC as DESC_HIERARCHY_5'
                                   ,'Hierarquia_6 as COD_HIERARCHY_6'
                                   ,'Hierarquia_6_DESC as DESC_HIERARCHY_6'
                                   ,'Hierarquia_7 as COD_HIERARCHY_7'
                                   ,'Hierarquia_7_DESC as DESC_HIERARCHY_7')


# # Transformando as unidades de medidas em colunas no MARM
# 
# O MARM possui as informações de unidade de medida, porém, quando existem mais de uma unidade de medida o material é repetido.

# In[582]:


df_marm=df_marm.selectExpr('matnr as matnr_marm','meinh','umrez','umren')


# In[583]:


df_marm=df_marm.withColumn('Valor',df_marm.umrez/df_marm.umren)


# In[584]:


marm_ZERT=df_mara.join(df_marm,df_mara.MATNR.cast("int")==df_marm.matnr_marm.cast("int"),how='left').distinct().cache()


# In[585]:


marm_ZERT=marm_ZERT.groupBy('matnr_marm').pivot('meinh').avg('valor')


# In[586]:


marm_ZERT=marm_ZERT.na.fill(0)


# # Joins

# ### Join entre MAKT e MARA

# In[587]:


df_makt=df_makt.selectExpr('matnr as matnr_makt'
                           ,'spras as spras'
                           ,'maktx as DESC_MATERIAL'
                           ,'maktg as maktg')


# In[588]:


df_mara=df_mara.selectExpr('MATNR as COD_MATERIAL'
                           ,'spart as COD_ACTIVITY_SECTOR'
                           ,'prdha as prdha'
                           ,'mtart as mtart'
                           ,'mstae as MARKED_DELETION')


# In[589]:


SPG_PD_PRODUTOS = df_mara.join(df_makt, df_mara.COD_MATERIAL.cast("int") == df_makt.matnr_makt.cast("int"), how='left').distinct()


# In[590]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.withColumn("MARKED_DELETION", trim(col("MARKED_DELETION")))


# In[591]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.filter(SPG_PD_PRODUTOS.MARKED_DELETION.isNull()|
                                       SPG_PD_PRODUTOS.MARKED_DELETION.like(' %')|
                                       SPG_PD_PRODUTOS.MARKED_DELETION.like('% ')|
                                       SPG_PD_PRODUTOS.MARKED_DELETION.like('')).dropDuplicates()


# ### Join Entre MARAMAKT e TSPAT

# In[594]:


df_tspat=df_tspat.selectExpr('spras as spras_tspat','spart as spart_tspat','vtext as DESC_ACTIVITY_SECTOR')


# In[595]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.join(df_tspat,SPG_PD_PRODUTOS.COD_ACTIVITY_SECTOR==df_tspat.spart_tspat, how='left').distinct()


# ### Join entre MARAMAKTTSPAT e MVKE

# In[598]:


df_mvke=df_mvke.selectExpr('matnr as matnr_mvke','mvgr1 as COD_GPD', 'vkorg as SALES_ORG_COD')


# In[599]:


df_mvke=df_mvke.where(df_mvke.COD_GPD.isNotNull())


# In[600]:


df_mvke=df_mvke.groupBy('matnr_mvke','COD_GPD', 'SALES_ORG_COD').agg({'COD_GPD':'count'})


# In[601]:


df_mvke=df_mvke.withColumnRenamed("count(COD_GPD)", "count_gpd")


# In[602]:


df_mvke.groupBy('matnr_mvke','COD_GPD', 'SALES_ORG_COD').agg(F.max('count_gpd'))


# In[603]:


df_mvke=df_mvke.drop("count_gpd")


# In[604]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.filter(~(SPG_PD_PRODUTOS.DESC_MATERIAL.like("%DELET%") |
                                         SPG_PD_PRODUTOS.DESC_MATERIAL.like("%DO NOT%") |
                                         SPG_PD_PRODUTOS.DESC_MATERIAL.like("%UTILIZAR%")))


# In[605]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.join(df_mvke,SPG_PD_PRODUTOS.COD_MATERIAL.cast("int")==df_mvke.matnr_mvke.cast("int"), how='left').distinct()


# In[607]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.drop("matnr_mvke")


# ### Join entre MARAMAKTTSPATMVKE com TVM1T

# In[609]:


df_tvm1t=df_tvm1t.selectExpr('spras as spras_tvm1t','mvgr1 as mvgr1_tvm1t','bezei as DESC_GPD')


# In[610]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.join(df_tvm1t,SPG_PD_PRODUTOS.COD_GPD==df_tvm1t.mvgr1_tvm1t, how='left').distinct()


# ### Join entre MARAMAKTTSPATMVKETVM1T e MARM 

# In[613]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.join(marm_ZERT,SPG_PD_PRODUTOS.COD_MATERIAL.cast("int")==marm_ZERT.matnr_marm.cast("int"),how='left')                                .distinct()


# ### Join entre MARAMAKTTSPATMVKETVM1T e Hierarquias.

# In[616]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.withColumn('Numero_prodh', F.length('prdha'))


# In[617]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.join(Hierarquias,SPG_PD_PRODUTOS.prdha==Hierarquias.prdha_hierarquia,how='left')                                .distinct()


# In[620]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.drop('spras'
                                    ,'maktg'
                                    ,'matnr_makt'
                                    ,'spras_tspat'
                                    ,'spart_tspat'
                                    ,'spras_tvm1t'
                                    ,'mvgr1_tvm1t'
                                    ,'matnr_marm'
                                    ,'prdha_hierarquia'
                                    ,'mtart'
                                    ,'prdha'
                                    ,'Numero_prodh')
# Drop columns


# ### Join Class 300

# In[621]:


df_material_ctbw_gpm=df_material_ctbw.selectExpr('MATNR as Material_COD_DROP', 'MARKETINGPRODUCTGROUP as COD_GPM')


# In[622]:


df_material_ctbw_gpm=df_material_ctbw_gpm.where(df_material_ctbw_gpm.COD_GPM.isNotNull())


# In[623]:


df_material_ctbw_gpm=df_material_ctbw_gpm.groupBy('Material_COD_DROP','COD_GPM').agg({'COD_GPM':'count'})


# In[624]:


df_material_ctbw_gpm=df_material_ctbw_gpm.withColumnRenamed("count(COD_GPM)", "count_gpm")


# In[625]:


df_material_ctbw_gpm.groupBy('Material_COD_DROP','COD_GPM').agg(F.max('count_gpm'))


# In[626]:


df_material_ctbw_gpm=df_material_ctbw_gpm.drop("count_gpm")


# ---------------

# In[627]:


df_material_ctbw_gdd=df_material_ctbw.selectExpr('MATNR as Material_COD','DEMANDBREAKDOWNGROUP as DESC_GDD')


# In[628]:


df_material_ctbw_gdd=df_material_ctbw_gdd.where(df_material_ctbw_gdd.DESC_GDD.isNotNull())


# In[629]:


df_material_ctbw_gdd=df_material_ctbw_gdd.groupBy('Material_COD','DESC_GDD').agg({'DESC_GDD':'count'})


# In[630]:


df_material_ctbw_gdd=df_material_ctbw_gdd.withColumnRenamed("count(DESC_GDD)", "count_gdd")


# In[631]:


df_material_ctbw_gdd.groupBy('Material_COD','DESC_GDD').agg(F.max('count_gdd'))


# In[632]:


df_material_ctbw_gdd=df_material_ctbw_gdd.drop("count_gdd")


# In[633]:


df_material_ctbw=df_material_ctbw_gdd.join(df_material_ctbw_gpm, df_material_ctbw_gdd.Material_COD.cast("int") == df_material_ctbw_gpm.Material_COD_DROP.cast("int"), how='inner')


# In[634]:


df_material_ctbw=df_material_ctbw.drop("Material_COD_DROP")


# In[635]:


SPG_PD_PRODUTOS=df_material_ctbw.join(SPG_PD_PRODUTOS, SPG_PD_PRODUTOS.COD_MATERIAL.cast("int") == df_material_ctbw.Material_COD.cast("int"),how='left')


# In[637]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.drop('Material_COD')


# In[639]:


SPG_PD_PRODUTOS = SPG_PD_PRODUTOS.filter((SPG_PD_PRODUTOS.SALES_ORG_COD.like('%BRIN%'))|
                                        (SPG_PD_PRODUTOS.SALES_ORG_COD.like('%BRIO%'))| 
                                        (SPG_PD_PRODUTOS.SALES_ORG_COD.like('%BRDI%'))| 
                                        (SPG_PD_PRODUTOS.SALES_ORG_COD.like('%BRDO%'))| 
                                        (SPG_PD_PRODUTOS.SALES_ORG_COD.like('%BRCC%'))|
                                        (SPG_PD_PRODUTOS.SALES_ORG_COD.like('%BRCO%'))|
                                        (SPG_PD_PRODUTOS.SALES_ORG_COD.like('%BRCG%'))|
                                        (SPG_PD_PRODUTOS.SALES_ORG_COD.like('%BRGO%'))|
                                        (SPG_PD_PRODUTOS.SALES_ORG_COD.like('%BRTR%'))|
                                        (SPG_PD_PRODUTOS.SALES_ORG_COD.like('%BRTO%')))


# In[641]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.withColumn('DESC_MATERIAL', trim(normalizing_name(col('DESC_MATERIAL'))))                                .dropDuplicates()


# In[646]:


SPG_PD_PRODUTOS=SPG_PD_PRODUTOS.withColumn('COD_GPM', trim(col('COD_GPM')))


# ## Gerar Arquivos

# ### Gerar Arquivos de ETL

# In[651]:


SPG_PD_PRODUTOS.write.partitionBy(144)
SPG_PD_PRODUTOS = SPG_PD_PRODUTOS.repartition(144)
SPG_PD_PRODUTOS.persist(pyspark.StorageLevel.MEMORY_ONLY)


# In[658]:


SPG_PD_PRODUTOS.write.parquet("s3a://"+EY_BUCKET+"/"+EY_BUCKET_PRODUCTS_OUTPUT_PATH, mode = "overwrite")

