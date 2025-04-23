# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "706dc789-a524-424c-8dc3-1ec4ec5f4e1a",
# META       "default_lakehouse_name": "OmniSync_DE_LH_320_Gold_Contoso",
# META       "default_lakehouse_workspace_id": "6b35ae7a-875a-4e1a-8f60-9f0a22d0e0d0",
# META       "known_lakehouses": []
# META     },
# META     "environment": {
# META       "environmentId": "4d7e0d58-dbbd-aef9-4745-975b73f3f167",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.ExternalCDC LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#load rows
from datetime import datetime
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark.conf.set('spark.sql.caseSensitive', True)

externalCDCSchema = StructType().add("Operation", "string") \
                                .add("Entity", "string") \
                                .add("Values", "string") \
                                .add("CreatedDate", "timestamp") \
                                .add("UpdatedDate", "timestamp")
                                
currency1= json.dumps({ "CurrencyKey": "", "CurrencyName": "EURO2", "CurrencyDescription": "Euro 2"})
staged_rows = [('Insert','Currency', currency1, datetime.now(), None)]
#create dataframe and append current datetime
staged_df = spark.createDataFrame(staged_rows,externalCDCSchema) \
            .write.mode("append").format("delta").saveAsTable("ExternalCDC")

print(staged_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#load rows
from datetime import datetime
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark.conf.set('spark.sql.caseSensitive', True)

externalCDCSchema = StructType().add("Operation", "string") \
                                .add("Entity", "string") \
                                .add("Values", "string") \
                                .add("CreatedDate", "timestamp") \
                                .add("UpdatedDate", "timestamp")
                                
currency1= json.dumps({ "CurrencyKey": "", "CurrencyName": "EURO2", "CurrencyDescription": "Euro 2"})
staged_rows = [('Delete','Currency', currency1, datetime.now(), datetime.now())]
#create dataframe and append current datetime
staged_df = spark.createDataFrame(staged_rows,externalCDCSchema) \
            .write.mode("append").format("delta").saveAsTable("ExternalCDC")

print(staged_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#load rows
from datetime import datetime
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark.conf.set('spark.sql.caseSensitive', True)

externalCDCSchema = StructType().add("Operation", "string") \
                                .add("Entity", "string") \
                                .add("Values", "string") \
                                .add("CreatedDate", "timestamp") \
                                .add("UpdatedDate", "timestamp")
                                
currency1= json.dumps({ "CurrencyKey": "", "CurrencyName": "EUR", "CurrencyDescription": "Euro xxxxxxxxxxx"})
staged_rows = [('Update','Currency', currency1, datetime.now(), datetime.now())]
#create dataframe and append current datetime
staged_df = spark.createDataFrame(staged_rows,externalCDCSchema) \
            .write.mode("append").format("delta").saveAsTable("ExternalCDC")

print(staged_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM  ExternalCDC 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_300_Gold_Contoso.dbo.ExternalCDC LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC VACUUM OmniSync_DE_LH_300_Gold_Contoso.dbo.ExternalCDC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_300_Gold_Contoso.dbo.Currency WHERE CurrencyName='EUR'")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger = sc._jvm.org.apache.log4j.LogManager.getLogger("com.omnisync.Logger")
logger.info("info message")
logger.warn("warn message")

import json
import timeit
import traceback
import sys
import unidecode

try:
  kk = 1 / 0
except Exception as ex:
     # could be done differently, just showing you can split it apart to capture everything individually
    ex_t = type(ex).__name__
    err = str(ex)
    err_msg = f'[{ex_t}] - {err}'
    logger.error(err_msg)

    # go through the trackback lines and individually add those to the log as an error
    for l in traceback.format_exc().splitlines():
        logger.error(l)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#load rows
from datetime import datetime
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark.conf.set('spark.sql.caseSensitive', True)

dataMappingSchema = StructType().add("MasterDataMappingKey", "integer") \
                                .add("FabricId", "string") \
                                .add("SalesForceId", "string") \
                                .add("SAPId", "string") \
                                .add("Entity", "string") \
                                .add("Name", "string") \
                                .add("CreatedDate", "timestamp") \
                                .add("UpdatedDate", "timestamp")
                                
# staged_rows = [(1, 1,'001','002','Currency','EUR', datetime.now(), None),
#                (2, 2,'003', None,'Currency','GBP', datetime.now(), None),
#                (3, 3, None,'004','Currency','USD', datetime.now(), None)]
#create dataframe and append current datetime
staged_df = spark.createDataFrame(staged_rows,dataMappingSchema) \
            .write.mode("overwrite").format("delta").saveAsTable("MasterDataMapping")

df = spark.sql("SELECT * FROM OmniSync_DE_LH_300_Gold_Contoso.dbo.MasterDataMapping LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_300_Gold_Contoso.dbo.MasterDataMapping " + \
                "WHERE (SalesForceId='003' OR SalesForceId IS NULL) AND " + \
                 "(SAPId='004' OR SAPId IS NULL)")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.SalesOrders ORDER BY SalesOrdersKey DESC LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping WHERE SalesForceId='001KB000009ROEzYAO' AND Entity='Customer'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping  WHERE SalesForceId='001KB000009ROEwYAO' LIMIT 1000")


display(df)
df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping  LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC UPDATE OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping SET SalesForceId='001KB000009ROEwYAO' WHERE SalesForceId='001KB000009ROEzYAO'


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE OmniSync_DE_LH_320_Gold_Contoso.dbo.Customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Customer ORDER BY CustomerKey DESC LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping
# MAGIC WHERE (SalesForceId='001d1000009tj2bAAA' OR SalesForceId IS NULL) AND (SAPId='' OR SAPId IS NULL)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping 
# MAGIC WHERE (SalesForceId='001d1000009tj2bAAA' OR SalesForceId IS NULL) AND (SAPId='' OR SAPId IS NULL)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.ExternalCDC LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM  OmniSync_DE_LH_320_Gold_Contoso.dbo.Sales WHERE SalesKey IS NULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM  OmniSync_DE_LH_320_Gold_Contoso.dbo.SalesOrders WHERE SalesOrdersKey>=32188091

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Sales ORDER BY SalesKey ASC  LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.SalesOrders ORDER BY SalesOrdersKey DESC LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Customer ORDER BY CustomerKey  DESC LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Store ORDER BY StoreKey DESC LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Sales ORDER BY CreatedDate DESC LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Customer WHERE CompanyName LIKE '%Albany%' LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Customer WHERE CustomerKey=18877 LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Store WHERE StoreKey=199 LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Store WHERE StoreName LIKE '%Albany%' LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT distinct storekey FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Sales

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Sales  WHERE CustomerKey=18877 LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
