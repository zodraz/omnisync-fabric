# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e855e388-3034-499e-a5ee-1808e036efbd",
# META       "default_lakehouse_name": "OmniSync_DE_LH_300_Gold_Contoso",
# META       "default_lakehouse_workspace_id": "6b35ae7a-875a-4e1a-8f60-9f0a22d0e0d0"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark.conf.set('spark.sql.caseSensitive', True)

externalCDCSchema = StructType().add("Operation", "string") \
                                .add("Entity", "string") \
                                .add("Values", "string") \
                                .add("CreatedDate", "date") \
                                .add("UpdatedDate", "date")
                             
# Create an empty DataFrame with the schema
empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), externalCDCSchema)
# Write the empty DataFrame to create the Delta table
empty_df.write.mode("overwrite").format("delta").saveAsTable("ExternalCDC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def decodeEntity(operation,entity,dict):
    if entity == 'Currency':
        if operation == 'Insert':
            staged_rows = [(dict['CurrencyKey'],dict['CurrencyName'],dict['CurrencyDescription'], datetime.now(), None)]
        elif operation == 'Update':
            staged_rows = [(dict['CurrencyKey'],dict['CurrencyName'],dict['CurrencyDescription'], dict['CreatedDate'], datetime.now())]
        else:
            staged_rows = None
    else:
       staged_rows = None
    return staged_rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getSchema(entity):
    if entity == 'Currency':
        return StructType().add("CurrencyKey", "string") \
                            .add("CurrencyName", "string") \
                            .add("CurrencyDescription", "string") \
                            .add("CreatedDate", "date") \
                            .add("UpdatedDate", "date")
    else:
       return None 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def checkIfInsertNeeded(currencyKey):
    df = spark.sql("SELECT * FROM OmniSync_DE_LH_300_Gold_Contoso.dbo.Currency WHERE CurrencyKey='"+ currencyKey+"'")
    return False if df.count() > 0 else True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fixJson(jsonString):
    jsonString = jsonString.replace("=","\":\"")
    jsonString = jsonString.replace("{","{\"")
    jsonString = jsonString.replace(", ","\", \"")
    jsonString = jsonString.replace("}", "\"}")

    print(jsonString)
    
    return jsonString


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import * 
from delta.tables import *
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

externalCDCSchema = spark.read.parquet("Tables/ExternalCDC").schema

df = spark.readStream.schema(externalCDCSchema).format("parquet").option("path", "Tables/ExternalCDC").load()

def sendToSinkTable(df, epoch_id):
    
    print('------------------------------Stream received---------------------------------------')
    
    dataCollect = df.collect()
    for row in dataCollect:
        print(row)

        operation=row.Operation
        entity=row.Entity

        values = fixJson(row.Values)

        dict = json.loads(values) 

        if operation == 'Insert':
            print('Insert PK to check: ' + dict['CurrencyKey'])
            dfEntity = decodeEntity('Insert',entity,dict)
            if checkIfInsertNeeded(dict['CurrencyKey']):
                print('needed')
            else:
                print('Row already in the system. Not inserting it.')
            # staged_df = spark.createDataFrame(dfEntity,getSchema(entity)) \
            #             .write.mode("append").format("delta").saveAsTable(entity)
        elif operation == 'Update':
            deltaTable = DeltaTable.forPath(spark, 'Tables/'+ entity)
            deltaTable.update(
                condition = "OrderID = 1",
                set = { "ItemPrice": "9", "OrderTotal": "38"  }
            )
        elif operation == 'Delete':
            deltaTable = DeltaTable.forPath(spark, 'Tables/'+ entity)
            # Declare the predicate by using a SQL-formatted string.
            deltaTable.delete("OrderID = 2")
        else:
            print('Error')

print('------------------------------Starting---------------------------------------')

df.writeStream \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation","Tables/ExternalCDC/checkpoint") \
    .format("delta") \
    .option("path", "Tables/ExternalCDC") \
    .foreachBatch(sendToSinkTable) \
    .start() \
    .awaitTermination()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#load rows
from datetime import datetime
import json
currency1= json.dumps({ "CurrencyKey": "EURO4", "CurrencyName": "EURO4", "CurrencyDescription": "Euro 4"})
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

# MAGIC %%sql
# MAGIC DELETE FROM  ExternalCDC 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltaTable = DeltaTable.forPath(spark, 'Tables/ExternalCDC')
deltaTable.delete("Values = \{CurrencyKey=EURO2, CurrencyDescription=Euro 2, CurrencyName=EURO2\}")

# METADATA ********************

# META {
# META   "language": "python",
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
