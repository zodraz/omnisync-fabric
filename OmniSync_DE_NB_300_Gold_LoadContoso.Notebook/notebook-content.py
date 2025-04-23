# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c0cafa85-d038-4941-9d06-1f7cd07bd5b0",
# META       "default_lakehouse_name": "OmniSync_DE_LH_200_Silver_Contoso",
# META       "default_lakehouse_workspace_id": "6b35ae7a-875a-4e1a-8f60-9f0a22d0e0d0",
# META       "known_lakehouses": [
# META         {
# META           "id": "c0cafa85-d038-4941-9d06-1f7cd07bd5b0"
# META         },
# META         {
# META           "id": "706dc789-a524-424c-8dc3-1ec4ec5f4e1a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.types import MapType,StringType,IntegerType,TimestampType,DoubleType,BooleanType,StructType,StructField
spark.conf.set('spark.sql.caseSensitive', True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_currency = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCurrency")
dim_currency.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.Currency")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.Continent")
dim_customer.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.Continent")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCustomer")
dim_customer.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.Customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_date  = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimDate")
dim_date.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.Date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_geography = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimGeography")
dim_geography.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.Geography")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProduct")
dim_product.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.Product")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product_category = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProductCategory")
dim_product_category.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.ProductCategory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product_subcategory = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProductSubcategory")
dim_product_subcategory.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.ProductSubcategory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_store = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimStore")
dim_store.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.Store")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_online_sales = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.FactOnlineSales")
fact_online_sales.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.SalesOrders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

fact_sales = spark.sql("""
    SELECT  DateKey,StoreKey,ProductKey,CurrencyKey,CustomerKey,
            UnitCost,UnitPrice,
            SUM(SalesQuantity) SalesQuantity,
            SUM(SalesQuantity*UnitCost) TotalCost,
			SUM(SalesQuantity*UnitPrice) SalesAmount,
            CURRENT_TIMESTAMP AS CreatedDate,
            CURRENT_TIMESTAMP AS UpdatedDate
    FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.FactOnlineSales
    WHERE IsDeleted = False 
    GROUP BY DateKey,StoreKey,ProductKey,CurrencyKey,CustomerKey,UnitCost,UnitPrice """)

fact_sales = fact_sales.withColumn('CreatedDate', lit(datetime.now()))
fact_sales = fact_sales.withColumn('UpdatedDate', lit(datetime.now()))
fact_sales = fact_sales.withColumn("SalesQuantity", fact_sales["SalesQuantity"].cast(IntegerType()))
fact_sales = fact_sales.withColumn("CustomerKey", fact_sales["CustomerKey"].cast(IntegerType()))
fact_sales = fact_sales.withColumn("SalesAmount", fact_sales["SalesAmount"].cast(DecimalType(19,4)))
fact_sales = fact_sales.withColumn("TotalCost", fact_sales["TotalCost"].cast(DecimalType(19,4)))
fact_sales = fact_sales.withColumn("UnitCost", fact_sales["UnitCost"].cast(DecimalType(19,4)))
fact_sales = fact_sales.withColumn("UnitPrice", fact_sales["UnitPrice"].cast(DecimalType(19,4)))

fact_sales.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.Sales")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT  DateKey,StoreKey,ProductKey,CurrencyKey,CustomerKey
# MAGIC         UnitCost,UnitPrice,
# MAGIC         SUM(SalesQuantity) SalesQuantity,
# MAGIC         SUM(SalesQuantity*UnitCost) TotalCost,
# MAGIC 		SUM(SalesQuantity*UnitPrice) SalesAmount
# MAGIC FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.FactOnlineSales
# MAGIC WHERE IsDeleted = False 
# MAGIC GROUP BY DateKey,StoreKey,ProductKey,CurrencyKey,CustomerKey,UnitCost,UnitPrice
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark.conf.set('spark.sql.caseSensitive', True)

externalCDCSchema = StructType().add("Operation", "string") \
                                .add("Entity", "string") \
                                .add("Values", "string") \
                                .add("CreatedDate", "timestamp") \
                                .add("UpdatedDate", "timestamp")
                        
# Create an empty DataFrame with the schema
empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), externalCDCSchema)
# Write the empty DataFrame to create the Delta table
empty_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.ExternalCDC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_masterdatamapping = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.MasterDataMapping")
dim_masterdatamapping.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
