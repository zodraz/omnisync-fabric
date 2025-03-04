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
# META           "id": "e855e388-3034-499e-a5ee-1808e036efbd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.conf.set('spark.sql.caseSensitive', True)

dim_channel = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimChannel")
display(dim_channel)
dim_channel.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_300_Gold_Contoso.dbo.Channel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_currency = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCurrency")
dim_currency.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_300_Gold_Contoso.dbo.Currency")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCustomer")
dim_customer.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_300_Gold_Contoso.dbo.Customer")

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

# CELL ********************

dim_date  = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimDate")
dim_date.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_300_Gold_Contoso.dbo.Date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_geography = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimGeography")
dim_geography.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_300_Gold_Contoso.dbo.Geography")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProduct")
dim_product.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_300_Gold_Contoso.dbo.Product")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product_category = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProductCategory")
dim_product_category.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_300_Gold_Contoso.dbo.ProductCategory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product_subcategory = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProductSubcategory")
dim_product_subcategory.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_300_Gold_Contoso.dbo.ProductSubcategory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_promotion = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimPromotion")
dim_promotion.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_300_Gold_Contoso.dbo.Promotion")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_store = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimStore")
dim_store.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_300_Gold_Contoso.dbo.Store")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_online_sales = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.FactOnlineSales")
fact_online_sales.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_300_Gold_Contoso.dbo.OnlineSales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales = spark.sql("""
SELECT  DateKey,StoreKey,ProductKey,PromotionKey,CurrencyKey,
        UnitCost,UnitPrice,
        SUM(SalesQuantity) SalesQuantity,
        SUM(ReturnQuantity) ReturnQuantity, 
        SUM(ReturnAmount) ReturnAmount,
        SUM(DiscountQuantity) DiscountQuantity, DiscountAmount,
        SUM(TotalCost) TotalCost,
        SUM(SalesAmount) SalesAmount
FROM (
    SELECT DateKey,StoreKey,ProductKey,PromotionKey,CurrencyKey,
            UnitCost,UnitPrice,
            SUM(SalesQuantity) SalesQuantity,
            SUM(ReturnQuantity) ReturnQuantity,
            CASE
                WHEN ReturnQuantity = 0 THEN 0
                ELSE SUM(ReturnAmount + DiscountAmount) 
            END AS ReturnAmount,
            SUM(DiscountQuantity) DiscountQuantity, DiscountAmount,
            CASE
                WHEN ReturnQuantity = 0 THEN SUM(SalesQuantity*UnitCost)
                ELSE SUM(ReturnQuantity * UnitCost)* -1 
            END AS TotalCost,
            SUM(SalesQuantity*UnitPrice)-SUM(DiscountQuantity* DiscountAmount) SalesAmount
    FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.FactOnlineSales
    GROUP BY DateKey,StoreKey,ProductKey,PromotionKey,CurrencyKey,UnitCost,UnitPrice,ReturnQuantity,DiscountAmount
  ) sales
  GROUP BY DateKey,StoreKey,ProductKey,PromotionKey,CurrencyKey,UnitCost,UnitPrice,DiscountAmount """)

fact_sales.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_300_Gold_Contoso.dbo.Sales")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT   ROW_NUMBER() OVER(ORDER BY DateKey ASC) AS SalesKey,
# MAGIC          DateKey,StoreKey,ProductKey,PromotionKey,CurrencyKey,
# MAGIC          UnitCost,UnitPrice,
# MAGIC 		 SUM(SalesQuantity) SalesQuantity,
# MAGIC 		 SUM(ReturnQuantity) ReturnQuantity, 
# MAGIC 		 SUM(ReturnAmount) ReturnAmount,
# MAGIC 		 SUM(DiscountQuantity) DiscountQuantity, DiscountAmount,
# MAGIC          SUM(TotalCost) TotalCost,
# MAGIC 		 SUM(SalesAmount) SalesAmount
# MAGIC   FROM (
# MAGIC 	  SELECT DateKey,StoreKey,ProductKey,PromotionKey,CurrencyKey,
# MAGIC 			 UnitCost,UnitPrice,
# MAGIC 			 SUM(SalesQuantity) SalesQuantity,
# MAGIC 			 SUM(ReturnQuantity) ReturnQuantity,
# MAGIC 			 CASE
# MAGIC 				WHEN ReturnQuantity = 0 THEN 0
# MAGIC 				ELSE SUM(ReturnAmount + DiscountAmount) 
# MAGIC 			 END AS ReturnAmount,
# MAGIC 			 SUM(DiscountQuantity) DiscountQuantity, DiscountAmount,
# MAGIC 			 CASE
# MAGIC 				WHEN ReturnQuantity = 0 THEN SUM(SalesQuantity*UnitCost)
# MAGIC 				ELSE SUM(ReturnQuantity * UnitCost)* -1 
# MAGIC 			 END AS TotalCost,
# MAGIC 			 SUM(SalesQuantity*Unitprice)-SUM(DiscountQuantity* DiscountAmount) SalesAmount
# MAGIC 	  FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.FactOnlineSales
# MAGIC 	  group by DateKey,StoreKey,ProductKey,PromotionKey,CurrencyKey,UnitCost,UnitPrice,ReturnQuantity,DiscountAmount
# MAGIC   ) m
# MAGIC   group by DateKey,StoreKey,ProductKey,PromotionKey,CurrencyKey,UnitCost,UnitPrice,DiscountAmount


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_300_Gold_Contoso.dbo.Currency LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
