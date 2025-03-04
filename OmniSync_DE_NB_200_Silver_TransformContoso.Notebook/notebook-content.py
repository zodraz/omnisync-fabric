# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4091faae-0bfd-4c88-84fe-efd3155983b6",
# META       "default_lakehouse_name": "OmniSync_DE_LH_100_Bronze_Contoso",
# META       "default_lakehouse_workspace_id": "6b35ae7a-875a-4e1a-8f60-9f0a22d0e0d0",
# META       "known_lakehouses": [
# META         {
# META           "id": "4091faae-0bfd-4c88-84fe-efd3155983b6"
# META         },
# META         {
# META           "id": "c0cafa85-d038-4941-9d06-1f7cd07bd5b0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.conf.set('spark.sql.caseSensitive', True)

dim_channel = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimChannel")
#display(dim_channel)
dim_channel = dim_channel.drop('ChannelLabel', 'ETLLoadID')
dim_channel = dim_channel.withColumnRenamed('LoadDate', 'CreatedDate')
dim_channel = dim_channel.withColumnRenamed('UpdateDate', 'UpdatedDate')
#display(dim_channel)
dim_channel.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimChannel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_currency = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimCurrency")
dim_currency = dim_currency.drop('CurrencyLabel', 'ETLLoadID')
dim_currency = dim_currency.withColumnRenamed('LoadDate', 'CreatedDate')
dim_currency = dim_currency.withColumnRenamed('UpdateDate', 'UpdatedDate')
dim_currency.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCurrency")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimCustomer")
dim_customer = dim_customer.drop('CustomerLabel', 'NameStyle', 'Title', 'Suffix', 'MiddleName', 'AddressLine2','ETLLoadID')
dim_customer = dim_customer.withColumnRenamed('LoadDate', 'CreatedDate')
dim_customer = dim_customer.withColumnRenamed('UpdateDate', 'UpdatedDate')
dim_customer.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCustomer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

dim_date  = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimDate")
dim_date  = dim_date.drop('FullDateLabel'
      ,'DateDescription'
      ,'CalendarYearLabel'
      ,'CalendarHalfYear'
      ,'CalendarHalfYearLabel'
      ,'CalendarQuarter'
      ,'CalendarQuarterLabel'
      ,'CalendarMonth'
      ,'CalendarWeek'
      ,'CalendarWeekLabel'
      ,'CalendarDayOfWeek'
      ,'CalendarDayOfWeekLabel'
      ,'FiscalYear'
      ,'FiscalYearLabel'
      ,'FiscalHalfYear'
      ,'FiscalHalfYearLabel'
      ,'FiscalQuarter'
      ,'FiscalMonth'
      ,'FiscalMonthLabel'
      ,'IsWorkDay'
      ,'IsHoliday'
      ,'HolidayName'
      ,'EuropeSeason'
      ,'NorthAmericaSeason'
      ,'AsiaSeason')
dim_date = dim_date.withColumnRenamed('LoadDate', 'CreatedDate')
dim_date = dim_date.withColumnRenamed('UpdateDate', 'UpdatedDate')
dim_date = dim_date.withColumn('Datekey', F.add_months(dim_date.Datekey, 12*14))
dim_date = dim_date.withColumn('CalendarYear', dim_date.CalendarYear + 14)
dim_date.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimDate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_geography = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimGeography")
dim_geography = dim_geography.drop('Geometry', 'ETLLoadID')
dim_geography = dim_geography.withColumnRenamed('LoadDate', 'CreatedDate')
dim_geography = dim_geography.withColumnRenamed('UpdateDate', 'UpdatedDate')
dim_geography.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimGeography")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimProduct")
dim_product = dim_product.drop('ProductLabel', 'ETLLoadID', 'StopSaleDate', 'ImageURL', 
                                 'ProductURL', 'Size', 'SizeRange', 'SizeUnitMeasureID')
dim_product = dim_product.withColumnRenamed('LoadDate', 'CreatedDate')
dim_product = dim_product.withColumnRenamed('UpdateDate', 'UpdatedDate')
dim_product = dim_product.na.fill({'Status': 'Off'})
dim_product = dim_product.na.fill({'StyleID': 1})
dim_product = dim_product.withColumn('AvailableForSaleDate', F.add_months(dim_product.AvailableForSaleDate, 12*14))
dim_product.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProduct")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product_category = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimProductCategory")
dim_product_category = dim_product_category.drop('ProductCategoryLabel','ProductCategoryDescription', 'ETLLoadID')
dim_product_category = dim_product_category.withColumnRenamed('LoadDate', 'CreatedDate')
dim_product_category = dim_product_category.withColumnRenamed('UpdateDate', 'UpdatedDate')
display(dim_product_category)
dim_product_category.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProductCategory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product_subcategory = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimProductSubcategory")
#display(dim_product_subcategory)
dim_product_subcategory = dim_product_subcategory.drop('ProductSubcategoryLabel', 'ProductSubcategoryDescription', 'ETLLoadID')
dim_product_subcategory = dim_product_subcategory.withColumnRenamed('LoadDate', 'CreatedDate')
dim_product_subcategory = dim_product_subcategory.withColumnRenamed('UpdateDate', 'UpdatedDate')
#display(dim_product_subcategory)
dim_product_subcategory.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProductSubcategory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_promotion = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimPromotion")
#display(dim_promotion)
dim_promotion = dim_promotion.drop('PromotionLabel', 'Promotiondescription', 'StartDate', 'EndDate', 
                                                       'ETLLoadID', 'MinQuantity', 'MaxQuantity')
dim_promotion = dim_promotion.withColumnRenamed('LoadDate', 'CreatedDate')
dim_promotion = dim_promotion.withColumnRenamed('UpdateDate', 'UpdatedDate')
#display(dim_promotion)
dim_promotion.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimPromotion")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_store = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimStore")
#display(dim_store)
dim_store = dim_store.drop('ScenarioLabel', 'ScenarioDescription', 'ETLLoadID')
dim_store = dim_store.withColumnRenamed('LoadDate', 'CreatedDate')
dim_store = dim_store.withColumnRenamed('UpdateDate', 'UpdatedDate')
#display(dim_store)
dim_store.write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimStore")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_online_sales  = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.FactOnlineSales")
#display(fact_online_sales )
fact_online_sales  = fact_online_sales .drop('ScenarioLabel', 'ScenarioDescription', 'ETLLoadID')
fact_online_sales  = fact_online_sales .withColumnRenamed('LoadDate', 'CreatedDate')
fact_online_sales  = fact_online_sales .withColumnRenamed('UpdateDate', 'UpdatedDate')
#display(fact_online_sales )
fact_online_sales .write.mode("overwrite").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.FactOnlineSales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
