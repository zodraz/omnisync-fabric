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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType,DateType
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

masterDataMappingSchema = StructType().add("MasterDataMappingKey", "integer") \
                                .add("FabricId", "string") \
                                .add("SalesForceId", "string") \
                                .add("SAPId", "string") \
                                .add("D365Id", "string") \
                                .add("Entity", "string") \
                                .add("Name", "string") \
                                .add("IsDeleted", "boolean") \
                                .add("CreatedDate", "timestamp") \
                                .add("UpdatedDate", "timestamp")
                        
# Create an empty DataFrame with the schema
empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), masterDataMappingSchema)
# Write the empty DataFrame to create the Delta table
empty_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.MasterDataMapping")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_currency = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimCurrency")
dim_currency = dim_currency.withColumnRenamed('CurrencyName', 'CurrencyCode')
dim_currency = dim_currency.withColumnRenamed('LoadDate', 'CreatedDate')
dim_currency = dim_currency.withColumnRenamed('UpdateDate', 'UpdatedDate')

dim_currency = dim_currency.withColumn("ConversionRate", lit('1.0'))
dim_currency = dim_currency.withColumn("IsDeleted", lit(False))

dim_currency = dim_currency.withColumn("ConversionRate",  when(dim_currency.CurrencyCode.contains("USD"), 0.98) \
                                                .when(dim_currency.CurrencyCode.contains("GBP"), 1.1)
                                                .otherwise(dim_currency.ConversionRate))
dim_currency = dim_currency.withColumn("ConversionRate", dim_currency["ConversionRate"].cast(DoubleType()))

dim_currency = dim_currency.select("CurrencyKey", "CurrencyCode", "CurrencyDescription", "ConversionRate", 
                                   "IsDeleted", "CreatedDate", "UpdatedDate")

dim_currency.write.mode("overwrite").option("overwriteSchema", "true").format("delta")\
            .saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCurrency")

mapping_rows = [(1, 7, '01LWU000002zwab2AA' , '16f35145-2412-f011-998b-7c1e52fba29f', None, 'Currency', 'EUR' , False, datetime.now(), None),
                (2, 1, '01LWU000002zwcD2AQ' , '6f7c960f-3c1d-f011-9989-002248a3370c', None, 'Currency', 'USD' , False, datetime.now(), None),
                (3, 20, '01LWU000002zwdp2AA' , '2087d443-3c1d-f011-9989-002248a3370c', None, 'Currency', 'GBP' , False, datetime.now(), None)]

df_mapping = spark.createDataFrame(mapping_rows, masterDataMappingSchema)
# Write the empty DataFrame to create the Delta table
df_mapping.write.mode("append").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.MasterDataMapping")

df = spark.sql("SELECT CurrencyKey, CurrencyCode, CurrencyDescription, ConversionRate, IsDeleted, CreatedDate, UpdatedDate  \
                FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCurrency LIMIT 1000")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/country-and-continent-codes-list.csv")

df = df.drop('Two_Letter_Country_Code','Three_Letter_Country_Code','Country_Number')
df = df.withColumnRenamed('Continent_Name','Continent')
df = df.withColumnRenamed('Continent_Code','Code')
df = df.withColumnRenamed('Country_Name','Country')

df.write.format("delta").mode("overwrite").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.Continent")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimCustomer")
dim_customer = dim_customer.drop('NameStyle', 'Title', 'Suffix', 'MiddleName', 'AddressLine2','ETLLoadID')
dim_customer = dim_customer.withColumnRenamed('CustomerLabel', 'CustomerCode')
dim_customer = dim_customer.withColumnRenamed('LoadDate', 'CreatedDate')
dim_customer = dim_customer.withColumnRenamed('UpdateDate', 'UpdatedDate')
dim_customer = dim_customer.withColumn("IsDeleted", lit(False))

dim_customer = dim_customer.select( "CustomerKey","GeographyKey", "CustomerCode", "FirstName","LastName","BirthDate", 
							        "MaritalStatus","Gender","EmailAddress","YearlyIncome", 
							        "TotalChildren","NumberChildrenAtHome","Education","Occupation", 
							        "HouseOwnerFlag","NumberCarsOwned","AddressLine1","Phone", 
							        "DateFirstPurchase","CustomerType","CompanyName","IsDeleted", 
							        "CreatedDate","UpdatedDate")

dim_customer.write.mode("overwrite").option("overwriteSchema", "true").format("delta")\
            .saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCustomer")

df_customer = spark.sql("SELECT NVL(MAX(CustomerKey),0)+1 AS NextKey FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCustomer")
nextCustomerValue = df_customer.first()['NextKey']
print('Next value is '+ str(nextCustomerValue))

buyitberlin_cutomer_id = nextCustomerValue + 1

staged_rows = [(nextCustomerValue,586, '111111', None,None,None,None,None,
                            'buyitberlin@fakemail.com',None,None,None,None,None,None,None,
                            'Rosa-Luxemburg-Straße 50', '555-1216',None,'Company','Buy it Berlin',
                            False, datetime.utcnow(),None),
               (buyitberlin_cutomer_id ,890, '423167', None,None,None,None,None,
                            'buyitnewyork@fakemail.com',None,None,None,None,None,None,None,
                            '9896 Talbot St.Brooklyn, NY 11215', '555-3456',None,'Company','Buy it New York',
                            False, datetime.utcnow(),None)]



df_customer = spark.createDataFrame(staged_rows, dim_customer.schema)
# Write the empty DataFrame to create the Delta table
df_customer.write.mode("append").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCustomer")

mapping_rows = [(4, nextCustomerValue, '001WU00000ulBm9YAE' , '6622d6cb-3c1d-f011-9989-002248a3370c', None, 'Customer', '111111' , False, datetime.now(), None),
                (5, buyitberlin_cutomer_id, '001WU00000ulDHhYAM' , '4ed67d9b-3d1d-f011-9989-002248a3370c', None, 'Customer', '123123' , False, datetime.now(), None)]

df_mapping = spark.createDataFrame(mapping_rows, masterDataMappingSchema)
# Write the empty DataFrame to create the Delta table
df_mapping.write.mode("append").format("delta").saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.MasterDataMapping")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_date  = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimDate")
dim_date = dim_date.withColumn('CreatedDate', lit(datetime.utcnow()))
dim_date = dim_date.withColumn('UpdatedDate', lit(datetime.utcnow()))
dim_date = dim_date.withColumn("IsDeleted", lit(False))
dim_date = dim_date.withColumnRenamed('Datekey', 'DateKey')
dim_date = dim_date.withColumn('DateKey', add_months(dim_date.DateKey, 12*14))
dim_date = dim_date.withColumn('CalendarYear', dim_date.CalendarYear + 14)

dim_date = dim_date.groupBy("DateKey", "CalendarYear", "CalendarMonthLabel", "FiscalQuarterLabel",
                            "IsDeleted", "CreatedDate", "UpdatedDate")

dim_date=dim_date.count()

dim_date = dim_date.select("DateKey", "CalendarYear", "CalendarMonthLabel", "FiscalQuarterLabel",
                            "IsDeleted", "CreatedDate", "UpdatedDate")


dim_date.write.mode("overwrite").option("overwriteSchema", "true").format("delta")\
              .saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimDate")

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
dim_geography = dim_geography.withColumn("IsDeleted", lit(False))

continents_df = [(4,'Continent','South America', None, None, None, datetime.now(), datetime.now(),False),
            (5,'Continent','Oceania', None, None, None, datetime.now(), datetime.now(),False),
            (6,'Continent','Africa', None, None, None, datetime.now(), datetime.now(),False),
            (7,'Continent','Antarctica', None, None, None, datetime.now(), datetime.now(),False),
                    
            (953,'Country/Region','South America', None, None, 'Argentina', datetime.now(), datetime.now(),False),
            (954,'Country/Region','Oceania', None, None, 'Australia', datetime.now(), datetime.now(),False),
            (955,'Country/Region','Africa', None, None, 'South Africa', datetime.now(), datetime.now(),False),
            (956,'Country/Region','Antarctica', None, None, 'South Georgia and the South Sandwich Islands', datetime.now(), datetime.now(),False),
                      
            (957,'State/Province','South America', None, 'Misiones', 'Argentina', datetime.now(), datetime.now(),False),
            (958,'State/Province','Oceania', None, 'Queensland', 'Australia', datetime.now(), datetime.now(),False),
            (959,'State/Province','Africa', None, ' North West', 'South Africa', datetime.now(), datetime.now(),False),
            (960,'State/Province','Antarctica', None, None, 'South Georgia and the South Sandwich Islands', datetime.now(), datetime.now(),False),
                    
            (961,'City','South America', 'Córdoba', 'Misiones', 'Argentina', datetime.now(), datetime.now(),False),
            (962,'City','Oceania', ' Mount Tyson', 'Queensland', 'Australia', datetime.now(), datetime.now(),False),
            (963,'City','Africa', 'Rustenburg', ' North West', 'South Africa', datetime.now(), datetime.now(),False),
            (964,'City','Antarctica','King Edward Point', 'Sandwich Islands', 'South Georgia and the South Sandwich Islands', datetime.now(), datetime.now(),False)]

#create dataframe and append current datetime
continents_df = spark.createDataFrame(continents_df,dim_geography.schema)
dim_geography = dim_geography.union(continents_df)

dim_geography = dim_geography.select( "GeographyKey", "GeographyType", "ContinentName", "CityName", "StateProvinceName",
                            "RegionCountryName", "IsDeleted", "CreatedDate", "UpdatedDate")

dim_geography.write.mode("overwrite").option("overwriteSchema", "true").format("delta")\
             .saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimGeography")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimProduct")
dim_product = dim_product.drop('ETLLoadID', 'ImageURL','StyleID','StyleName', 
                                 'ProductURL', 'SizeRange', 'UnitMeasureID','UnitMeasureName',
                                 'StockTypeID', 'StockTypeName')
dim_product = dim_product.withColumnRenamed('ProductLabel', 'ProductCode')
dim_product = dim_product.withColumnRenamed('LoadDate', 'CreatedDate')
dim_product = dim_product.withColumnRenamed('UpdateDate', 'UpdatedDate')
dim_product = dim_product.withColumn("IsDeleted", lit(False))
dim_product = dim_product.withColumn('SizeUnitMeasureID', lit('2'))
dim_product = dim_product.withColumn('SizeUnitMeasureName', lit('centimeters'))
dim_product = dim_product.withColumn('WeightUnitMeasureID', lit('2'))
dim_product = dim_product.withColumn('WeightUnitMeasureName', lit("grams"))
dim_product = dim_product.withColumn("ClassID", dim_product["ClassID"].cast(IntegerType()))
dim_product = dim_product.withColumn("ColorID", dim_product["ColorID"].cast(IntegerType()))
dim_product = dim_product.withColumn("SizeUnitMeasureID", dim_product["SizeUnitMeasureID"].cast(IntegerType()))
dim_product = dim_product.withColumn("WeightUnitMeasureID", dim_product["WeightUnitMeasureID"].cast(IntegerType()))
dim_product = dim_product.withColumn('AvailableForSaleDate', add_months(dim_product.AvailableForSaleDate, 12*14))
dim_product = dim_product.withColumn('StopSaleDate', dim_product["StopSaleDate"].cast(DateType()))
dim_product = dim_product.withColumn('CurrencyKey', lit('7').cast(IntegerType()))
dim_product = dim_product.na.fill({'Size': '1x1x1'})
dim_product = dim_product.na.fill({'SizeUnitMeasureID': 2})


dim_product = dim_product.select("ProductKey", "ProductCode", "ProductName", "ProductDescription","ProductSubcategoryKey",
                                "Manufacturer", "BrandName","ClassID","ClassName","ColorID",
                                "ColorName","Size","SizeUnitMeasureID", "SizeUnitMeasureName", "Weight","WeightUnitMeasureID",
                                "WeightUnitMeasureName","UnitCost","UnitPrice","CurrencyKey", "AvailableForSaleDate","StopSaleDate","Status",
                                "IsDeleted", "CreatedDate", "UpdatedDate")

dim_product.write.mode("overwrite").option("overwriteSchema", "true").format("delta")\
            .saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProduct")


# dim_product = spark.sql("SELECT NVL(MAX(CustomerKey),0)+1 AS NextKey FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.DimProduct")
# nextValue = dim_product.first()['NextKey']
# print('Next value is '+ str(nextValue))

# staged_rows = [(nextValue, dict['ProductCode'], dict['ProductName'] ,dict['ProductDescription'], dict['ProductSubcategoryKey'],dict['Manufacturer'],
#                             dict['BrandID'],dict['BrandName'],dict['ClassID'],dict['ClassName'],
#                             dict['ColorID'], dict['ColorName'], dict['Size'], dict['SizeUnitMeasureID'],
#                             dict['SizeMeasureName'], dict['Weight'], dict['WeightUnitMeasureID'], dict['WeightUnitMeasureName'],
#                             dict['UnitCost'], dict['UnitPrice'], currencyKey, availableForSaleDate, dict['Status'], dict['IsDeleted'],
#                             createdDateTimeStamp,None)]

# dim_product = spark.createDataFrame(staged_rows, dim_product.schema)
# # Write the empty DataFrame to create the Delta table
# dim_product.write.mode("append").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.DimProduct")

# mapping_rows = [(3, nextValue, '001KB000009ROH5YAO' , None, 'Product', '111111' , False, datetime.now(), None),
#                 (4, nextValue + 1, '001KB000009ROEzYAO' , None, 'Product', '123123' , False, datetime.now(), None)]

# df_mapping = spark.createDataFrame(mapping_rows, externalCDCSchema)
# # Write the empty DataFrame to create the Delta table
# df_mapping.write.mode("append").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product_category = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimProductCategory")
dim_product_category = dim_product_category.drop('ProductCategoryDescription', 'ETLLoadID')
dim_product_category = dim_product_category.withColumnRenamed('ProductCategoryLabel', 'ProductCategoryCode')
dim_product_category = dim_product_category.withColumnRenamed('LoadDate', 'CreatedDate')
dim_product_category = dim_product_category.withColumnRenamed('UpdateDate', 'UpdatedDate')
dim_product_category = dim_product_category.withColumn("IsDeleted", lit(False))

# salesforce_categories = [(9,'09','Beverages', datetime.now(), datetime.now(), False),
#             (10,'10','Chips', datetime.now(), datetime.now(), False),
#             (11,'11','Detergent', datetime.now(), datetime.now(),False),
#             (12,'12','Frozen', datetime.now(), datetime.now(), False),
#             (13,'13','Hygiene', datetime.now(), datetime.now(), False),
#             (14,'14','Snacks', datetime.now(), datetime.now(), False)]

#create dataframe and append current datetime
# salesforce_categories_df = spark.createDataFrame(salesforce_categories,dim_product_category.schema)
# dim_product_category = dim_product_category.union(salesforce_categories_df)

dim_product_category = dim_product_category.select( "ProductCategoryKey", "ProductCategoryCode", "ProductCategoryName",
                              "IsDeleted", "CreatedDate", "UpdatedDate")

dim_product_category.write.mode("overwrite").option("overwriteSchema", "true").format("delta")\
                    .saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProductCategory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product_subcategory = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimProductSubcategory")
dim_product_subcategory = dim_product_subcategory.drop('ProductSubcategoryDescription', 'ETLLoadID')
dim_product_subcategory = dim_product_subcategory.withColumnRenamed('ProductSubcategoryLabel', 'ProductSubcategoryCode')
dim_product_subcategory = dim_product_subcategory.withColumnRenamed('LoadDate', 'CreatedDate')
dim_product_subcategory = dim_product_subcategory.withColumnRenamed('UpdateDate', 'UpdatedDate')
dim_product_subcategory = dim_product_subcategory.withColumn("IsDeleted", lit(False))

subcat_schema = StructType([ \
                StructField("ProductSubcategoryKey",IntegerType(),True), \
                StructField("ProductSubcategoryCode",StringType(),False), \
                StructField("ProductSubcategoryName",StringType(),False), \
                StructField("ProductCategoryKey",IntegerType(),True), \
                StructField("CreatedDate", TimestampType(), True), \
                StructField("UpdatedDate", TimestampType(), True), \
                StructField("IsDeleted", BooleanType(), True)
            ])

salesforce_subcategories = [(50,'50','Audio', 1, datetime.now(), datetime.now(), False),
            (51,'51','TV and Video', 2, datetime.now(), datetime.now(), False),
            (52,'52','Computers', 3, datetime.now(), datetime.now(), False),
            (53,'53','Cameras and camcorders', 4, datetime.now(), datetime.now(), False),
            (54,'54','Cell phones', 5, datetime.now(), datetime.now(), False),
            (55,'55','Music, Movies and Audio Books', 6, datetime.now(), datetime.now(), False),
            (56,'56','Games and Toys', 7, datetime.now(), datetime.now(), False),
            (57,'57','Home Appliances', 8, datetime.now(), datetime.now(), False)]

#create dataframe and append current datetime
salesforce_subcategories_df = spark.createDataFrame(salesforce_subcategories,subcat_schema)
dim_product_subcategory = dim_product_subcategory.union(salesforce_subcategories_df)

salesforce_subcategories_df = salesforce_subcategories_df.select( "ProductSubcategoryKey", "ProductSubcategoryCode", "ProductSubcategoryName", "ProductCategoryKey",
                              "IsDeleted", "CreatedDate", "UpdatedDate")


dim_product_subcategory.write.mode("overwrite").option("overwriteSchema", "true").format("delta") \
                       .saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProductSubcategory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimProductCategory LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_store = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.DimStore")
dim_store = dim_store.drop('ScenarioLabel', 'ScenarioDescription', 'EntityKey', 'ETLLoadID','Geometry',
                            'Status','OpenDate','CloseDate')

dim_store = dim_store.withColumn('GeoLocation', regexp_replace('GeoLocation', 'POINT ', ''))
dim_store = dim_store.withColumn('GeoLocation', regexp_replace('GeoLocation', '\)', ''))
dim_store = dim_store.withColumn('GeoLocation', regexp_replace('GeoLocation', '\(', ''))
splitted_col = split(dim_store['GeoLocation'], ' ')
dim_store = dim_store.withColumn('Longitude', splitted_col.getItem(0))
dim_store = dim_store.withColumn('Latitude', splitted_col.getItem(1)) 

dim_store = dim_store.withColumnRenamed('LoadDate', 'CreatedDate')
dim_store = dim_store.withColumnRenamed('UpdateDate', 'UpdatedDate')
dim_store = dim_store.withColumn("IsDeleted", lit(False))

dim_store = dim_store.withColumn("StoreTypeID",  when(dim_store.StoreType.contains("Store"), 1) \
                                                .when(dim_store.StoreType.contains("Catalog"), 2) \
                                                .when(dim_store.StoreType.contains("Online"), 3) \
                                                .when(dim_store.StoreType.contains("Reseller"), 4) \
                                                # .when(dim_store.StoreType.contains("Flagship"), 5) \
                                                # .when(dim_store.StoreType.contains("Virtual"), 6) \
                                                # .when(dim_store.StoreType.contains("Van"), 7) \
                                                .otherwise(dim_store.StoreType))
dim_store = dim_store.withColumn("StoreTypeID", dim_store["StoreTypeID"].cast(IntegerType()))
dim_store = dim_store.withColumn("Longitude", dim_store["Longitude"].cast(DoubleType()))
dim_store = dim_store.withColumn("Latitude", dim_store["Latitude"].cast(DoubleType()))

dim_store = dim_store.drop('GeoLocation')
dim_store = dim_store.withColumn("StoreCode",  dim_store["StoreKey"].cast(StringType()))

#Buy it New York customer as Default
dim_store = dim_store.withColumn("CustomerKey", lit(buyitberlin_cutomer_id))

dim_store = spark.sql("SELECT *,REPLACE(REPLACE(StoreName, 'Contoso ',''),' Store','') AS StoreNameFixed \
                FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimStore")
                

for store_row in dim_store.collect():
    print(store_row)
    try:
        customers_lookup = df = spark.sql("SELECT CustomerKey \
                                    FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCustomer \
                                    WHERE CustomerType='Company' AND \
                                    REPLACE(CompanyName, 'Company','') = '" + store_row['StoreNameFixed'] + "'")

        if customers_lookup.count() > 0 :
            dim_store = dim_store.withColumn("CustomerKey", when(dim_store.StoreKey == store_row['StoreKey'],customers_lookup.first()['CustomerKey'] ) \
                    .otherwise(dim_store.CustomerKey))
    except:
        pass

dim_store = dim_store.select( "StoreKey", "StoreCode", "GeographyKey", "CustomerKey", "StoreTypeID", "StoreType", "StoreName", "StoreDescription",
                              "StorePhone", "StoreFax", "AddressLine1", "EmployeeCount", "Longitude",
                              "Latitude", "IsDeleted", "CreatedDate", "UpdatedDate")

dim_store.write.mode("overwrite").option("overwriteSchema", "true").format("delta")\
               .saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.DimStore")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_online_sales  = spark.sql("SELECT * FROM OmniSync_DE_LH_100_Bronze_Contoso.dbo.FactOnlineSales")
fact_online_sales  = fact_online_sales.drop('ScenarioLabel', 'ScenarioDescription', 'PromotionKey', 
                                              'ReturnQuantity','ReturnAmount', 'DiscountAmount', 'DiscountQuantity', 'ETLLoadID')
fact_online_sales = fact_online_sales.withColumnRenamed('OnlineSalesKey', 'SalesOrdersKey')
fact_online_sales = fact_online_sales.withColumnRenamed('LoadDate', 'CreatedDate')
fact_online_sales = fact_online_sales.withColumnRenamed('UpdateDate', 'UpdatedDate')
fact_online_sales = fact_online_sales.withColumn("IsDeleted", lit(False))
fact_online_sales = fact_online_sales.withColumn('CreatedDate',  lit(datetime.now()))
fact_online_sales = fact_online_sales.withColumn('UpdatedDate',  lit(datetime.now()))
fact_online_sales = fact_online_sales.withColumn('DateKey', add_months(fact_online_sales.DateKey, 12*16))
fact_online_sales = fact_online_sales.withColumn("SalesAmount", fact_online_sales["SalesAmount"].cast(DecimalType(19,4)))
fact_online_sales = fact_online_sales.withColumn("TotalCost", fact_online_sales["TotalCost"].cast(DecimalType(19,4)))
fact_online_sales = fact_online_sales.withColumn("UnitCost", fact_online_sales["UnitCost"].cast(DecimalType(19,4)))
fact_online_sales = fact_online_sales.withColumn("UnitPrice", fact_online_sales["UnitPrice"].cast(DecimalType(19,4)))
fact_online_sales = fact_online_sales.withColumn("SalesQuantity", fact_online_sales["SalesQuantity"].cast(IntegerType()))

fact_online_sales = fact_online_sales.select( "SalesOrdersKey", "DateKey", "StoreKey", "ProductKey", "CurrencyKey", "CustomerKey",
                              "SalesOrderNumber", "SalesOrderLineNumber", "SalesQuantity", "SalesAmount", "TotalCost", "UnitCost",
                              "UnitPrice", "IsDeleted", "CreatedDate", "UpdatedDate").alias("so")

# Leave only Company rows
org_customers = df = spark.sql("SELECT * FROM OmniSync_DE_LH_200_Silver_Contoso.dbo.DimCustomer WHERE CustomerType='Company'").alias("org")

fact_online_sales = fact_online_sales.join(org_customers, on="CustomerKey", how="inner") \
                                 .select("so.SalesOrdersKey", "so.DateKey", "so.StoreKey", "so.ProductKey", "so.CurrencyKey", "so.CustomerKey",
                                 "so.SalesOrderNumber", "so.SalesOrderLineNumber", "so.SalesQuantity", "so.SalesAmount", "so.TotalCost", "so.UnitCost",
                                "so.UnitPrice", "so.IsDeleted", "so.CreatedDate", "so.UpdatedDate")

fact_online_sales.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("OmniSync_DE_LH_200_Silver_Contoso.dbo.FactOnlineSales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
