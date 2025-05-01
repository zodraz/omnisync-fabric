#!/usr/bin/env python
# coding: utf-8

# ## OmniSync_DE_NB_LoadCDC
# 
# New notebook

# In[1]:


from datetime import datetime
import sys
from pyspark.sql import SparkSession
from dateutil import parser
import decimal
from pyspark.sql.types import MapType,StringType,IntegerType,TimestampType,DoubleType,FloatType,DecimalType, \
                              BooleanType, BinaryType, StructType,StructField
import pandas as pd
import notebookutils
import traceback
from pyspark.sql.types import * 
from delta.tables import *
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.functions import from_json
import json
import traceback

spark.conf.set('spark.sql.caseSensitive', True)
logger = sc._jvm.org.apache.log4j.LogManager.getLogger("com.omnisync.Logger")

def getRowToCreate(entity, dict):

    df = spark.sql("SELECT NVL(MAX("+ getPrimaryKey(entity) + "),0)+1 AS NextKey FROM " + "OmniSync_DE_LH_320_Gold_Contoso.dbo." + entity )
    nextValue = df.first()['NextKey']
    logger.info('Next value is '+ str(nextValue))

    if entity!='Sales':
        if str(dict['CreatedDate']).isdigit():
            createdDateTimeStamp = datetime.utcfromtimestamp(int(dict['CreatedDate']) / 1000)
            print(createdDateTimeStamp.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            createdDateTimeStamp = parser.parse(dict['CreatedDate'])

        if 'Latitude' in dict and 'Longitude' in dict:
            geometryKey = notebookutils.notebook.run("OmniSync_DE_NB_GeographyCDC", 90, 
                        {'useRootDefaultLakehouse': True, "latitude": dict['Latitude'], "longitude": dict['Longitude'] })
            geometryKey = int(geometryKey) if geometryKey != 'None' else None
        elif 'addressLine1' in dict:
            geometryKey = notebookutils.notebook.run("OmniSync_DE_NB_GeographyCDC", 90, 
                        {'useRootDefaultLakehouse': True, "addressLine1": dict['addressLine1'] })
            geometryKey = int(geometryKey) if geometryKey != 'None' else None
    else:
        createdDateTimeStamp = datetime.utcnow()

    if entity == 'Currency':  
        staged_rows = [(nextValue,dict['CurrencyCode'],None, float(dict['ConversionRate']),
                       False, datetime.now(), None)]
    elif entity == 'Customer':
        emailAddress =  dict['EmailAddress'] if dict['EmailAddress']!= None and dict['EmailAddress']!=''else None  
        phone =    dict['Phone'] if dict['Phone']!= None and dict['Phone']!=''else None  
        addressLine1 =  dict['AddressLine1'] if dict['AddressLine1']!= None and dict['AddressLine1']!=''else None    
        staged_rows = [(nextValue,geometryKey, dict['CustomerCode'], None,None,None,None,None,
                            emailAddress,None,None,None,None,None,None,None,
                            addressLine1, phone,None,'Company', dict['CompanyName'],
                            False, createdDateTimeStamp,None)]
    elif entity == 'Store':
        numEmployees =   int(dict['EmployeeCount']) if dict['EmployeeCount']!= None and dict['EmployeeCount']!=''else None
        latitude =   float(dict['Latitude']) if dict['Latitude']!= None and dict['Latitude']!=''else None 
        longitude =  float(dict['Longitude']) if dict['Longitude']!= None and dict['Longitude']!=''else None
        storeDescription =  dict['StoreDescription'] if dict['StoreDescription']!= None and dict['StoreDescription']!=''else None  
        storeFax =  dict['StoreFax'] if dict['StoreFax']!= None and dict['StoreFax']!=''else None  
        storePhone =  dict['StorePhone'] if dict['StorePhone']!= None and dict['StorePhone']!=''else None  
        addressLine1 =  dict['AddressLine1'] if dict['AddressLine1']!= None and dict['AddressLine1']!=''else None
        customerKeyRetrieved = getMappedKey(dict['CustomerKey'], dict, 'Customer')  
        customerKey =   int(customerKeyRetrieved) if customerKeyRetrieved!= None else None
        staged_rows = [(nextValue, dict['StoreCode'], geometryKey, int(customerKey), dict['StoreTypeID'] ,dict['StoreType'], dict['StoreName'],storeDescription,
                            storePhone,storeFax,addressLine1,
                            numEmployees, longitude, latitude, False,
                            createdDateTimeStamp,None)]    
    elif entity == 'Product':
        availableForSaleDate = None
        if 'AvailableForSaleDate' in dict and dict['AvailableForSaleDate']!='':
            if str(dict['AvailableForSaleDate']).isdigit():
                availableForSaleDate = datetime.utcfromtimestamp(int(dict['AvailableForSaleDate']) / 1000).date()
                print(availableForSaleDate.strftime('%Y-%m-%d'))
            else:
                availableForSaleDate = parser.parse(dict['AvailableForSaleDate']).date()
        stopSaleDate = None
        if 'StopSaleDate' in dict and dict['StopSaleDate']!='':
            if str(dict['StopSaleDate']).isdigit():
                stopSaleDate = datetime.utcfromtimestamp(int(dict['StopSaleDate']) / 1000).date()
                print(stopSaleDate.strftime('%Y-%m-%d'))
            else:
                stopSaleDate = parser.parse(dict['StopSaleDate']).date()
        currencyKey = getCurrencyKey(dict)
        productSubcategoryKey = getSubCategory(dict['ProductSubcategoryKey'])
        manufacturer =  dict['Manufacturer'] if dict['Manufacturer']!= None and dict['Manufacturer']!='' else None  
        brandName =  dict['BrandName'] if dict['BrandName']!= None and dict['BrandName']!='' else None  
        classID =  int(dict['ClassID']) if dict['ClassID']!= None and dict['ClassID']!=-1 else None 
        className =  dict['ClassName'] if dict['ClassName']!= None and dict['ClassName']!='' else None 
        colorID =  int(dict['ColorID']) if dict['ColorID']!= None and dict['ColorID']!=''else None  
        colorName =  dict['ColorName'] if dict['ColorName']!= None and dict['ColorName']!='' else None
        size =  dict['Size'] if dict['Size']!= None and dict['Size']!='' else None   
        sizeUnitMeasureID =  int(dict['SizeUnitMeasureID']) if dict['SizeUnitMeasureID']!= None and dict['SizeUnitMeasureID']!='' else None  
        sizeUnitMeasureName =  dict['SizeUnitMeasureName'] if dict['SizeUnitMeasureName']!= None and dict['SizeUnitMeasureName']!='' else None  
        weight =  float(dict['Weight']) if dict['Weight']!= None and dict['Weight']!='' else None  
        weightUnitMeasureID =  int(dict['WeightUnitMeasureID']) if dict['WeightUnitMeasureID']!= None and dict['WeightUnitMeasureID']!='' else None  
        weightUnitMeasureName =  dict['WeightUnitMeasureName'] if dict['WeightUnitMeasureName']!= None and dict['WeightUnitMeasureName']!='' else None  
        staged_rows = [(nextValue, dict['ProductCode'], dict['ProductName'] ,dict['ProductDescription'], productSubcategoryKey, manufacturer,
                            brandName,classID,className,
                            colorID, colorName, size, sizeUnitMeasureID,
                            sizeUnitMeasureName, weight, weightUnitMeasureID, weightUnitMeasureName,
                            None, None, currencyKey, availableForSaleDate, stopSaleDate, dict['Status'], False,
                            createdDateTimeStamp,None)] 
        print(staged_rows)
    elif entity == 'SalesOrders':
        dateKey = createDateKeyIfNeeded(dict['DateKey'])
        currencyKey = getCurrencyKey(dict)
        storeKeyRetrieved = getMappedKey(dict['StoreKey'], dict, 'Store')
        storeKey = int(storeKeyRetrieved) if storeKeyRetrieved!= None else None
        productKeyRetrieved = getMappedKey(dict['ProductKey'], dict, 'Product')
        productKey = int(productKeyRetrieved) if productKeyRetrieved!= None else None
        customerKeyRetrieved = getMappedKey(dict['CustomerKey'], dict, 'Customer')
        customerKey = int(customerKeyRetrieved) if customerKeyRetrieved!= None else None
        salesAmount = decimal.Decimal(int(dict['SalesQuantity']) * decimal.Decimal(dict['UnitPrice']))
        totalCost = decimal.Decimal(int(dict['SalesQuantity']) * decimal.Decimal(dict['UnitCost']))
        staged_rows = [(nextValue, dateKey, storeKey, productKey, currencyKey,
                            customerKey,dict['SalesOrderNumber'],int(dict['SalesOrderLineNumber']),int(dict['SalesQuantity']),
                            salesAmount, totalCost,
                            decimal.Decimal(dict['UnitCost']), decimal.Decimal(dict['UnitPrice']), False,
                            createdDateTimeStamp,None)]
    elif entity == 'Sales':
        staged_rows = [(nextValue, dict['DateKey'], dict['StoreKey'],dict['ProductKey'], dict['CurrencyKey'],dict['CustomerKey'],
                        decimal.Decimal(dict['UnitCost']), decimal.Decimal(dict['UnitPrice']), 
                        int(dict['SalesQuantity']), decimal.Decimal(dict['TotalCost']),
                        decimal.Decimal(dict['SalesAmount']), False, createdDateTimeStamp, None)]
    else:
       staged_rows = None

    print('Staged rows on insert:')
    print(staged_rows)

    return staged_rows


def getRowToUpdate(entity, dict, fabricId, operation):
    
    if entity!='Sales':
        if 'Latitude' in dict and 'Longitude' in dict:
            geometryKey = notebookutils.notebook.run("OmniSync_DE_NB_GeographyCDC", 90, 
                        {'useRootDefaultLakehouse': True, "latitude": dict['Latitude'], "longitude": dict['Longitude'] })
        elif 'addressLine1' in dict:
            geometryKey = notebookutils.notebook.run("OmniSync_DE_NB_GeographyCDC", 90, 
                        {'useRootDefaultLakehouse': True, "addressLine1": dict['addressLine1'] })

    print("SELECT " + getPrimaryKey(entity) + ",CreatedDate FROM OmniSync_DE_LH_320_Gold_Contoso.dbo." + entity + \
                   " WHERE " +  getPrimaryKey(entity) + "=" + fabricId)                 
        
    df = spark.sql("SELECT " + getPrimaryKey(entity) + ",CreatedDate FROM OmniSync_DE_LH_320_Gold_Contoso.dbo." + entity + \
                   " WHERE " +  getPrimaryKey(entity) + "=" + fabricId)

    
    df.show()
    if df.first() == None:
        raise Exception(entity + " with " +  getPrimaryKey(entity) + " " + fabricId + " has not been found.")

    keyValue = df.first()[getPrimaryKey(entity)]
    createdDateValue = df.first()['CreatedDate']

    if entity!='Sales':
        if 'UpdatedDate' in dict:
            if str(dict['UpdatedDate']).isdigit():
                updatedTimeStamp = datetime.utcfromtimestamp(int(dict['UpdatedDate']) / 1000)
                print(updatedTimeStamp.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                if str(dict['UpdatedDate'])!='':
                    updatedTimeStamp = parser.parse(dict['UpdatedDate'])
                else:
                    updatedTimeStamp = datetime.now()
    else:
        updatedTimeStamp = datetime.now()

    if entity == 'Currency':
        staged_rows = { "CurrencyCode": lit(dict['CurrencyCode']),
                        "ConversionRate": lit(dict['ConversionRate']),
                        "IsDeleted": lit(dict['IsDeleted']), "UpdatedDate":  lit(updatedTimeStamp)}      
    elif entity == 'Customer':
        if operation=='update':
            addressLine1 =  lit(dict['AddressLine1']) if dict['AddressLine1']!= None and dict['AddressLine1']!=''else lit(None).cast('string') 

            staged_rows = { "AddressLine1": addressLine1,
                            "IsDeleted": lit(False)}

            if 'UpdatedDate' in dict:
                staged_rows['UpdatedDate'] =  lit(updatedTimeStamp)

            if geometryKey != 'None':
                staged_rows['GeographyKey'] =  lit(int(geometryKey))

            rowsToUpdate = ['EmailAddress','Phone','CompanyName']
            for key,value in dict.items():
                if key in rowsToUpdate:   
                    staged_rows[key] = lit(value)
        elif operation=='delete' or operation=='undelete':
            isDeleted = True if operation == 'delete' else False
            staged_rows = { "IsDeleted": lit(isDeleted), 
                            "UpdatedDate":  lit(datetime.now())}
    elif entity == 'Store':
        if operation=='update':
            numEmployees = lit(int(dict['EmployeeCount'])) if dict['EmployeeCount']!= None and dict['EmployeeCount']!='' else lit(None).cast('integer')
            latitude = lit(float(dict['Latitude'])) if dict['Latitude']!= None and dict['Latitude']!='' else lit(None).cast('float') 
            longitude = lit(float(dict['Longitude'])) if dict['Longitude']!= None and dict['Longitude']!='' else lit(None).cast('float') 
            storeDescription = lit(dict['StoreDescription']) if dict['StoreDescription']!= None and dict['StoreDescription']!='' else lit(None).cast('string')  
            storeFax = lit(dict['StoreFax']) if dict['StoreFax']!= None and dict['StoreFax']!='' else lit(None).cast('string')  
            storePhone = lit(dict['StorePhone']) if dict['StorePhone']!= None and dict['StorePhone']!='' else lit(None).cast('string')  
            addressLine1 = lit(dict['AddressLine1']) if dict['AddressLine1']!= None and dict['AddressLine1']!='' else lit(None).cast('string')
            customerKey = lit(getMappedKey(dict['CustomerKey'], dict, 'Customer'))

            staged_rows = {"CustomerKey": customerKey,"StoreCode": lit(dict['StoreCode']),
                          "StoreTypeID": lit(dict['StoreTypeID']),"StoreType": lit(dict['StoreType']),
                          "StoreName": lit(dict['StoreName']),"StoreDescription": storeDescription,
                          "StorePhone": storePhone,"StoreFax": storeFax,
                          "AddressLine1": addressLine1,
                          "EmployeeCount": numEmployees,"Longitude": longitude,
                          "Latitude": latitude, "IsDeleted": lit(False),
                          "UpdatedDate":  lit(updatedTimeStamp)}

            if geometryKey != 'None':
                staged_rows['GeographyKey'] =  lit(int(geometryKey))

        elif operation=='delete' or operation=='undelete':
            isDeleted = True if operation == 'delete' else False
            staged_rows = { "IsDeleted": lit(isDeleted), 
                            "UpdatedDate":  lit(updatedTimeStamp)}        
    elif entity == 'Product':
        if operation=='update':
            staged_rows = { "IsDeleted": lit(False)}
            if 'CurrencyKey' in dict:
                currencyKey = getCurrencyKey(dict)
                staged_rows['CurrencyKey'] = lit(currencyKey)
            if 'ProductSubcategoryKey' in dict:
                productSubcategoryKey = getSubCategory(dict['ProductSubcategoryKey'])   
                staged_rows['ProductSubcategoryKey'] = lit(productSubcategoryKey)
            if 'AvailableForSaleDate' in dict and dict['AvailableForSaleDate']!='' and str(dict['AvailableForSaleDate']).isdigit():
                availableForSaleDate = datetime.utcfromtimestamp(int(dict['AvailableForSaleDate']) / 1000).date()
                print(availableForSaleDate.strftime('%Y-%m-%d'))
                staged_rows['AvailableForSaleDate'] = lit(availableForSaleDate)
            if 'StopSaleDate' in dict and dict['StopSaleDate']!='' and str(dict['StopSaleDate']).isdigit():
                availableStopSaleDate = datetime.utcfromtimestamp(int(dict['StopSaleDate']) / 1000).date()
                print(availableStopSaleDate.strftime('%Y-%m-%d'))
                staged_rows['StopSaleDate'] = lit(availableStopSaleDate)
            if 'UpdatedDate' in dict:
                staged_rows['UpdatedDate'] =  lit(updatedTimeStamp)
            rowsToUpdate = ['ProductCode','ProductName','ProductDescription','ProductSubcategoryKey','Manufacturer','BrandName','ClassName','ClassID',
                            'ColorID','ColorName','Size','SizeUnitMeasureID','SizeUnitMeasureName','Weight','WeightUnitMeasureID','WeightUnitMeasureName',
                            'Status','UnitPrice','UnitCost']
            for key,value in dict.items():  
                if key in rowsToUpdate: 
                    staged_rows[key] = lit(value)
        elif operation=='delete' or operation=='undelete':
            isDeleted = True if operation == 'delete' else False
            staged_rows = { "IsDeleted": lit(isDeleted), 
                            "UpdatedDate":  lit(datetime.now())}
    elif entity == 'SalesOrders':
        if operation=='update':
            dateKey = createDateKeyIfNeeded(dict['DateKey'])
            currencyKey = getCurrencyKey(dict)
            storeKey = getMappedKey(dict['StoreKey'], dict, 'Store')
            productKey = getMappedKey(dict['ProductKey'], dict, 'Product')
            customerKey = getMappedKey(dict['CustomerKey'], dict, 'Customer')
            salesAmount = decimal.Decimal(int(dict['SalesQuantity']) * decimal.Decimal(dict['UnitPrice']))
            totalCost = decimal.Decimal(int(dict['SalesQuantity']) * decimal.Decimal(dict['UnitCost']))
            staged_rows = { "DateKey": lit(dateKey),
                            "StoreKey": lit(storeKey),"ProductKey": lit(productKey),
                            "CurrencyKey": lit(currencyKey),"CustomerKey": lit(customerKey),
                            "SalesOrderNumber": lit(dict['SalesOrderNumber']),"SalesOrderLineNumber": lit(int(dict['SalesOrderLineNumber'])),
                            "SalesQuantity": lit(int(dict['SalesQuantity'])),"SalesAmount": lit(salesAmount),
                            "TotalCost": lit(totalCost),"UnitCost": lit(decimal.Decimal(dict['UnitCost'])),
                            "UnitPrice": lit(decimal.Decimal(dict['UnitPrice'])),"IsDeleted": lit(False)}
            if 'UpdatedDate' in dict:
                staged_rows['UpdatedDate'] =  lit(updatedTimeStamp)
        elif operation=='delete':
            staged_rows = { "IsDeleted": lit(True), 
                            "UpdatedDate":  lit(datetime.now())} 
    elif entity == 'Sales':
        if operation=='update':
            staged_rows = { "DateKey": lit(dict['DateKey']),
                            "StoreKey": lit(dict['StoreKey']),"ProductKey": lit(dict['ProductKey']),
                            "CurrencyKey": lit(dict['CurrencyKey']),"CustomerKey": lit(dict['CustomerKey']),
                            "UnitCost": lit(decimal.Decimal(dict['UnitCost'])),"UnitPrice": lit(decimal.Decimal(dict['UnitPrice'])),
                            "SalesQuantity": lit(int(dict['SalesQuantity'])),
                            "SalesAmount": lit(decimal.Decimal(dict['SalesAmount'])),"TotalCost": lit(decimal.Decimal(dict['TotalCost'])),
                            "IsDeleted": lit(False)}
            staged_rows['UpdatedDate'] =  lit(updatedTimeStamp)
        elif operation=='delete':
            staged_rows = { "IsDeleted": lit(True), 
                            "UpdatedDate":  lit(datetime.now())}
    else:
        staged_rows = None
    
    print('Staged rows on update:')
    print(staged_rows)
    
    return staged_rows

def getSchema(entity):
    if entity == 'Currency':
        return StructType([ \
                StructField("CurrencyKey",IntegerType(),False), \
                StructField("CurrencyCode",StringType(),False), \
                StructField("CurrencyDescription",StringType(),True), \
                StructField("ConversionRate",DoubleType(),True), \
                StructField("IsDeleted", BooleanType(),False), \
                StructField("CreatedDate", TimestampType(), False), \
                StructField("UpdatedDate", TimestampType(), True)
            ])
    elif entity == 'Customer':
        return StructType([ \
                StructField("CustomerKey",IntegerType(),False), \
                StructField("GeographyKey",IntegerType(),True), \
                StructField("CustomerCode",StringType(),False), \
                StructField("FirstName",StringType(),True), \
                StructField("LastName",StringType(),True), \
                StructField("BirthDate",DateType(),True), \
                StructField("MaritalStatus",StringType(),True), \
                StructField("Gender",StringType(),True), \
                StructField("EmailAddress",StringType(),True), \
                StructField("YearlyIncome",DecimalType(19,4),True), \
                StructField("TotalChildren",ByteType(),True), \
                StructField("NumberChildrenAtHome",ByteType(),True), \
                StructField("Education",StringType(),True), \
                StructField("Occupation",StringType(),True), \
                StructField("HouseOwnerFlag",StringType(),True), \
                StructField("NumberCarsOwned",ByteType(),True), \
                StructField("AddressLine1",StringType(),True), \
                StructField("Phone",StringType(),True), \
                StructField("DateFirstPurchase",DateType(),True), \
                StructField("CustomerType",StringType(),False), \
                StructField("CompanyName",StringType(),True), \
                StructField("IsDeleted", BooleanType(),False), \
                StructField("CreatedDate", TimestampType(), False), \
                StructField("UpdatedDate", TimestampType(), True)
            ])
    elif entity == 'Product':
        return StructType([ \
                StructField("ProductKey",IntegerType(),False), \
                StructField("ProductCode",StringType(),False), \
                StructField("ProductName",StringType(),False), \
                StructField("ProductDescription",StringType(),True), \
                StructField("ProductSubcategoryKey",IntegerType(),False), \
                StructField("Manufacturer",StringType(),True), \
                StructField("BrandName",StringType(),True), \
                StructField("ClassID",IntegerType(),True), \
                StructField("ClassName",StringType(),True), \
                StructField("ColorID",IntegerType(),True), \
                StructField("ColorName",StringType(),True), \
                StructField("Size",StringType(),True), \
                StructField("SizeUnitMeasureID",IntegerType(),True), \
                StructField("SizeUnitMeasureName",StringType(),True), \
                StructField("Weight",DoubleType(),True), \
                StructField("WeightUnitMeasureID",IntegerType(),True), \
                StructField("WeightUnitMeasureName",StringType(),True), \
                StructField("UnitCost",DecimalType(19,4),True), \
                StructField("UnitPrice",DecimalType(19,4),True), \
                StructField("CurrencyKey",IntegerType(),False), \
                StructField("AvailableForSaleDate",DateType(),True), \
                StructField("StopSaleDate",DateType(),True), \
                StructField("Status",StringType(),True), \
                StructField("IsDeleted", BooleanType(),False), \
                StructField("CreatedDate", TimestampType(), False), \
                StructField("UpdatedDate", TimestampType(), True)
            ])
    elif entity == 'SalesOrders':
        return StructType([ \
                StructField("SalesOrdersKey",IntegerType(),False), \
                StructField("DateKey",DateType(),False), \
                StructField("StoreKey",IntegerType(),False), \
                StructField("ProductKey",IntegerType(),False), \
                StructField("CurrencyKey",IntegerType(),False), \
                StructField("CustomerKey",IntegerType(),False), \
                StructField("SalesOrderNumber",StringType(),False), \
                StructField("SalesOrderLineNumber",IntegerType(),False), \
                StructField("SalesQuantity",IntegerType(),False), \
                StructField("SalesAmount",DecimalType(19,4),False), \
                StructField("TotalCost",DecimalType(19,4),False), \
                StructField("UnitCost",DecimalType(19,4),False), \
                StructField("UnitPrice",DecimalType(19,4),False), \
                StructField("IsDeleted", BooleanType(),False), \
                StructField("CreatedDate", TimestampType(), False), \
                StructField("UpdatedDate", TimestampType(), True)
            ])
    elif entity == 'Sales':
        return StructType([ \
                StructField("SalesKey",IntegerType(),False), \
                StructField("DateKey",DateType(),False), \
                StructField("StoreKey",IntegerType(),False), \
                StructField("ProductKey",IntegerType(),False), \
                StructField("CurrencyKey",IntegerType(),False), \
                StructField("CustomerKey",IntegerType(),False), \
                StructField("UnitCost",DecimalType(19,4),False), \
                StructField("UnitPrice",DecimalType(19,4),False), \
                StructField("SalesQuantity",IntegerType(),False), \
                StructField("TotalCost",DecimalType(19,4),False), \
                StructField("SalesAmount",DecimalType(19,4),True), \
                StructField("IsDeleted", BooleanType(),False), \
                StructField("CreatedDate", TimestampType(), False), \
                StructField("UpdatedDate", TimestampType(), True)
            ])
    elif entity == 'Store':
        return StructType([ \
                StructField("StoreKey",IntegerType(),False), \
                StructField("StoreCode",StringType(),False), \
                StructField("GeographyKey",IntegerType(),True), \
                StructField("CustomerKey",IntegerType(),True), \
                StructField("StoreTypeID",IntegerType(),False), \
                StructField("StoreType",StringType(),False), \
                StructField("StoreName",StringType(),False), \
                StructField("StoreDescription",StringType(),True), \
                StructField("StorePhone",StringType(),True), \
                StructField("StoreFax",StringType(),True), \
                StructField("AddressLine1",StringType(),True), \
                StructField("EmployeeCount",IntegerType(),True), \
                StructField("Longitude",DoubleType(),True), \
                StructField("Latitude",DoubleType(),True), \
                StructField("IsDeleted", BooleanType(),False), \
                StructField("CreatedDate", TimestampType(), False), \
                StructField("UpdatedDate", TimestampType(), True)
            ])
    elif entity == 'MasterDataMapping':
        return StructType([ \
                StructField("MasterDataMappingKey",IntegerType(),False), \
                StructField("FabricId",StringType(),False), \
                StructField("SalesForceId",StringType(),True), \
                StructField("SAPId",StringType(),True), \
                StructField("D365Id",StringType(),True), \
                StructField("Entity",StringType(),False), \
                StructField("Name",StringType(),True), \
                StructField("IsDeleted", BooleanType(),False), \
                StructField("CreatedDate", TimestampType(), False), \
                StructField("UpdatedDate", TimestampType(), True)
            ])
    elif entity == 'Date':
        return StructType([ \
                StructField("DateKey",DateType(),False), \
                StructField("CalendarYear",IntegerType(),False), \
                StructField("CalendarMonthLabel",StringType(),False), \
                StructField("FiscalQuarterLabel",StringType(),False), \
                StructField("IsDeleted", BooleanType(),False), \
                StructField("CreatedDate", TimestampType(), False), \
                StructField("UpdatedDate", TimestampType(), True)
            ])
        
    else:
       logger.error('Schema for ' + entity + ' not found.')
       print('Schema for ' + entity + ' not found.')
       return None

def getMappedKey(id, dict, entity):

    if 'SalesForceId' in dict:
        df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping WHERE SalesForceId='"+ id + "' AND Entity='" + entity + "'" )
        print("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping WHERE SalesForceId='"+ id + "' AND Entity='" + entity + "'" )
    elif 'SAPId' in dict:
        df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping WHERE SAPId='"+ id  + "' AND Entity='" + entity + "'" )
        print("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping WHERE SAPId='"+ id  + "' AND Entity='" + entity + "'" )
    elif 'D365Id' in dict:
        df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping WHERE D365Id='"+ id  + "' AND Entity='" + entity + "'" )
        print("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping WHERE D365Id='"+ id  + "' AND Entity='" + entity + "'" )

    if df.count()>0:       
        fabricId =  df.first()['FabricId'] 
        print('Found a mapped key for id: ' + id + ' and Entity: ' + entity + ' with fabricId: ' + fabricId)
        return fabricId
    else:
        print('NOT Found a mapped key for id: ' + id + ' and Entity: ' + entity)

def getCurrencyKey(dict):
    if 'SalesForceId' in dict:
        df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Currency WHERE CurrencyCode='"+ dict['CurrencyKey'] + "'" )

        if df.count() == 0:
            raise Exception('Currency not found: ' + dict['CurrencyKey'])
        else:
            return df.first()['CurrencyKey']
    elif 'D365Id' in dict:
        fabricId = getMappedKey(dict['CurrencyKey'], dict, 'Currency')
        if fabricId is None:
            raise Exception('Currency not found: ' + dict['CurrencyKey'])
        else:
            return fabricId

def getSubCategory(category):
   return int(category)

def createDateKeyIfNeeded(date):
    
    if str(date).isdigit():
        dateToCheck = datetime.utcfromtimestamp(int(date) / 1000)
        print(dateToCheck)
    else:
        dateToCheck = datetime.strptime(str(date), '%Y-%m-%d').date()

    sDate = dateToCheck.strftime('%Y-%m-%d')
 
    df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Date WHERE DateKey='"+ sDate + "'" )

    if df.count() == 0:
        year = dateToCheck.year
        quarter = f'Q{(dateToCheck.month-1)//3+1}'
        month = dateToCheck.strftime("%b")

        staged_rows = { "DateKey": lit(sDate), "CalendarYear": lit(year),
                        "CalendarMonthLabel": lit(month), "FiscalQuarterLabel": lit(quarter),
                        "IsDeleted": lit(False),"CreatedDate": lit(datetime.now())}  

        insertEntity(staged_rows, 'Date')  
    else:
        return df.first()['DateKey']

def checkIfInsertNeeded(dict, entity):

    salesForceId = dict['SalesForceId'] if 'SalesForceId' in dict else ""
    SAPId = dict['SAPId'] if 'SAPId' in dict else ""
    D365Id = dict['D365Id'] if 'D365Id' in dict else ""

    df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping " + \
                   "WHERE (SalesForceId='" + salesForceId + "' OR SalesForceId IS NULL) AND " + \
                   "(D365Id='" + D365Id + "' OR D365Id IS NULL) AND " + \
                   "(SAPId=' " + SAPId + "' OR SAPId IS NULL) AND Entity='" + entity + "'" ) 

    print("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping " + \
                   "WHERE (SalesForceId='" + salesForceId + "' OR SalesForceId IS NULL) AND " + \
                   "(D365Id='" + D365Id + "' OR D365Id IS NULL) AND " + \
                   "(SAPId=' " + SAPId + "' OR SAPId IS NULL) AND Entity='" + entity + "'" )

    print(df)

    return False if df.count() > 0 else True

def insertEntity(row, entity):
    staged_df = spark.createDataFrame(row,getSchema(entity)) \
                        .write.mode("append").format("delta").saveAsTable(entity)
        
def getNaturalKey(entity):
    if entity == 'Currency':
        return 'CurrencyCode'
    elif entity == 'Customer':
        return 'CustomerCode'
    elif entity == 'Store':
        return 'StoreCode'
    elif entity == 'Product':
        return 'ProductCode'
    else: 
        return entity + 'Key'

def getPrimaryKey(entity):
    return entity + 'Key'

def getPrimaryKeyValue(rows):
    return str(rows[0][0])

def fixJson(jsonString):
    if jsonString is None:
        return
    jsonString = jsonString.replace("\t","")
    jsonString = jsonString.replace("\r\n","")
    print(jsonString)   
    return jsonString

def mergeMasterDataMapping(dict, fabricId, entity, key, isDeleted):
    salesForceId = dict['SalesForceId'] if 'SalesForceId' in dict else ""
    SAPId = dict['SAPId'] if 'SAPId' in dict else ""
    D365Id = dict['D365Id'] if 'D365Id' in dict else ""

    if salesForceId != None or SAPId!= None: 
       df_mapping = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping " + \
                   "WHERE (SalesForceId='" + salesForceId + "' OR SalesForceId IS NULL) AND " + \
                   "(D365Id='" + D365Id + "' OR D365Id IS NULL) AND " + \
                   "(SAPId='" + SAPId + "' OR SAPId IS NULL) AND Entity='" + entity + "'" ) 
    else:
        raise Exception("No SAPId, D365Id or SalesForceId on CDC row")

    if df_mapping.count() == 0 :

        df_mapping_next = spark.sql("SELECT NVL(MAX(MasterDataMappingKey),0)+1 AS NextKey FROM \
                                        OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping")
        nextValueDataMapping = df_mapping_next.first()['NextKey']
        logger.info('Next value for MasterDataMapping is '+ str(nextValueDataMapping))
        print('Next value for MasterDataMapping is '+ str(nextValueDataMapping))

        if salesForceId != None: 
            mapping_rows = [(nextValueDataMapping, fabricId, salesForceId , None, None,\
                              entity, key , False, datetime.now(), None)]
        elif SAPId != None: 
            mapping_rows = [(nextValueDataMapping, fabricId, None, SAPId , None, \
                             entity, key , False, datetime.now(),None)]
        elif D365Id != None: 
            mapping_rows = [(nextValueDataMapping, fabricId, None, None , D365Id, \
                             entity, key , False, datetime.now(),None)]

        print(mapping_rows)

        spark.createDataFrame(mapping_rows, getSchema('MasterDataMapping')) \
             .write.mode("append").format("delta").saveAsTable('MasterDataMapping')
        logger.info('Created MasterDataMapping: with id ' + getPrimaryKeyValue(mapping_rows))
        print('Created MasterDataMapping: with id ' + getPrimaryKeyValue(mapping_rows))

    else:

        logger.info("Mapping with SalesForceId / SAPId / D365: " + salesForceId +" / " +  SAPId +" / " +  D365Id + \
                    " which is already in the system. Updating...")
        print("Mapping with SalesForceId / SAPId / D365: " + salesForceId +" / " +  SAPId +" / " +  D365Id + \
                    " which is already in the system. Updating...")

        df_row = df_mapping.first()

        id = df_row['MasterDataMappingKey']

        if (salesForceId == None and df_row['SalesForceId']!=None):
            mapping_rows['SalesForceId'] = df_row['SalesForceId']

        if (SAPId == None and  df_row['SAPId']!=None):
            mapping_rows['SAPId'] = df_row['SAPId']

        if (D365Id == None and  df_row['D365Id']!=None):
            mapping_rows['D365Id'] = df_row['D365Id']  

        createdDateTimeStamp = df_row['CreatedDate']

        mapping_rows = {"FabricId": lit(df_row['FabricId']),
                        "Entity": lit(df_row['Entity']),
                        "Name": lit(df_row['Name']), 
                        "IsDeleted": lit(isDeleted),
                        "CreatedDate": lit(createdDateTimeStamp), 
                        "UpdatedDate":  lit(datetime.now())}

        logger.info('Update key to check on MasterDataMapping : ' + str(id))
        print('Update key to check on MasterDataMapping: ' + str(id))
        deltaTable = DeltaTable.forPath(spark, 'Tables/dbo/MasterDataMapping')

        deltaTable.update(
            condition = col('MasterDataMappingKey') == id,
            set = mapping_rows
        )

def getFabricIdFromSalesForceOrSAPOrD365(dict, entity):

    salesForceId = dict['SalesForceId'] if 'SalesForceId' in dict else ""
    SAPId = dict['SAPId'] if 'SAPId' in dict else "" 
    D365Id = dict['D365Id'] if 'D365Id' in dict else ""         
    logger.info("Create key to check with SalesForceId: " + salesForceId + " and/or SAPId: " +  SAPId + " and/or D365Id: " + D365Id )
    print("Key to check with SalesForceId: " + salesForceId + " and/or SAPId: " +  SAPId + " and/or D365Id: " + D365Id)

    sqlStatement = "SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping " + \
                   "WHERE (SalesForceId='" + salesForceId + "' OR SalesForceId IS NULL) AND " + \
                   "(D365Id='" + D365Id + "' OR D365Id IS NULL) AND " + \
                   "(SAPId='" + SAPId + "' OR SAPId IS NULL) AND Entity='" + entity + "'"

    df = spark.sql(sqlStatement)  

    fabricId  = None
    if df.count() > 0:
        fabricId = df.first()['FabricId']

    return fabricId


def handleException(logger,  ex: Exception):
    # By this way we can know about the type of error occurring
    ex_t = type(ex).__name__
    err = str(ex)
    err_msg = f'[{ex_t}] - {err}'
    print(err_msg)
    logger.error(err_msg)
    # go through the trackback lines and individually add those to the log as an error
    for l in traceback.format_exc().splitlines():
        logger.error(l)
        print(l)

def mergeOperation(operation, logger, entity, dict, pk, naturalKey):
    isDeleted = False
    if operation=='delete':
        isDeleted = True
    strOperation= operation.capitalize()
    fabricId = getFabricIdFromSalesForceOrSAPOrD365(dict, entity)
    if (fabricId == None):
        logger.warn("FabricId does not exist on the system. " + strOperation + " not valid.")
        print("FabricId does not exist on the system. " + strOperation + " not valid.")
        return
    logger.info(strOperation + ' key to check: ' + fabricId)
    print(strOperation + ' key to check: ' + fabricId)
    deltaTable = DeltaTable.forPath(spark, 'Tables/dbo/'+ entity)
    entityRow = getRowToUpdate(entity, dict, fabricId, operation)
    if (entityRow == None):
        logger.warn("Could not decode entity: " + entity )
        print("Could not decode entity: " + entity )
        return

    deltaTable.update(
        condition = col(pk) == fabricId,
        set = entityRow
    )
    logger.info(strOperation + ' done with id: ' + fabricId)
    print(strOperation + ' done with id: ' + fabricId)
    mergeMasterDataMapping(dict, fabricId, entity, dict[naturalKey], isDeleted)

    if (operation=='update' or operation=='delete') and entity=='SalesOrders':
        materializeSales(operation, dict)

    if (entity=='Customer' and operation=='delete'):
        deleteRelatedCustomerEntities(logger,fabricId)

def materializeSales(operation, dict):

    currencyKey = str(getCurrencyKey(dict))
    customerKey = getMappedKey(dict['CustomerKey'], dict, 'Customer')
    storeKeyRetrieved = getMappedKey(dict['StoreKey'], dict, 'Store')
    storeKey = str(storeKeyRetrieved) if storeKeyRetrieved!= None else None
    productKeyRetrieved = getMappedKey(dict['ProductKey'], dict, 'Product')
    productKey = str(productKeyRetrieved) if productKeyRetrieved!= None else None

    delete_sales = spark.sql("""
            DELETE 
            FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Sales """ +
            "WHERE DateKey='" + dict['DateKey'] + "' AND " +
            "      StoreKey='" + storeKey + "' AND  ProductKey='" + productKey + "' AND " + 
            "      CurrencyKey='" + currencyKey + "' AND  CustomerKey='" + customerKey + "'")

    sales = spark.sql("""
            SELECT DateKey,StoreKey,ProductKey,CurrencyKey,CustomerKey,
                    CAST(UnitCost AS DECIMAL(19,4)) AS UnitCost,
                    CAST(UnitPrice AS DECIMAL(19,4)) AS UnitPrice,
                    INT(SUM(SalesQuantity)) AS SalesQuantity,
                    CAST(SUM(SalesQuantity * UnitCost) AS DECIMAL(19,4)) AS TotalCost,
                    CAST(SUM(SalesQuantity * UnitPrice) AS DECIMAL(19,4)) AS SalesAmount,
                    CURRENT_TIMESTAMP AS CreatedDate,
                    CURRENT_TIMESTAMP AS UpdatedDate
              FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.SalesOrders """ +
             "WHERE IsDeleted=False AND DateKey='" + dict['DateKey'] + "' AND " +
             "      StoreKey='" + storeKey + "' AND  ProductKey='" + productKey + "' AND " +
             "      CurrencyKey='" + currencyKey + "' AND  CustomerKey='" + customerKey + "' "
             "GROUP BY DateKey,StoreKey,ProductKey,CurrencyKey,CustomerKey,UnitCost,UnitPrice ")

    sales.write.mode("append").format("delta").saveAsTable('Sales')

def deleteRelatedCustomerEntities(logger,customerKey):
    
    df_stores = spark.sql("SELECT StoreKey FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Store \
                           WHERE CustomerKey='" + str(customerKey) + "'").collect()

    df_stores_update = spark.sql("Update OmniSync_DE_LH_320_Gold_Contoso.dbo.Store \
                                  SET IsDeleted=True WHERE CustomerKey='" + str(customerKey) + "'")

    for store_row in df_stores:
        logger.info(store_row)
        print(store_row)

        df_stores_mapping = spark.sql("Update OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping \
                                  SET IsDeleted=True WHERE FabricId='" + str(store_row.StoreKey) + "'")

    df_salesorders = spark.sql("SELECT SalesOrdersKey FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.SalesOrders \
                           WHERE CustomerKey='" + str(customerKey) + "'").collect()

    df_salesorders_update = spark.sql("Update OmniSync_DE_LH_320_Gold_Contoso.dbo.SalesOrders \
                                  SET IsDeleted=True WHERE CustomerKey='" + str(customerKey) + "'")

    for salesorder_row in df_salesorders:
        logger.info(salesorder_row)
        print(salesorder_row)
        df_salesorder_update = spark.sql("Update OmniSync_DE_LH_320_Gold_Contoso.dbo.MasterDataMapping \
                                  SET IsDeleted=True WHERE FabricId='" + str(salesorder_row.SalesOrdersKey) + "'")

    delete_sales = spark.sql("""
            DELETE 
            FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Sales """ +
            "WHERE CustomerKey='" + str(customerKey) + "'")
        


# In[5]:




if __name__ == "__main__":
    spark = SparkSession.builder.appName("MyApp").getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    logger = sc._jvm.org.apache.log4j.LogManager.getLogger("com.omnisync.Logger")

    externalCDCSchema = spark.read.parquet("Tables/dbo/ExternalCDCv1").schema

    df = spark.readStream.schema(externalCDCSchema).format("parquet").option("path", "Tables/dbo/ExternalCDCv1").load()

    def sendToSinkTable(df, epoch_id):
        
        print('------------------------------Stream received---------------------------------------')
        
        # try:
        dataCollect = df.collect()
        for row in dataCollect:
            logger.info(row)
            try:
                operation=row.Operation.lower()
                print('Operation:' + operation)
                entity=row.Entity
                print('Entity:' + entity)
                values = row.Values
                fixedValues=fixJson(values)
                if fixedValues is None:
                    print("Values on row are null...")
                    continue
                dict = json.loads(fixedValues)
                naturalKey = getNaturalKey(entity)
                pk = getPrimaryKey(entity) 
                salesForceId = dict['SalesForceId'] if 'SalesForceId' in dict else ""
                SAPId = dict['SAPId'] if 'SAPId' in dict else "" 
                D365Id = dict['D365Id'] if 'D365Id' in dict else "" 
                # Default natural key to SalesForceId or SAPId or D365
                if naturalKey not in dict:
                    if 'SalesForceId' in dict:
                        naturalKey =  'SalesForceId'
                    elif 'SAPId' in dict:
                        naturalKey =  'SAPId'
                    elif 'D365Id' in dict:
                        naturalKey =  'D365Id'

                if operation == 'create':                        
                    logger.info("Create key to check with SalesForceId: " + salesForceId + " and/or SAPId: " +  SAPId + " and/or D365Id: " +  D365Id )
                    print("Create key to check with SalesForceId: " + salesForceId + " and/or SAPId: " +  SAPId + " and/or D365Id: " +  D365Id )

                    entityRow = getRowToCreate(entity, dict)
                    if (entityRow == None):
                        logger.info("Could not decode entity: " + entity )
                        print("Could not decode entity: " + entity )
                        continue

                    if checkIfInsertNeeded(dict, entity):
                        insertEntity(entityRow, entity)
                        logger.info('Created ' + entity + " with id: " + getPrimaryKeyValue(entityRow))
                        print('Created ' + entity + " with id: " + getPrimaryKeyValue(entityRow))
                        mergeMasterDataMapping(dict, getPrimaryKeyValue(entityRow), entity, dict[naturalKey], False)
                        if entity=='SalesOrders':
                            materializeSales(operation, dict)
                    else:
                        logger.warn(entity + ' with id to check with SalesForceId: ' + salesForceId + ' and/or SAPId: ' +  SAPId + ' and/or D365Id: ' +  D365Id  + \
                                    ' already in the system. Skipping insert...')
                        print(entity + ' with id to check with SalesForceId: ' + salesForceId + ' and/or SAPId: ' +  SAPId + ' and/or D365Id: ' +  D365Id  + \
                                    ' already in the system. Skipping insert...')
                        
                elif operation == 'update' or operation == 'delete':
                    mergeOperation(operation, logger, entity, dict, pk, naturalKey)             
                else:
                    logger.error('Error. Operation not recognized: ' + operation)
                    print('Error. Operation not recognized: ' + operation)
            except Exception as ex:
                print("-----------------------Error-------------------------")
                handleException(logger, ex)

    logger.info('------------------------------Starting---------------------------------------')
    print('------------------------------Starting---------------------------------------')

    df.writeStream \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .option("checkpointLocation","Files/__cdc_checkpointv10") \
        .format("delta") \
        .foreachBatch(sendToSinkTable) \
        .start() \
        .awaitTermination()

