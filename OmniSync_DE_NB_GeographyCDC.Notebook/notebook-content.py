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
# META       "known_lakehouses": [
# META         {
# META           "id": "706dc789-a524-424c-8dc3-1ec4ec5f4e1a"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "4d7e0d58-dbbd-aef9-4745-975b73f3f167",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

latitude = None
longitude = None
addressline1 = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
from datetime import datetime 
from requests.structures import CaseInsensitiveDict
import urllib.parse

logger = sc._jvm.org.apache.log4j.LogManager.getLogger("com.omnisync.Logger")

if (latitude == None and longitude == None) or (latitude == '' and longitude == '') or \
    addressline1 == None or addressline1 == '':
    notebookutils.notebook.exit(None)

key_vault_uri = 'https://kv-omnisync-prod-ne-09.vault.azure.net/'
secret_name = 'geoapi-secret'

# secret = notebookutils.credentials.getSecret(key_vault_uri, secret_name)

secret = 'd978ae07dc0545738d559385dc435376'

if (latitude!=None and longitude!=None and latitude!='' and longitude!=''):
    url = "https://api.geoapify.com/v1/geocode/reverse?lat="+ latitude + "&lon=" + longitude +"&apiKey=" + secret
else:
    url = "https://api.geoapify.com/v1/geocode/search?text="+ urllib.parse.quote(addressline1) +"&apiKey=" + secret

headers = CaseInsensitiveDict()
headers["Accept"] = "application/json"

res = requests.get(url, headers=headers)

logger.info('Status code: ' + str(res.status_code))
logger.info(res.content)

if res.status_code != 200 :
    notebookutils.notebook.exit(None)

json = res.json()

print(json)

json_data = json['features'][0]['properties']

if 'country' not in json_data:
    logger.info('Country not found in decoded web service. Exiting')
    notebookutils.notebook.exit(None)

country = json_data['country']
state = json_data['state']
logger.info('Country: ' + country)
print('Country: ' + country)
logger.info('State: ' + state)
print('State: ' + state)

if 'city' in json_data:
    city = json_data['city']
    logger.info('City: ' + city)
    print('City: ' + city)

if 'street' in json_data:
    street = json_data['street']
    logger.info('Street: ' + street)
    print('Street: ' + street)

df_continent = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Continent WHERE Country LIKE '%" + country + "%'")
if df_continent.count() == 0:
    logger.info("Continent not found for country: " + country + ". Exiting...")
    print("Continent not found for country: " + country + ". Exiting...")
    notebookutils.notebook.exit(None)

continent = df_continent.first()['Continent']
logger.info('Continent: ' + continent)
print('Continent: ' + continent)

#Country
df = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Geography WHERE GeographyType='Country/Region' \
                AND RegionCountryName LIKE'%" + country + "%'")
if df.count() == 0:
    logger.info("Country not found: " + country + ". Creating it...")
    print("Country not found: " + country + ". Creating it...")
    df_next = spark.sql("SELECT NVL(MAX(GeographyKey),0)+1 AS NextGeoKey FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Geography")
    nextValue = df_next.first()['NextGeoKey']
    new_country_df_row = [(nextValue,'Country/Region', continent, None, None, country, False, datetime.now(), datetime.now())]
    new_country_df = spark.createDataFrame(new_country_df_row, df.schema)
    df = df.union(new_country_df)
    df.write.mode("append").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.Geography")

#State
df_state = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Geography WHERE GeographyType='State/Province' \
                AND ContinentName='" + continent + "' AND RegionCountryName LIKE '%" + country + "%' \
                AND StateProvinceName LIKE '%" + state + "%'")
if df_state.count() == 0:
    logger.info("State/Province not found: " + state + ". Creating it...")
    print("State/Province not found: " + state + ". Creating it...")
    df_next = spark.sql("SELECT NVL(MAX(GeographyKey),0)+1 AS NextGeoKey FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Geography")
    geoKey = nextValue = df_next.first()['NextGeoKey']
    new_state_df_row = [(nextValue, 'State/Province', continent, None, state, country, False, datetime.now(), datetime.now())]
    new_state_df = spark.createDataFrame(new_state_df_row, df.schema)
    df_state = df_state.union(new_state_df)
    df_state.write.mode("append").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.Geography")
else:
    logger.info("State/Province found: " + state)
    print("State/Province found: " + state)
    df_state.show()

if city == None:
    notebookutils.notebook.exit(geoKey)   

#City
df_city = spark.sql("SELECT * FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Geography WHERE GeographyType='City' \
                 AND ContinentName='" + continent + "' AND RegionCountryName LIKE '%" + country + "%' \
                AND StateProvinceName LIKE '%" + state + "%' AND CityName LIKE '%" + city + "%'")
if df_city.count() == 0:
    logger.info("City not found: " + city + ". Creating it...")
    print("City not found: " + city + ". Creating it...")
    df_next = spark.sql("SELECT NVL(MAX(GeographyKey),0)+1 AS NextGeoKey FROM OmniSync_DE_LH_320_Gold_Contoso.dbo.Geography")
    geoKey = nextValue = df_next.first()['NextGeoKey']
    new_city_df_row = [(nextValue, 'City', continent, city, state, country, False, datetime.now(), datetime.now())]
    new_city_df = spark.createDataFrame(new_city_df_row, df.schema)
    df_city = df_city.union(new_city_df)
    df_city.write.mode("append").format("delta").saveAsTable("OmniSync_DE_LH_320_Gold_Contoso.dbo.Geography")
else:
    geoKey = df_city.first()['GeographyKey']
    logger.info("City found: " + city)
    print("City found: " + city)
    df_city.show()  

notebookutils.notebook.exit(geoKey)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
