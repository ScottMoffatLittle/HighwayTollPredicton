import os
import sys
import time
import csv
import json
import collections
import calendar
import holidays
import pickle
from datetime import datetime

import xml.etree.ElementTree as ET

import gpudb
import category_encoders as ce
import pandas as pd
import requests
import pytz


TBL_NAME_toll_stream = "tolling_and_weather"

TBL_SCHEMA_toll_stream = [
    ["startzoneid", "int", "int16"],
    ["endzoneid", "int", "int16"],
    ["direction", "string", "char8"],

    ["toll_prev_1", "float"],
    ["toll_prev_2", "float"],

    ["humidity", "float"],
    ["temperature", "float"],
    ["windspeed", "float"],
    ["weather", "string"],

    ["timestamp", "string", "datetime"],
    ]

db_conn_str="http://172.30.50.78:9191"
db_user = "no_cred"
db_pass = "no_cred"    

try:
    if db_user == 'no_cred' or db_pass == 'no_cred':
        db=gpudb.GPUdb(encoding='BINARY',
                       host=db_conn_str)
    else:
        db=gpudb.GPUdb(encoding='BINARY',
                       host=db_conn_str,
                       username=db_user,
                       password=db_pass)

    if db.has_table(table_name=TBL_NAME_toll_stream)['table_exists']:
        print('Table exists {0:s}'.format(TBL_NAME_toll_stream))

    else:
        status = gpudb.GPUdbTable(_type=TBL_SCHEMA_toll_stream,
                                  name=TBL_NAME_toll_stream,
                                  db=db,
                                  options={'collection_name': 'traffic'})
        print(status)
        print('\tTable created {0:s}'.format('Table created'))          

    sink_table = gpudb.GPUdbTable( name = TBL_NAME_toll_stream, db = db)

except gpudb.gpudb.GPUdbException as e:
    print(e)

#Retrieve one hot encodings
#encoder = pickle.load(open('toll_encoder.sav', 'rb'))

API_KEY_TOLLING = "rroAyo8V7jUoB4R7og4o9zBkagSL2JBQmbrrdKt4j1qIFzKBUBFMaZhHFm1FeI0b"
API_KEY_WEATHER = "62c667044ed34c21941755b53b286186"

ENDPOINT_TOLLS = f"https://smarterroads.org/dataset/download/29?token={API_KEY_TOLLING}&file=hub_data/TollingTripPricing-I66/TollingTripPricing_current.xml"
ENDPOINT_WEATHER = f"https://api.weatherbit.io/v2.0/current?city=Arlington,VA&key={API_KEY_WEATHER}"

'''
core_columns = ['EndZoneID', 'StartZoneID', 'Direction', 'Toll_Prev_1', 'Toll_Prev_2',
       'Temperature', 'Humidity', 'Wind Speed', 'Weather', 'timestamp']

all_columns = ['EndZoneID', 'StartZoneID', 'Direction_1', 'Direction_2',
       'Temperature', 'Humidity', 'Wind Speed', 'Weather_1', 'Weather_2',
       'Weather_3', 'Weather_4', 'hour', 'minute', 'weekday_1', 'weekday_2',
       'weekday_3', 'weekday_4', 'weekday_5', 'weekday_6', 'weekday_7',
       'month_1', 'month_2', 'month_3', 'month_4', 'month_5', 'month_6',
       'month_7', 'month_8', 'month_9', 'month_10', 'month_11', 'month_12',
       'year', 'Holiday', 'Morning Tolling', 'Evening Tolling', 'Toll_Prev_1',
       'Toll_Prev_2']
'''

def persist_stream(indf):


    insertables = []
    for index, row in indf.iterrows():
        insertable = collections.OrderedDict()
        insertable["startzoneid"] = row["StartZoneID"]
        insertable["endzoneid"] = row["EndZoneID"]
        insertable["direction"] = row["Direction"]
        insertable["toll_prev_1"] = row["Toll_Prev_1"]
        insertable["toll_prev_2"] = row["Toll_Prev_2"]
        insertable["humidity"] = row["Humidity"]
        insertable["temperature"] = row["Temperature"]
        insertable["windspeed"] = row["Wind Speed"]
        insertable["weather"] = row["Weather"]
        insertables.append(insertable)

    try:
        insert_status=sink_table.insert_records(insertables)
        print(insert_status)
        print('\tSample records inserted ')

    except gpudb.gpudb.GPUdbException as e:
        
        print(e)    

def get_cur_weather():
    
    weather_json = requests.post(url = ENDPOINT_WEATHER)
    weather_dict = weather_json.json()
    weather_data = weather_dict['data'][0]

    #need wind speed, temp, humidity, and how heavy the weather is
    wind_speed = weather_data['wind_spd']
    temp = weather_data['temp']
    humidity = weather_data['rh']
    precipitation = float(weather_data['precip'])
    desc = 'none'
    if (float(weather_data['snow']) > 0) or (precipitation > 7.5):
        desc = 'heavy'
    elif precipitation > 5:
        desc = 'med'
    elif precipitation > 2:
        desc = 'light'
    return temp, wind_speed, desc, humidity, weather_dict

def get_new_tolling_data():
    # Obtain current toll xml file from the smarterroads website
    r = requests.get(ENDPOINT_TOLLS, allow_redirects=True)
    rows = []
    root = ET.fromstring(r.content)#.getroot()
    for elem in root:
        tag = {}
        tag["EndZoneID"] = elem.attrib['EndZoneID']
        tag["StartZoneID"] = elem.attrib['StartZoneID']
        tag["StartDateTime"] = elem.attrib['IntervalDateTime']
        tag["EndDateTime"] = elem.attrib['IntervalEndDateTime']
        tag["TollRate"] = elem.attrib['ZoneTollRate']
        tag["Direction"] = elem.attrib['CorridorID']
        rows.append(tag)
    return rows

def prepare_new_data(in_tolling, pds_temp, pds_wind, pds_desc, pds_humd):

    dfNew = pd.DataFrame(in_tolling)

    #Change non-categorical rows from 'object' type to int/float
    dfNew['Direction'] = dfNew['Direction'].astype(int)
    dfNew['StartZoneID'] = dfNew['StartZoneID'].astype(int)
    dfNew['EndZoneID'] = dfNew['EndZoneID'].astype(int)
    dfNew['TollRate'] = dfNew['TollRate'].astype(float)

    #Split dataframe into routes
    routes = []
    zones = [[3100, 3110, 3120, 3130], [3200, 3210, 3220, 3230]]
    for direction in zones:
        i = 0
        for zone in direction:
            start_zone = dfNew[dfNew.StartZoneID == zone]
            j = i
            while j < 4:
                new_route = start_zone[start_zone.EndZoneID == direction[j]]
                routes.append(new_route)
                j +=1
            i += 1

    #Fill in previous tolls 
    for route in routes:
        route['Toll_Prev_2'] = route['TollRate'].shift((-1), fill_value = 0)

    #Concat routes into one dataframe and rename the TollRate colunt to the first
    #previous toll, as it is technically from the past
    dfNew = pd.concat(routes)
    dfNew = dfNew.rename(columns={'TollRate':'Toll_Prev_1'})

    #take only the columns with the most current toll information
    recent_time = dfNew['StartDateTime'].iloc[0]
    dfNew = dfNew[dfNew['StartDateTime'] == recent_time]

    #Get current time to make predictions on
    tz = pytz.timezone("GMT")
    time = datetime.now().astimezone(tz)

    #Add ML useable columns to represent the date and time
    dfNew["timestamp"] = time
    #dfNew["hour"] = time.hour
    #dfNew["minute"] = time.minute
    #fNew["weekday"] = calendar.day_name[time.weekday()]
    #dfNew["month"] = calendar.month_name[time.month]
    #dfNew["year"] = time.year - 2017

    #Add a feature that is a 1 if the date is a federal holiday, 0 otherwise
    #us_holidays = holidays.UnitedStates()
    #dfNew['Holiday'] = int(str(time) in us_holidays)

    #Add two columns to specify whether morning tolling or evening tolling is active
    #dfNew['Morning Tolling'] = cur_morning_tolling(time)
    #dfNew['Evening Tolling'] = cur_evening_tolling(time)

    #Drop columns we no longer need
    dfNew = dfNew.drop(columns=['StartDateTime', 'EndDateTime'])
    
    dfNew['Temperature'] = pds_temp
    dfNew['Wind Speed'] = pds_wind
    dfNew['Weather'] = pds_desc
    dfNew['Humidity'] = pds_humd

    #One hot encode categorical columns using the encoder we loaded
    dfNew['Direction'] = dfNew['Direction'].map({1100:'E', 1200:'W'})
    #dfNew = encoder.transform(dfNew)

    #Rearrange dataframe to have correct column order
    #dfNew = dfNew[all_columns]
    return dfNew

def main():
    print("Starting toll and weather data monitor")
    
    last_tolling = get_new_tolling_data()
    pds_temp, pds_wind, pds_desc, pds_humd, last_weather = get_cur_weather()
    updated_world_view = prepare_new_data(last_tolling, pds_temp, pds_wind, pds_desc, pds_humd)
    print(updated_world_view)

    persist_stream(updated_world_view)

    while True:
        time.sleep(60)

        # Check if our world-view needs updating
        new_tolling = get_new_tolling_data()
        pds_temp, pds_wind, pds_desc, pds_humd, new_raw_weather = get_cur_weather()

        if last_tolling[0]["EndDateTime"]==new_tolling[0]["EndDateTime"] and \
            last_weather == new_raw_weather:
            print("Nothing has changed, no need to update data. Will check again in 60sec.")
        else:

            if last_weather != new_raw_weather:
                print("New weather data to consider")
            if last_tolling[0]["EndDateTime"]!=new_tolling[0]["EndDateTime"]:
                print("New tolling data to consider")
            
            updated_world_view = prepare_new_data(new_tolling, pds_temp, pds_wind, pds_desc, pds_humd)
            persist_stream(updated_world_view)

            last_weather = new_raw_weather
            last_tolling = new_tolling

if __name__ == "__main__":
    main()