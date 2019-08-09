import os
import sys
import csv
import pickle
import random
import math
import calendar
import json
from datetime import datetime
import holidays

import gpudb
import requests
import pytz
import xml.etree.ElementTree as ET
import pandas as pd
import category_encoders as ce
from sklearn.linear_model import LinearRegression

LENGTH = 20

db_conn_str="http://172.30.18.126:9191"
db_user = "no_cred"
db_pass = "no_cred"    
TBL_NAME_toll_stream = "tolling_and_weather"

db_to_df_name_map = {"startzoneid": "StartZoneID",
                     "endzoneid": "EndZoneID",
                     "direction": "Direction",
                     "toll_prev_1": "Toll_Prev_1",
                     "toll_prev_2": "Toll_Prev_2",
                     "temperature": "Temperature",
                     "humidity": "Humidity", 
                     "windspeed": "Wind Speed", 
                     "weather": "Weather"}

#Retrieve model and one hot encodings
model = pickle.load(open('toll_model.sav', 'rb'))

#Retrieve one hot encodings
encoder = pickle.load(open('toll_encoder.sav', 'rb'))

try:
    if db_user == 'no_cred' or db_pass == 'no_cred':
        db=gpudb.GPUdb(encoding='BINARY',
                       host=db_conn_str)
    else:
        db=gpudb.GPUdb(encoding='BINARY',
                       host=db_conn_str,
                       username=db_user,
                       password=db_pass)

    toll_stream_tbl = gpudb.GPUdbTable( name = TBL_NAME_toll_stream, db = db)

except gpudb.gpudb.GPUdbException as e:
    print(e)


def get_weather_dict():
    url = 'https://api.weatherbit.io/v2.0/forecast/hourly?city=Arlington,VA&key=62c667044ed34c21941755b53b286186'
    weather_json = requests.post(url = url)
    weather_dict = json.loads(weather_json.text)
    return weather_dict

def get_future_weather(dfNew, weather_dict, hour):
    #Get just the data from the necessary hour
    weather_data = weather_dict['data'][hour]

    #need wind speed, temp, humidity, and how heavy the weather is
    wind_speed = weather_data['wind_spd']
    temp = weather_data['temp']
    humidity = weather_data['rh']
    precipitation = float(weather_data['precip'])
    desc = 1
    if (float(weather_data['snow']) > 0) or (precipitation > 7.5):
        desc = 4
    elif precipitation > 5:
        desc = 3
    elif precipitation > 2:
        desc = 2

    dfNew['Temperature'] = temp
    dfNew['Wind Speed'] = wind_speed
    dfNew['Humidity'] = humidity
    
    cols = ['Weather_1', 'Weather_2', 'Weather_3', 'Weather_4']
    for col in cols:
        dfNew[col] = 0

    dfNew['Weather_{ID}'.format(ID=str(desc))] = 1
    return dfNew

def cur_morning_tolling(hour, minute):
    if (hour == 9 and minute >= 30) or (hour == 13 and minute <= 30) or (hour >= 10 and hour <= 12):
        return 1
    return 0 

def cur_evening_tolling(hour, minute):
    if (hour >= 19 and hour <= 23):
        return 1
    return 0

def floor_0_predictions(predictions):
    for i in range(len(predictions)):
        if predictions[i] < 0:
            predictions[i] = 0
    return predictions

def feature_xform(inDictRaw):
    def cur_morning_tolling(time):
        hour = time.hour
        minute = time.minute
        if (hour == 9 and minute >= 30) or (hour == 13 and minute <= 30) or (hour >= 10 and hour <= 12):
            return 1
        return 0 

    def cur_evening_tolling(time):
        hour = time.hour
        if (hour >= 19 and hour <= 23):
            return 1
        return 0

    #  df = pd.DataFrame(columns=db_to_df_name_map.keys())
    df = pd.DataFrame(columns=db_to_df_name_map)
    print(df)
    for a in inDictRaw:
        df = df.append(a, ignore_index=True)
    df = df.rename(columns=db_to_df_name_map)

    tz = pytz.timezone("GMT")
    time = datetime.now().astimezone(tz)

    #Add ML useable columns to represent the date and time
    #df["timestamp"] = time
    df["hour"] = time.hour
    df["minute"] = time.minute
    df["weekday"] = calendar.day_name[time.weekday()]
    df["month"] = calendar.month_name[time.month]
    df["year"] = time.year - 2017

    #Add a feature that is a 1 if the date is a federal holiday, 0 otherwise
    us_holidays = holidays.UnitedStates()
    df['Holiday'] = int(str(time) in us_holidays)

    #Add two columns to specify whether morning tolling or evening tolling is active
    df['Morning Tolling'] = cur_morning_tolling(time)
    df['Evening Tolling'] = cur_evening_tolling(time)

    df = encoder.transform(df)

    all_columns = ['EndZoneID', 'StartZoneID', 'Direction_1', 'Direction_2',
           'Temperature', 'Humidity', 'Wind Speed', 'Weather_1', 'Weather_2',
           'Weather_3', 'Weather_4', 'hour', 'minute', 'weekday_1', 'weekday_2',
           'weekday_3', 'weekday_4', 'weekday_5', 'weekday_6', 'weekday_7',
           'month_1', 'month_2', 'month_3', 'month_4', 'month_5', 'month_6',
           'month_7', 'month_8', 'month_9', 'month_10', 'month_11', 'month_12',
           'year', 'Holiday', 'Morning Tolling', 'Evening Tolling', 'Toll_Prev_1',
           'Toll_Prev_2']


    dfNew = df[all_columns]

    return dfNew

#This is the method that makes the prediction for blackbox.
#Input a json of 'hours', which is how far into the future you want to predict,
#and 'route', wich is a number (0 - 19) correspoding to the entrance / exit combination.
#Route correspondances can be found in the Route_ID.csv file
#The function returns a list of predicted tolls for a certain route, starting at the 
#current time and then predicting in increments of 6 minutes
def get_forecast(cur_df, hours):
    #Predict the inputted number of hours into the future
    predictions = []
    pred = []

    hour = cur_df['hour'].iloc[0]
    minute = cur_df['minute'].iloc[0]

    new_tolling_active = True

    if (cur_evening_tolling(hour, minute) == 1) or (cur_morning_tolling(hour, minute) == 1):
        new_tolling_active = False
        #Make prediction on current dataframe:
        pred = floor_0_predictions(model.predict(cur_df))
    else:
        pred = [0] * LENGTH
    predictions.append(pred)
    cur_df['Toll_Prev_2'] = cur_df['Toll_Prev_1']
    cur_df['Toll_Prev_1'] = pred

    weather_dict = get_weather_dict()
    hour_counter = 0
    time_inc = 6
    for i in range(hours*int(60 / time_inc) - 1):
        #Increase the time by 'time_inc' minutes
        if minute + time_inc > 60:
            minute = minute + time_inc - 60
            if hour + 1 > 23:
                hour = 0
            else:
                hour +=1
            cur_df = get_future_weather(cur_df, weather_dict, hour_counter)
            hour_counter += 1
        else:
            minute += time_inc
        cur_df['minute'] = minute
        cur_df['hour'] = hour

        avg_start_morning_tolls = [0.46, 0.98, 1.77, 2.70, 0.50, 1.42, 2.51, 0.76, 1.59, 0.82] + [0.0]*10
        avg_start_evening_tolls = [0.0]*10 + [2.77, 4.91, 5.55, 6.00, 2.21, 2.85, 3.31, 0.69, 1.16, 0.47]
        #Move the previous tolls up one slot, or set them to zero if tolling is not active
        #If it is the start of a tolling time, set the tolls equal to the average start value for that route
        if cur_morning_tolling(hour, minute) == 1:
            if new_tolling_active:
                pred = avg_start_morning_tolls
                new_tolling_active = False
            else:
                #pred = [0] * LENGTH/2 + floor_0_predictions(model.predict(cur_df))[10:19]
                pred = floor_0_predictions(model.predict(cur_df))
            cur_df['Morning Tolling'] = 1

        elif cur_evening_tolling(hour, minute) == 1:
            if new_tolling_active:
                pred = avg_start_evening_tolls
                new_tolling_active = False
            else:
                #pred = floor_0_predictions(model.predict(cur_df))[0:10] + [0] * LENGTH/2
                pred = floor_0_predictions(model.predict(cur_df))
            cur_df['Evening Tolling'] = 1

        else:
            cur_df['Morning Tolling'] = 0
            cur_df['Evening Tolling'] = 0
            pred = [0] * LENGTH
            new_tolling_active = True

        predictions.append(pred)
        cur_df['Toll_Prev_2'] = pred
        cur_df['Toll_Prev_1'] = pred

    return predictions

def predict(inMap):
    hours = inMap['hours']
    route = inMap['route']
    toll_dataset_inst = toll_stream_tbl.get_records(encoding='json')
    df = feature_xform(toll_dataset_inst)
    
    pred = get_forecast(df, hours)
    route_pred = []
    for time in pred:
        route_pred.append(time[route])

    return {'forecast': str(route_pred)}

#Test function for prediction
def main():
    print(predict({'hours': 1, 'route':3}))
    print()
    print(predict({'hours': 3, 'route':14}))
    
if __name__ == "__main__":
    main()
