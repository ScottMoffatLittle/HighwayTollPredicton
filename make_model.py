import pandas as pd
import calendar
from datetime import datetime
import holidays
import category_encoders as ce
from sklearn.linear_model import LinearRegression
import pickle

if __name__ == '__main__':
	df = pd.read_csv('csv_files/toll_weather.csv')
	df = df.drop_duplicates()

	#Categorize the weather description as none, light, medium, or heavy
	df['Weather Desc'] = df['Weather Desc'].fillna('none')
	df['Weather'] = df['Weather Desc'].map({'none':'none', 'Thunderstorm':'heavy', 
	 'Light thunderstorm, rain':'med', 'Heavy thunderstorm, rain':'heavy',
	 'Mist':'light', 'In the vicinity thunderstorm':'light',
	 'In the vicinity thunderstorm, light rain':'light', 'Light rain, mist':'light',
	 'Thunderstorm, rain, mist':'heavy', 'Light rain':'light', 'Rain':'med',
	 'Heavy thunderstorm, rain, mist':'heavy', 'Light thunderstorm, rain, mist':'med',
	 'Heavy rain, mist':'heavy', 'Rain, mist':'med', 'Heavy rain':'heavy', 'Light drizzle, mist':'light',
	 'In the vicinity thunderstorm, rain':'med', 'Thunderstorm, rain':'heavy', 'Haze':'none',
	 'In the vicinity thunderstorm, light rain, mist':'light', 'Light drizzle':'light',
	 'In the vicinity thunderstorm, heavy rain, mist':'heavy',
	 'In the vicinity thunderstorm, heavy rain':'heavy',
	 'In the vicinity thunderstorm, rain, mist':'med', 'Ground fog':'light',
	 'Light rain, snow, mist':'heavy', 'Light rain, ice pellets':'heavy',
	 'Light rain, snow, ice pellets, mist':'heavy', 'Light rain, ice pellets, mist':'heavy',
	 'Light rain, snow':'med', 'Light snow':'heavy', 'Light snow, mist':'heavy',
	 'Light ice pellets, rain':'heavy',
	 'Light funnel cloud(s) (tornado or water-spout), rain':'heavy', 'Snow, mist':'heavy',
	 'Light snow, ice pellets, mist':'heavy', 'Snow, ice pellets, mist':'heavy',
	 'Light funnel cloud(s) (tornado or water-spout), mist':'heavy',
	 'Light ice pellets, mist':'heavy', 'Ice pellets, rain':'heavy'})

	#Drop features that do not correlate
	df = df.drop(['Wind Direction', 'Cloud Cover', 'Unnamed: 0', 'Visibility', 'Pressure', 'Weather Desc'], axis=1)
	df['Wind Speed'] = df['Wind Speed'].fillna(3.8)

	#Add ML useable columns to represent the date and time
	df["date"] = df.StartDateTime.apply(lambda x : x.split()[0])
	df["hour"] = df.StartDateTime.apply(lambda x : x.split()[1].split(":")[0])
	df["minute"] = df.StartDateTime.apply(lambda x : x.split()[1].split(":")[1])
	df["weekday"] = df.date.apply(lambda dateString : calendar.day_name[datetime.strptime(dateString,"%Y-%m-%d").weekday()])
	df["month"] = df.date.apply(lambda dateString : calendar.month_name[datetime.strptime(dateString,"%Y-%m-%d").month])
	df["year"] = df.date.apply(lambda dateString : int(dateString[:4]) - 2017)

	#Add a feature that is a 1 if the date is a federal holiday, 0 otherwise
	us_holidays = holidays.UnitedStates()
	df['Holiday'] = df.date.apply(lambda dateString : int(dateString in us_holidays))

	#One hot encode categorical columns
	encoder = ce.one_hot.OneHotEncoder(cols = ['weekday', 'month', 'Direction', 'Weather'])
	df = encoder.fit_transform(df)

	#Drop december 2018, as there was a government shutdown that month and it skews our data
	df = df[(df.month_10 == 0) & (df.year == 1)]

	#determine whether morning tolling is currently active
	def morning_tolling(datetime):
	    hour = int(datetime[11:13])
	    minute = int(datetime[14:16])
	    if (hour == 9 and minute >= 30) or (hour == 13 and minute <= 30) or (hour >= 10 and hour <= 12):
	        return 1
	    return 0

	#determine whether evening tolling is currently active
	def evening_tolling(datetime):
	    hour = int(datetime[11:13])
	    if (hour >= 19 and hour <= 23):
	        return 1
	    return 0

	#Add two columns to specify whether morning tolling or evening tolling is active
	df['Morning Tolling'] = df['StartDateTime'].apply(lambda x: morning_tolling(x))
	df['Evening Tolling'] = df['StartDateTime'].apply(lambda x: evening_tolling(x))

	#Drop columns we no longer need
	df = df.drop(columns=['StartDateTime', 'EndDateTime', 'date'])

	#Add fields to each row of previous tolls
	routes = []
	zones = [[3100, 3110, 3120, 3130], [3200, 3210, 3220, 3230]]
	for direction in zones:
	    i = 0
	    for zone in direction:
	        start_zone = df[df.StartZoneID == zone]
	        j = i
	        while j < 4:
	            new_route = start_zone[start_zone.EndZoneID == direction[j]]
	            routes.append(new_route)
	            j +=1
	        i += 1

	for route in routes:
	    tolls = route.TollRate
	    for i in range(2):
	        route['Toll_Prev_{ID}'.format(ID=str(i + 1))] = route['TollRate'].shift((i + 1), fill_value = 0)

	#concatenate the routes back together to a single dataframe
	concat = pd.concat(routes)

	#Fit the linear regression model to the data
	y = concat['TollRate']
	X = concat.drop(columns=['TollRate'])
	model = LinearRegression()
	model = model.fit(X, y)

	#To presist the model via pickle, choose file names first
	model_file = 'toll_model.sav'
	encoder_file = 'toll_encoder.sav'
	
	#Persist the model as bit dump
	pickle.dump(model, open(model_file, 'wb'))
	pickle.dump(encoder, open(encoder_file, 'wb'))
	print("Complete")
