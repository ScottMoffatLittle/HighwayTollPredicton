import wget

print('Beginning file download with wget module')
base_url = 'https://smarterroads.org/dataset/download/29?token=rroAyo8V7jUoB4R7og4o9zBkagSL2JBQmbrrdKt4j1qIFzKBUBFMaZhHFm1FeI0b&file=hub_data/TollingTripPricing-I66/'
years = ['2018', '2019']
#hours = ['05', '06', '07', '08', '09', '15', '16', '17', '18', '19']
hours = ['00', '01', '02', '03', '10', '11', '12', '13', '14', '20', '21', '22', '23']

start_month = 6
end_month = 12
start_day = 7
for year in years:
    for month in range(start_month, end_month):
        for day in range(start_day, 31):
            for hour in hours:
                for minute in range(60):
                    if minute % 5 == 2 or minute % 5 == 3:
                        if month < 10:
                            str_mon = '0' + str(month)
                        else: str_mon = str(month)
                        
                        if day < 10:
                            str_day = '0' + str(day)
                        else: str_day = str(day)
                        
                        if minute < 10:
                            str_min = '0' + str(minute)
                        else: str_min = str(minute)
                        
                        url = base_url + year + '/' + str_mon + '/' + str_day + '/' + hour + '/' + str_min + '/' + 'TollingTripPricing_' + year + str_mon + str_day + '_' + hour + str_min + '.xml'
                        try:
                            wget.download(url, './data')
                        except:
                            pass
        start_day = 0
    start_month = 0
    end_month = 6

