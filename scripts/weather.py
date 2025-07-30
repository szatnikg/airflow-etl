import pandas as pd
import asyncio
import aiohttp
import json
import os

import sys
sys.path.insert(0, os.path.abspath(r'/opt/airflow/scripts'))
from static_dir import StaticDirectory

class WeatherApiHandler(StaticDirectory):
    """Connecting to and fetching from weather API"""

    def __init__(self, api_key):
        # call super for downstream initialization
        super().__init__()

        self.api_key = api_key
        self.coordinates = self.load_coordinates_heap()
        # Define the column names
        columns = [
            'City', 'main', 'description', 'temp', 'pressure', 'humidity', 'sea_level', 
            'temp_max', 'temp_min', 'grnd_level', 
             'wind_speed'
        ]

        # Create an empty DataFrame with the specified columns
        self.weather_df = pd.DataFrame(columns=columns)

    def load_coordinates_heap(self):
        # city -- longitude / latitude is a static value therefore we can save it 
        # to reduce number of API calls made 
        # if dataset too large: this dict based solution shall be reconsidered
        # create different folder to store any number of these jsons if multiple such tasks exist.
        # as per containerization it shall be mounted...

        coordinates={}
        if os.path.exists('coordinates.json'):
            with open('coordinates.json','r',encoding='utf-8') as c_file:
                
                if c_file.read().strip():  # Check if file is not empty
                    c_file.seek(0)  

                    coordinates= json.load(c_file)
        return coordinates
        
    def register_coordinates_heap(self, city, lon, lat):
        
        self.coordinates[city]={'lon':lon, 'lat':lat}

        return

    def save_coordinates_heap(self):
        with open('coordinates.json','w',encoding='utf-8') as c_file:
            json.dump(self.coordinates, c_file)
        
        return

    def extract_cities_weather(self, cities):
        # extract data from api
        # get latitude and longitude for city:
        # cities can be duplicated so build a heap for future processing (decrease number of calls + time and processing time)
        asyncio.run(self.main_city_call(cities, only_coordinate=True ))
        self.save_coordinates_heap()

        asyncio.run(self.main_city_call(cities, only_coordinate=False)) 
        
        path=f'{self.TEMP}/weather.parquet'
        self.weather_df.to_parquet(path, index=False)

        if not self.weather_df.count().all() or (self.weather_df.count().all() > 0.5 * len(cities)):
            raise ValueError('=== ##ERROR## less than 50% weather data was fetched for cities')
        else:
            print('\n=== WEATHER DATA OBTAINED SUCCESSFULLY')
            
        if os.path.exists(path):
            print('\n=== WEATHER TABLE WRITTEN TO: ', path)
        
        return


    async def city_fetch(self, session, city):
        coordinates_url = f'http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={self.api_key}'
        
        async with session.get(coordinates_url) as response:
            res = await response.text()
            
            if response.status>300:
                print('session.get.weather ERROR -- status code '+res)
                return
            data = json.loads(res.encode('utf-8'))
            
            try: self.register_coordinates_heap(city, data[0]['lon'], data[0]['lat'])
            except:
                self.register_coordinates_heap(city, None, None)
                print('=== ##WARNING## No data available for '+city)
            return data 
    
    async def weather_fetch(self, session, coords,city):
        # we could run this only on a distinc list of cities, but I leave this step out.
        if coords['lat']==None or coords['lon']==None:
            return
        weather_URL = f"http://api.openweathermap.org/data/2.5/weather?lat={coords['lat']}&lon={coords['lon']}&appid={self.api_key}&units=metric"
        
        async with session.get(weather_URL) as response:
            res = await response.text()
            if response.status>300:
                print('session.get.weather ERROR -- status code '+res)
                return
            data = json.loads(res.encode('utf-8'))
            # build df (in case of memory issues  consider writing to disk periodically and flushing self.weather_df)
            
            new_data = {
                'City': city,
                'temp': data['main']['temp'],
                'pressure': data['main']['pressure'],
                'humidity': data['main']['humidity'],
                'sea_level': data['main']['sea_level'],
                'temp_max': data['main']['temp_max'],
                'temp_min': data['main']['temp_min'],
                'grnd_level': data['main']['grnd_level'],
                'main': data['weather'][0]['main'],
                'description': data['weather'][0]['description'],
                'wind_speed': data['wind']['speed']
            }
            if all(value is not None for value in new_data.values()):
                if  isinstance(new_data['City'], str) and isinstance(new_data['temp'], (int, float)) and \
                    isinstance(new_data['pressure'], (int, float)) and isinstance(new_data['humidity'], (int, float)) and \
                    isinstance(new_data['wind_speed'], (int, float)):
       
                    new_df = pd.DataFrame([new_data])

                    if not new_df.empty and not new_df.isna().all().all():
                        # Concatenate the new data with the existing DataFrame
                        self.weather_df = pd.concat([self.weather_df, new_df], ignore_index=True)
            
            return

    async def main_city_call(self, cities, only_coordinate=True):
        async with aiohttp.ClientSession() as session:
            if only_coordinate:
                #exclude cities that are already in heap.
                # this is to reduce future processing needs..
                filtered_cities = [city for city in cities if city not in self.coordinates]
                
                tasks = [self.city_fetch(session, city) for city in filtered_cities]
                
                if len(tasks):
                    return await asyncio.gather(*tasks)
            else:
                tasks = [self.weather_fetch(session, self.coordinates[city], city) for city in cities]
                if len(tasks):
                    return await asyncio.gather(*tasks)

def run():
    
    executor = WeatherApiHandler(os.getenv('API_KEY'))

    df = pd.read_parquet(executor.TEMP+'/Customers.parquet')
    executor.extract_cities_weather(df['City'].dropna().unique())


if __name__=="__main__":
    run()
    