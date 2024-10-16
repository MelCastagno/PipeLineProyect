# Weather Data Pipeline

git remote add origin https://github.com/MelCastagno/PipeLineProyect.git
git branch -M main
git push -u origin main

## Features

- Fetch historical weather data using the OpenWeather API.
- Transform the data into a clean format.
- Load the transformed data into a MySQL database.

## Project Structure

```plaintext
.
├── extract.py               # Fetch weather data from OpenWeather API
├── transform.py             # Clean and transform the fetched data
├── load.py                  # Load the transformed data into a MySQL database
├── README.md                # Project documentation
├── extracted_weather_data.json  # Example of raw JSON data from the API
├── transformed_weather_data.csv # Example of transformed CSV data#   P i p e L i n e P r o y e c t  
 