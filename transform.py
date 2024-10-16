# transform.py

import pandas as pd

def transform_weather_data(input_file, output_file):
    """
    Transform raw weather data to calculate daily average temperatures and humidity.
    """
    df = pd.read_csv(input_file)
    
    # Convert 'dt' to datetime
    df['date'] = pd.to_datetime(df['dt'], unit='s')
    
    # Extract date (without time) for grouping
    df['date_only'] = df['date'].dt.date
    
    # Select relevant columns
    transformed_df = df[['date_only', 'main.temp', 'main.temp_min', 'main.temp_max', 'main.humidity']]
    
    # Rename columns for clarity
    transformed_df.rename(columns={
        'main.temp': 'avg_temp',
        'main.temp_min': 'min_temp',
        'main.temp_max': 'max_temp',
        'main.humidity': 'humidity_percentage'
    }, inplace=True)
    
    # Group by date and calculate daily averages
    daily_df = transformed_df.groupby('date_only').agg({
        'avg_temp': 'mean',
        'min_temp': 'min',
        'max_temp': 'max',
        'humidity_percentage': 'mean'
    }).reset_index()
    
    # Rename 'date_only' back to 'date'
    daily_df.rename(columns={'date_only': 'date'}, inplace=True)
    
    # Data Validation
    # Check for missing values
    if daily_df.isnull().values.any():
        raise ValueError("Data contains missing values.")
    
    # Check data types
    assert daily_df['avg_temp'].dtype in [float, int], "avg_temp should be numeric"
    assert daily_df['min_temp'].dtype in [float, int], "min_temp should be numeric"
    assert daily_df['max_temp'].dtype in [float, int], "max_temp should be numeric"
    assert daily_df['humidity_percentage'].dtype in [float, int], "humidity_percentage should be numeric"
    
    # Range Checks
    if not daily_df['avg_temp'].between(-50, 60).all():
        raise ValueError("Average temperature values out of realistic range.")
    if not daily_df['humidity_percentage'].between(0, 100).all():
        raise ValueError("Humidity percentage values out of realistic range.")
    
    # Save the transformed data
    daily_df.to_csv(output_file, index=False)
    print(f"Data transformed and saved to {output_file}")

if __name__ == "__main__":
    INPUT_FILE = 'raw_weather_data.csv'
    OUTPUT_FILE = 'transformed_weather_data.csv'
    
    transform_weather_data(INPUT_FILE, OUTPUT_FILE)
