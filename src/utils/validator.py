def validate_fuel_data(df):
    df = df[df['price'] > 0]
    df = df.dropna(subset=['station_name','fuel_type','date'])
    return df