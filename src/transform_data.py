import pandas as pd
from pathlib import Path
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

path_name = Path(__file__).parent.parent / 'data' / 'weather_data.json'
columns_name_to_drop = ['weather', 'weather_icon']
columns_name_to_rename = {
    "base": "base",
    "visibility": "visibility",
    "dt": "datetime",
    "timezone": "timezone",
    "id": "city_id",
    "name": "city_name",
    "cod": "code",
    "coord.lon": "longitude",
    "coord.lat": "latitude",
    "main.temp": "temperature",
    "main.feels_like": "feels_like",
    "main.temp_min": "temp_min",
    "main.temp_max": "temp_max",
    "main.humidity": "humidity",
    "main.pressure": "pressure",
    "main.sea_level": "sea_level",
    "main.grnd_level": "grnd_level",
    "wind.speed": "wind_speed",
    "wind.deg": "wind_deg",
    "wind.gust": "wind_gust",
    "clouds.all": "clouds",
    "sys.type": "sys_type",
    "sys.id": "sys_id",
    "sys.country": "country",
    "sys.sunrise": "sunrise",
    "sys.sunset": "sunset",
}
columns_to_normalize_datetime = ['datetime', 'sunrise', 'sunset']


def create_dataframe(path_name: str) -> pd.DataFrame:
    logging.info("-> Creating dataframe from json file...")
    path = Path(path_name)

    if not path.exists():
        raise FileNotFoundError(f"File not found at {path_name}")

    with open(path) as f:
        data = json.load(f)

    df = pd.json_normalize(data)
    logging.info(f"\n data frame created with {len(df)} rows")
    return df


def normalize_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    df_weather = pd.json_normalize(df['weather'].apply(lambda x: x[0]))
    # passa por cada linha da coluna e pega o primeiro item
    df_weather = df_weather.rename(columns={'id': 'weather_id',
                                            'main': 'weather_main',
                                            'description': 'weather_description',
                                            'icon': 'weather_icon', })

    df = pd.concat([df, df_weather], axis=1)
    logging.info(f"\n data frame normalized with {len(df)} rows")
    return df


def drop_columns(df: pd.DataFrame, columns_names: list[str]) -> pd.DataFrame:
    logging.info(f"\n dropping columns with {len(columns_names)} rows")
    df = df.drop(columns=columns_names)
    logging.info(f"\n data frame dropped with {len(df.columns)} columns removed")
    return df


def rename_columns(df: pd.DataFrame, columns_names: dict[str, str]) -> pd.DataFrame:
    logging.info(f"\n renaming columns with {len(columns_names)} rows")
    df.rename(columns=columns_names, inplace=True)
    logging.info(f"\n data frame renamed with {len(df.columns)} columns renamed")
    return df


def normalize_datetime_columns(df: pd.DataFrame, columns_names: list[str]) -> pd.DataFrame:
    logging.info(f"\n normalizing datetime columns with {len(columns_names)} rows")
    for name in columns_names:
        df[name] = pd.to_datetime(df[name], unit='s', utc=True).dt.tz_convert('America/Sao_Paulo')
    logging.info(f"\n data frame normalized with {len(df.columns)} columns normalized")
    return df


def data_transformations():
    print("-> Data transformations")
    df = create_dataframe(path_name)
    df = normalize_weather_data(df)
    df = drop_columns(df, columns_name_to_drop)
    df = rename_columns(df, columns_name_to_rename)
    df = normalize_datetime_columns(df, columns_to_normalize_datetime)
    logging.info(f"\n data frame normalized with {len(df.columns)} columns normalized")
    return df
