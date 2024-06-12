import os
import pandas as pd
import requests
import zipfile
import sqlite3
import io
from sqlalchemy import create_engine

def ensure_directory(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except Exception as e:
            print(f"Error creating directory {path}: {e}")
            exit(1)

def download_and_transform_dataset1(url, output_folder, db_path):
    ensure_directory(output_folder)
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        zip_file.extractall(output_folder)
    csv_filename = os.path.join(output_folder, zip_file.namelist()[1])
    df = pd.read_csv(csv_filename, on_bad_lines='skip')
    df.drop(columns=['Country Code'], inplace=True)
    df = df.loc[~(df==0).all(axis=1)]
    df.dropna(inplace=True)
    ensure_directory(os.path.dirname(db_path))
    conn = sqlite3.connect(db_path)
    df.to_sql('world_forest_data', conn, if_exists='replace', index=False)
    conn.close()
    return df

def download_and_transform_dataset2(url, output_folder, db_path):
    ensure_directory(output_folder)
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        zip_file.extractall(output_folder)
    csv_filename = os.path.join(output_folder, zip_file.namelist()[0])
    df = pd.read_csv(csv_filename, on_bad_lines='skip')
    df.drop(columns=['1970','1971','1972','1973','1974','1975','1976','1977','1978','1979','1980','1981','1982','1983','1984','1985','1986','1987','1988','1989'], inplace=True)
    df = df.loc[~(df==0).all(axis=1)]
    df.dropna(inplace=True)
    ensure_directory(os.path.dirname(db_path))
    conn = sqlite3.connect(db_path)
    df.to_sql('global_temperature_data', conn, if_exists='replace', index=False)
    conn.close()
    return df

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Moves up two levels from current script's directory
    data_dir = os.path.join(base_dir, 'data')
    project_dir = os.path.join(base_dir, 'project')
    forest_data_path=os.path.join(project_dir, 'forest_data')
    temp_data_path=os.path.join(project_dir, 'temperature_data')
    forest_db=os.path.join(data_dir, 'world_forest_data.sqlite')
    temp_db=os.path.join(data_dir, 'global_temperature_data.sqlite')

    forest_data_url = 'https://www.kaggle.com/api/v1/datasets/download/webdevbadger/world-forest-area'

    temperature_data_url = 'https://www.kaggle.com/api/v1/datasets/download/mdazizulkabirlovlu/all-countries-temperature-statistics-1970-2021?datasetVersionNumber=1'


    forest_df=download_and_transform_dataset1(forest_data_url, forest_data_path, forest_db)
    temp_df=download_and_transform_dataset2(temperature_data_url, temp_data_path, temp_db)

    merged_df = pd.merge(forest_df, temp_df,on="Country Name", how='inner')

    #SQLite database path
    
    db_name = 'temp_and_forest_data'
    db_path = os.path.join(data_dir, f"{db_name}.sqlite")
    engine = create_engine(f'sqlite:///{db_path}')
    
    #Save the processed data to the database
    conn = sqlite3.connect(db_path)
    merged_df.to_sql('temp_forest_merged_data', conn, if_exists='replace', index=False)
    '''merged_df.to_sql('temp_forest_merged_data', engine, index=False, if_exists='replace')'''

if __name__ == "__main__":
    main()

     