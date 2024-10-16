# load.py

import pandas as pd
from sqlalchemy import create_engine
import logging
import sys

def load_data_to_mysql(input_file, db_name, user, password, host='localhost', port=3306, table_name='weather'):
    """
    Carga datos transformados en una base de datos MySQL.
    """
    try:
        # Configurar logging
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            handlers=[
                                logging.FileHandler("load.log"),
                                logging.StreamHandler(sys.stdout)
                            ])
        
        logging.info("Iniciando la carga de datos a MySQL.")

        # Crear la conexión a la base de datos
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db_name}')
        logging.info("Conexión a MySQL establecida exitosamente.")

        # Leer el archivo CSV transformado
        df = pd.read_csv(input_file)
        logging.info(f"Archivo '{input_file}' leído exitosamente. Número de registros: {len(df)}.")

        # Verificar que el DataFrame no está vacío
        if df.empty:
            logging.warning("El DataFrame está vacío. No se cargará ninguna tabla.")
            return

        # Cargar los datos en la tabla MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Datos cargados exitosamente en la tabla '{table_name}' de la base de datos '{db_name}'.")

    except FileNotFoundError:
        logging.error(f"El archivo de entrada '{input_file}' no se encontró.")
    except pd.errors.EmptyDataError:
        logging.error(f"El archivo de entrada '{input_file}' está vacío.")
    except Exception as e:
        logging.error(f"Ocurrió un error inesperado: {e}")

if __name__ == "__main__":
    INPUT_FILE = 'transformed_weather_data.csv'
    DB_NAME = 'weather_data'
    USER = 'weather_user'
    PASSWORD = 'password123'  # Reemplaza con tu contraseña real
    HOST = 'localhost'
    PORT = 3306
    TABLE_NAME = 'weather'
    
    load_data_to_mysql(INPUT_FILE, DB_NAME, USER, PASSWORD, HOST, PORT, TABLE_NAME)
