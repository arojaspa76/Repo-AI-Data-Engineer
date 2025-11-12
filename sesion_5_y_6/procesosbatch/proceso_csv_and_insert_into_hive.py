import pandas as pd
import time
import schedule
from pyhive import hive
import os
import argparse
import math

'''
Ingeniero: Andrés Felipe Rojas Parra
Maestria en Big Data y Data Science

Descripcion del script:
Script para automatizar el envio de informacion al sistema de big data (HiveQL y Hadoop)

Keypoint:
table_name: Nombre de la tabla donde se almacenará la informacion de eventos
csvalertpath: Ruta donde esta el archivo generado por la NVIDIA Jetson Nano de los eventos capturados por el modelo.
hiveqlhost: Direccion IP o url de conexion al servidor HiveQL
hiveipport: Puerto de conexion al servidor de HiveQL

'''
# --- utilidades ---
def esc(s: str) -> str:
    """Escapa comillas simples y normaliza backslashes."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = str(s).replace("\\", "/")
    return s.replace("'", "''")

def ts_literal(x) -> str:
    """Devuelve literal SQL TIMESTAMP o NULL."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "NULL"
    # asegura formato compatible con Hive TIMESTAMP
    t = pd.to_datetime(x)
    return f"TIMESTAMP '{t.strftime('%Y-%m-%d %H:%M:%S')}'"

def str_literal(x) -> str:
    """Devuelve 'texto' o NULL."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "NULL"
    return f"'{esc(x)}'"


def validate_csv_sent_file(file_path):
    '''
    Funcion por validar que exista archivo

    Prametros:
    file_path: Ruta del archivo csv a validar.

    Retorna un dataframe
    '''
    bReturn = False

    if os.path.exists(file_path):
        bReturn = True
    else:
        bReturn = False

    return bReturn

def delete_csv_sent_file(file_path):

    bReturn = False
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"El archivo {file_path} ha sido borrado.")
        bReturn = True
    else:
        print(f"El archivo {file_path} no existe.")    
        bReturn = False
    
    return bReturn

def read_csv_file(file_path):
    '''
    Funcion por leer archivos

    Prametros:
    file_path: Ruta del archivo csv con las alertas.

    Retorna un dataframe
    '''

    return pd.read_csv(file_path)


def remove_duplicates(dataframe):
    '''
    Funcion para eliminar los registros duplicados

    Parametros:
    dataframe: Dataframe del archivo leido

    Retorna un dataframe sin duplicados
    '''
    cols = ["dispositivo","tipoinfraccion","imagen","ubicacion","zonainteres","fechahora"]
    # clean_data = dataframe.drop_duplicates(subset=['fechahora'])
    clean_data = dataframe.drop_duplicates(subset=cols, keep="first").reset_index(drop=True)
    return clean_data

def insert_into_hive(clean_data, debug):
    '''
    Funcion para enviar la informacion a la base de datos

    Parametro:
    clean_data: Dataframe sin duplicados
    '''

    #hiveqlhost = '10.0.0.103'
    hiveqlhost = 'localhost'
    hiveipport= 10000
    hiveuser='arojaspa'
    hiveauth='NONE'
    hivedatabase='cursobsg'
    table_name = 'eventos'

    conn = hive.Connection(host=hiveqlhost, port=hiveipport, username=hiveuser, database=hivedatabase, auth=hiveauth)
    cursor = conn.cursor()

    # --- parámetros ---
    batch_size = 40
    cols = "(dispositivo, tipoinfraccion, imagen, ubicacion, zonainteres, fechahora)"

    # --- normaliza el DataFrame por si 'fechahora' viene como string mixto ---
    # (opcional, pero ayuda a evitar sorpresas)
    # clean_data['fechahora'] = pd.to_datetime(clean_data['fechahora'], errors='coerce')

    print("Enviando Datos..")
    
    #for index, row in clean_data.iterrows():
    #    query = f"""INSERT INTO {table_name} (dispositivo,tipoinfraccion,imagen,ubicacion,zonainteres,fechahora) VALUES ('{row['dispositivo']}','{row['tipoinfraccion']}','{row['imagen'].replace('\\', '/')}','{row['ubicacion']}','{row['zonainteres']}','{row['fechahora']}')"""
    #    print(query)  # Para validar el registro envido
    #    cursor.execute(query)

    n = len(clean_data)
    total_batches = math.ceil(n / batch_size)

    for i, start in enumerate(range(0, n, batch_size), start=1):
        chunk = clean_data.iloc[start:start+batch_size]
        print(f"Enviando paquete {i} de {total_batches}... ({len(chunk)} registros)")

        values = []
        for _, row in chunk.iterrows():
            dispositivo   = str_literal(row.get('dispositivo'))
            tipoinfraccion= str_literal(row.get('tipoinfraccion'))
            imagen        = str_literal(row.get('imagen'))
            ubicacion     = str_literal(row.get('ubicacion'))
            zonainteres   = str_literal(row.get('zonainteres'))
            fechahora     = str_literal(row.get('fechahora'))

            tuple_sql = f"({dispositivo},{tipoinfraccion},{imagen},{ubicacion},{zonainteres},{fechahora})"
            values.append(tuple_sql)

        query = f"INSERT INTO {table_name} {cols} VALUES {', '.join(values)}"
        if debug:
            print(query)             # para auditar lo que se envía
        cursor.execute(query)    # PyHive: no necesitas commit

    # Commit los cambios
    conn.commit()

    # cerrando el cursor y la conexion
    cursor.close()
    conn.close()

def process_csv_and_insert_into_hive(debug):
    '''
    Funcion que se utiliza para activar el job. Se ejcuta segun la configuracion que se haya realizado el job

    '''

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csvalertpath = os.path.join(script_dir, "eventosdetectadosnvidia.csv")
        csvsentpath = os.path.join(script_dir, "eventosdetectadosnvidia_inHiveQL.csv")  

        bSentFile = False
        bNvidiaFile = False
        bError = False
        clean_data = ""

        if validate_csv_sent_file(csvsentpath):
            try:
                df_sent = read_csv_file(csvsentpath)
                bSentFile = True
                if debug:
                    print("Se validó el archivo de eventos enviados")
            except Exception as e:
                print(e)
        else:
            if debug:
                print("No se encontró el archivo de elementos enviados")


        if validate_csv_sent_file(csvalertpath):
            try:
                nvidia = read_csv_file(csvalertpath)
                bNvidiaFile = True
                if debug:
                    print("Se validó el archivo de eventos a enviar")
            except Exception as e:
                print(e)
        else:
            if debug:
                print("No se encontró el archivo de elementos a enviar")


        if not bSentFile and not bNvidiaFile:
            print("No hay informacion para ser procesada. Solicitando la cancelacion del Job.")
            bError = True
            raise RuntimeError()
        
        elif not bSentFile and bNvidiaFile:
            # Elimina duplicados de los eventos
            if debug:
                print("Removiendo dupliados de eventos a enviar")
            clean_data = remove_duplicates(nvidia)

        elif bSentFile and bNvidiaFile:
            if debug:
                print("Removiendo dupliados de eventos a enviar")

            # Elimina duplicados de los eventos
            nvidia_without_duplicates = remove_duplicates(nvidia)
            
            # despues de limpiar duplicados, compara con el df enviados para solo enviar los nuevos eventos.
            clean_data = nvidia_without_duplicates[~nvidia_without_duplicates.isin(df_sent).all(axis=1)]

            if debug:
                print(f"Comparando datos de eventos enviados vs los que se van a enviar. Total eventos nuevos {clean_data.shape[0]}...")

    
    except RuntimeError as e:
        print('Error: El job fue cancelado por falta de informacion', e)
        schedule.cancel_job
        
    finally:

        if not bError:
            print("Procesando informacion...")
            rows, columns = clean_data.shape

            if rows > 0:
                insert_into_hive(clean_data, debug)
            else:
                print("No hay datos para insertar en la base de datos...")
            
            # Si el archio existe, lo borra para crear uno nuevo. Si no existe, lo crea.
            delete_csv_sent_file(csvsentpath)

            # copia la informacion enviada a HiveQL en el dataframe df_sent
            if not bSentFile:
                df_sent = pd.DataFrame()

            df_sent = pd.concat([df_sent,clean_data], ignore_index=True)

            # Guarda la informacion en el archivo csv
            df_sent.to_csv("eventosdetectadosnvidia_inHiveQL.csv", index=False)
        
        else:
            print('Inicializando otra instancia...', )
        

def main(debug):
    '''
    Funcion de inicio del script
    '''

    last = None

    # Configuración del job
    #schedule.every(1).hours.do(process_csv_and_insert_into_hive)
    #schedule.every(1).minutes.do(lambda: process_csv_and_insert_into_hive(debug))
    schedule.every(10).seconds.do(lambda: process_csv_and_insert_into_hive(debug))

    while True:

        schedule.run_pending()

        # Muestra la informacion de la siguiente ejecucion
        next_run = schedule.next_run()
        if next_run != last:
            print(f"Next run: {next_run}")
        last = next_run

        time.sleep(1)


if __name__ == '__main__':

    # Initialize the argument parser
    parser = argparse.ArgumentParser(description="Arguments to setup the Camera Object")

    parser.add_argument('--debug', dest='debug', action='store_true', help="To do debug")
    parser.add_argument('--no-debug', dest='debug', action='store_false', help="To do debug")

    # Parse the arguments
    args = parser.parse_args()

    if args.debug:
        print("Debug in __Main__: ", args.debug)

    main(args.debug)
    #process_csv_and_insert_into_hive(args.debug)
