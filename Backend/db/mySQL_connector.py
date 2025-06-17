# db/database_connector.py
import mysql.connector
from mysql.connector import Error
import os

def obtener_connection_db():
    """Crea y devuelve un objeto de conexión a la base de datos."""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE'),
            charset=os.getenv('DB_CHARSET')
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error al conectar a MySQL: {e}")
        return None

def obtener_datos_glosas(columnas_a_obtener):
    """
    Obtiene datos de la tabla 'gema' para las columnas especificadas.
    
    Argumentos:
        columnas_a_obtener (list): Una lista de nombres de columnas que se desean seleccionar.
        
    Retorna:
        tuple: (datos, mensaje_error)
               - datos (list of dict): Los registros obtenidos de la base de datos.
               - mensaje_error (str o None): Un mensaje de error si ocurre alguna falla.
    """
    connection = obtener_connection_db()
    if not connection:
        return None, "Fallo al obtener la conexión a la base de datos."

    cursor = None
    try:
        # Preparamos la consulta SQL de forma segura para evitar inyección SQL,
        # aunque aquí solo usamos nombres de columnas controlados por nosotros.
        columnas_str = ", ".join([f"`{col}`" for col in columnas_a_obtener])
        consulta = f"SELECT {columnas_str} FROM gema;"
        
        # Usamos un cursor en modo diccionario para obtener {columna: valor},
        # lo cual es más manejable que trabajar con tuplas.
        cursor = connection.cursor(dictionary=True)
        cursor.execute(consulta)
        
        # fetchall() devuelve una lista de diccionarios con los resultados
        registros = cursor.fetchall()
        
        return registros, None  # Retorna los datos y ningún error
    
    except Error as e:
        print(f"Error al ejecutar la consulta de datos: {e}")
        return None, str(e)
    
    finally:
        if cursor:
            cursor.close()
        if connection.is_connected():
            connection.close()

def test_db_query():
    """
    Ejecuta una consulta de prueba para contar las filas en la tabla 'gema'.
    Devuelve el conteo de filas o un mensaje de error.
    """
    connection = obtener_connection_db()
    if not connection:
        return None, "Fallo al obtener la conexión a la base de datos."

    cursor = None
    try:
        cursor = connection.cursor()
        query = "SELECT COUNT(id_gema) FROM gema;"
        cursor.execute(query)
        # fetchone() devuelve una tupla, por ejemplo, (123,)
        result = cursor.fetchone()
        row_count = result[0] if result else 0
        return row_count, None  # Retorna el conteo y ningún error
    except Error as e:
        print(f"Error al ejecutar la consulta: {e}")
        return None, str(e)  # Retorna nada y el mensaje de error
    finally:
        # Asegurarse de cerrar el cursor y la conexión
        if cursor:
            cursor.close()
        if connection.is_connected():
            connection.close()
            

