# db/mySQL_connector.py
import mysql.connector
from mysql.connector import Error
import os
from config import settings

def _obtener_connection_db():
    """
    Función auxiliar privada para crear y devolver un objeto de conexión a la BD.

    Esta función centraliza la lógica de conexión, leyendo las credenciales de
    forma segura desde las variables de entorno.
    
    Returns:
        mysql.connector.connection_cext.CMySQLConnection | None: 
        El objeto de conexión si es exitoso, o None si ocurre un error.
    """
    try:
        # Intenta establecer la conexión con los parámetros definidos en el .env
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE'),
            charset=os.getenv('DB_CHARSET')
        )
        # Verifica si la conexión fue realmente exitosa
        if connection.is_connected():
            return connection
    except Error as e:
        # Si algo falla (ej. credenciales incorrectas, BD no disponible), lo imprime en la consola del servidor.
        print(f"Error al conectar a MySQL: {e}")
        return None

def obtener_datos_glosas(fecha_inicio: str = None, fecha_fin: str = None) -> tuple:
    """
    Obtiene los datos principales uniendo las tablas de detalle ('glo_det') y cabecera ('glo_cab_test').
    Permite filtrar los resultados por un rango de fechas basado en la 'fechanotificacion'.

    Args:
        fecha_inicio (str, optional): La fecha de inicio del filtro en formato 'YYYY-MM-DD'.
        fecha_fin (str, optional): La fecha de fin del filtro en formato 'YYYY-MM-DD'.

    Returns:
        tuple: Una tupla (registros, mensaje_error). 'registros' es una lista de diccionarios
               con los datos, y 'mensaje_error' es None si todo fue bien.
    """
    connection = _obtener_connection_db()
    if not connection:
        return None, "Fallo al obtener la conexión a la base de datos."

    cursor = None
    try:
        # La consulta base une las dos tablas. Se usan alias 'c' y 'd' para mayor claridad.
        # Se seleccionan explícitamente las columnas para evitar ambigüedades y mejorar el rendimiento.
        query = """
            SELECT
                c.fechanotificacion, c.tipo, c.nom_entidad, c.fc_serie, c.fc_docn, c.saldocartera,
                d.fecha_gl, d.gl_docn, d.estatus1, d.vr_glosa,
                d.freg, d.gr_docn, d.fecha_rep
            FROM
                glo_det d
            INNER JOIN
                glo_cab_test c ON d.gl_docn = c.gl_docn
        """
        params = [] # Lista para almacenar los valores de los filtros de forma segura.

        # Si el usuario proporciona un rango de fechas, se añade dinámicamente el filtro a la consulta.
        if fecha_inicio and fecha_fin:
            # La cláusula WHERE utiliza placeholders (%s). Esto es CRUCIAL para prevenir inyección SQL.
            query += f" WHERE c.`{settings.COL_FECHA_NOTIFICACION}` BETWEEN %s AND %s"
            # Los valores de las fechas se añaden a la lista de parámetros.
            params.extend([f"{fecha_inicio} 00:00:00", f"{fecha_fin} 23:59:59"])

        # Se crea un cursor que devuelve las filas como diccionarios.
        cursor = connection.cursor(dictionary=True)
        print("Ejecutando consulta SQL con JOIN y de forma segura...")
        
        # El conector de MySQL reemplaza los %s con los valores de 'params' de forma segura.
        cursor.execute(query, tuple(params))
        
        registros = cursor.fetchall() # Obtiene todas las filas del resultado.
        return registros, None
    
    except Error as e:
        print(f"Error al ejecutar la consulta JOIN: {e}")
        return None, str(e)
    
    finally:
        # Este bloque se ejecuta siempre, asegurando que la conexión se cierre.
        if cursor: 
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def obtener_rango_fechas() -> tuple:
    """
    Obtiene la fecha mínima y máxima de notificación de la tabla de cabeceras.
    Esta información es útil para inicializar los filtros de fecha en el frontend.

    Returns:
        tuple: Una tupla (rango, mensaje_error). 'rango' es un diccionario {'fecha_min', 'fecha_max'}.
    """
    connection = _obtener_connection_db()
    if not connection: 
        return None, "Fallo al obtener la conexión."

    cursor = None
    try:
        # Consulta simple y rápida para obtener los valores extremos del rango de fechas.
        query = f"SELECT MIN(`{settings.COL_FECHA_NOTIFICACION}`) AS fecha_min, MAX(`{settings.COL_FECHA_NOTIFICACION}`) AS fecha_max FROM glo_cab_test;"
        
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchone() # Solo esperamos una fila.
        
        # Maneja el caso en que la tabla esté vacía y no haya fechas.
        if result and result.get('fecha_min') is not None:
            return result, None
        else:
            return {"fecha_min": None, "fecha_max": None}, "No se encontraron fechas en la tabla."
            
    except Error as e:
        print(f"Error al obtener el rango de fechas: {e}")
        return None, str(e)
        
    finally:
        # Asegura siempre el cierre de recursos.
        if cursor: 
            cursor.close()
        if connection and connection.is_connected():
            connection.close()