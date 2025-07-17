# db/mySQL_connector.py
import mysql.connector
from mysql.connector import Error
import os
from config import settings

def _obtener_connection_db():
    """
    Función privada para crear y devolver un objeto de conexión a la base de datos.
    Reutiliza la lógica de conexión para mantener el código limpio.
    """
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

def obtener_datos_glosas(fecha_inicio: str = None, fecha_fin: str = None) -> tuple:
    """
    Obtiene datos uniendo 'glo_det' (detalle) con 'glo_cab_test' (cabecera).
    Filtra por rango de fechas en 'fecha_gl' de forma segura y eficiente.
    """
    connection = _obtener_connection_db()
    if not connection:
        return None, "Fallo al obtener la conexión a la base de datos."

    cursor = None
    try:
        # Consulta SQL con INNER JOIN y alias para las tablas (c=cabecera, d=detalle)
        # Seleccionamos explícitamente las columnas para evitar ambigüedad (ej. gl_docn)
        # y para renombrar columnas con espacios como 'fecha gl' a 'fecha_gl'.
        query = """
            SELECT
                c.fechanotificacion, c.tipo, c.nom_entidad, c.fc_serie, c.fc_docn, c.saldocartera,
                d.fecha_gl,
                d.gl_docn,
                d.estatus1,
                d.vr_glosa,
                d.freg,
                d.gr_docn,
                d.fecha_rep
            FROM
                glo_det d
            INNER JOIN
                glo_cab_test c ON d.gl_docn = c.gl_docn
        """
        params = []

        if fecha_inicio and fecha_fin:
            # Añadir la cláusula WHERE usando placeholders para seguridad (anti SQL Injection)
            query += f" WHERE c.`{settings.COL_FECHA_NOTIFICACION}` BETWEEN %s AND %s"
            # Los parámetros se añaden a una lista que se pasará a cursor.execute()
            params.extend([f"{fecha_inicio} 00:00:00", f"{fecha_fin} 23:59:59"])

        cursor = connection.cursor(dictionary=True)
        print("Ejecutando consulta SQL con JOIN y de forma segura...")
        cursor.execute(query, tuple(params)) # El conector escapa los valores en 'params'
        
        registros = cursor.fetchall()
        return registros, None
    except Error as e:
        print(f"Error al ejecutar la consulta JOIN: {e}")
        return None, str(e)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected(): connection.close()

def obtener_rango_fechas() -> tuple:
    """
    Obtiene el rango de fechas de la columna de objeción, ahora apuntando a 'glo_det'.
    """
    connection = _obtener_connection_db()
    if not connection: return None, "Fallo al obtener la conexión."

    cursor = None
    try:
        # La consulta ahora apunta a la tabla de detalle (glo_det)
        query = f"SELECT MIN(`{settings.COL_FECHA_NOTIFICACION}`) AS fecha_min, MAX(`{settings.COL_FECHA_NOTIFICACION}`) AS fecha_max FROM glo_cab_test;"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result and result.get('fecha_min') is not None:
            return result, None
        else:
            return {"fecha_min": None, "fecha_max": None}, "No se encontraron fechas en la tabla."
            
    except Error as e:
        print(f"Error al obtener el rango de fechas: {e}")
        return None, str(e)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected(): connection.close()