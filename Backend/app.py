# app.py
# --- Importaciones ---
# Módulos estándar y de Flask
import traceback
import datetime
import io
import time
from functools import wraps
import polars as pl
from flask import Flask, jsonify, send_file, request
from flask_cors import CORS
from dotenv import load_dotenv
from flask_caching import Cache
from extensions import cache

# Módulos específicos de la aplicación
from config import settings
from db.mySQL_connector import obtener_rango_fechas
from logic.data_processor import (
    buscar_facturas_completas,
    generar_y_comprobar_todas_las_tablas,
    generar_excel_en_memoria,
    obtener_resumenes_paginados,
    obtener_detalle_especifico_factura
)

# --- Decorador para Medir Tiempo de Ejecución ---
def log_execution_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        execution_time_ms = (end_time - start_time) * 1000
        print(f"Endpoint '{func.__name__}' ejecutado en {execution_time_ms:.2f} ms")
        return result
    return wrapper

# --- Configuración Inicial de la Aplicación ---
load_dotenv()  # Carga las variables de entorno desde el archivo .env
app = Flask(__name__)  # Inicializa la aplicación Flask

# Configura CORS (Cross-Origin Resource Sharing) para permitir que el frontend
# (que corre en otro dominio, ej. mi-dashboard.test) se comunique con esta API.
# 'expose_headers' permite que el JavaScript del frontend pueda leer cabeceras
# como 'Content-Disposition' para obtener el nombre del archivo al descargar.
CORS(app, expose_headers=['Content-Disposition'])

# --- 2. Configurar y vincular el caché con tu aplicación ---
# Usamos 'simple' que guarda el caché en la memoria del servidor. Es perfecto para empezar.
# Para producción, se podrían usar sistemas más robustos como 'redis' o 'memcached'.
cache.init_app(app, config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 300  # Tiempo por defecto en segundos (5 minutos)
})

# ==============================================================================
# SECCIÓN: ENDPOINTS DE LA API
# ==============================================================================

@app.route('/api/reportes/rango-fechas', methods=['GET'])
@log_execution_time
@cache.cached(timeout= 3600) # 1hr
def get_rango_fechas():
    """
    Endpoint de inicialización.
    Devuelve la fecha mínima y máxima de los datos para que el frontend pueda
    configurar los selectores de fecha con un rango válido.
    """
    try:
        rango, error = obtener_rango_fechas()
        # Si hubo un error en la BD que no sea "tabla vacía", lo lanzamos.
        if error and "No se encontraron fechas" not in error:
            raise Exception(error)
        
        # Formatear las fechas a YYYY-MM-DD, el formato estándar para <input type="date">
        if rango:
            if rango.get('fecha_min'):
                rango['fecha_min'] = rango['fecha_min'].strftime('%Y-%m-%d')
            if rango.get('fecha_max'):
                rango['fecha_max'] = rango['fecha_max'].strftime('%Y-%m-%d')
            
        return jsonify({'success': True, 'data': rango}), 200
    except Exception as e:
        traceback.print_exc() # Imprime el error completo en la consola del servidor para depuración.
        return jsonify({'success': False, 'message': 'Error al obtener rango de fechas.', 'error': str(e)}), 500


@app.route('/api/reportes/analizar-y-comprobar', methods=['GET'])
@log_execution_time
# --- Decorador de Caché ---
# timeout=300: Cachea por 5 minutos.
# query_string=True: CRÍTICO. Crea una clave de caché diferente para cada combinación
# de fecha_inicio y fecha_fin. Así, el análisis de Enero no se confunde con el de Febrero.
@cache.cached(timeout=300, query_string=True)
def analizar_y_comprobar():
    """
    Endpoint principal para el dashboard.
    Ejecuta el análisis completo basado en el rango de fechas proporcionado por el usuario
    y devuelve el JSON con todos los KPIs y datos agregados para los gráficos.
    """
    try:
        # Extrae los parámetros de filtro de la URL (ej. ?fecha_inicio=2024-01-01)
        fecha_inicio = request.args.get('fecha_inicio')
        fecha_fin = request.args.get('fecha_fin')
        
        # Llama a la función orquestadora principal de la capa de lógica.
        _, comprobacion = generar_y_comprobar_todas_las_tablas(fecha_inicio, fecha_fin)
        
        # Enriquece la respuesta con una marca de tiempo para que el usuario sepa cuándo se generó.
        comprobacion["timestamp_analisis"] = datetime.datetime.now().isoformat()
        
        return jsonify({
            'success': True,
            'message': 'Análisis finalizado con éxito.',
            'data': comprobacion
        }), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'Ocurrió un error durante el análisis.', 'error': str(e)}), 500


@app.route('/api/reportes/descargar-excel', methods=['GET'])
@log_execution_time
@cache.cached(timeout=300, query_string=True)
def descargar_excel():
    """
    Endpoint para la descarga del reporte.
    Vuelve a ejecutar el análisis para garantizar la consistencia de los datos y
    envía el archivo Excel generado como una respuesta binaria.
    """
    try:
        fecha_inicio = request.args.get('fecha_inicio')
        fecha_fin = request.args.get('fecha_fin')
        
        # Vuelve a ejecutar la lógica principal. Esto asegura que el Excel refleje
        # exactamente los mismos filtros que el dashboard (diseño sin estado).
        dataframes, _ = generar_y_comprobar_todas_las_tablas(fecha_inicio, fecha_fin)
        
        # Comprueba si el análisis devolvió algún dato para evitar generar un Excel vacío.
        if not dataframes or all(df.is_empty() for df in dataframes.values()):
            return jsonify({'success': False, 'message': 'No se encontraron datos para generar el Excel con los filtros aplicados.'}), 404
        
        # Construye un nombre de archivo dinámico y descriptivo.
        nombre_periodo = f"{fecha_inicio}_a_{fecha_fin}" if fecha_inicio and fecha_fin else datetime.date.today().isoformat()
        nombre_archivo = f"{settings.EXCEL_OUTPUT_FILENAME_BASE}_{nombre_periodo}.xlsx"
        
        print(f"Generando archivo Excel en memoria: {nombre_archivo}")
        buffer = generar_excel_en_memoria(dataframes)
        
        # Crea una copia del buffer en memoria para enviar.
        buffer_to_send = io.BytesIO(buffer.getvalue())

        # Utiliza send_file para manejar el envío del archivo binario.
        return send_file(
            buffer_to_send,
            as_attachment=True,  # Le dice al navegador que lo descargue en lugar de mostrarlo.
            download_name=nombre_archivo,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'Error al generar el archivo Excel.', 'error': str(e)}), 500


# CÓDIGO CORREGIDO Y SEGURO para app.py
@app.route('/api/reportes/resumenes-paginados', methods=['GET'])
@log_execution_time
@cache.cached(timeout=300, query_string=True)
def get_resumenes_paginados():
    try:
        fecha_inicio = request.args.get('fecha_inicio')
        fecha_fin = request.args.get('fecha_fin')
        categorias_str = request.args.get('categorias')
        pagina = request.args.get('pagina', 1, type=int)
        entidad = request.args.get('entidad', None)
        por_pagina = 20

        if not all([fecha_inicio, fecha_fin, categorias_str]):
            return jsonify({'success': False, 'message': 'Faltan parámetros requeridos (fechas, categorias).'}), 400

        lista_categorias = categorias_str.split(',')

        # --- CORRECCIÓN CLAVE AQUÍ ---
        # Usar argumentos con nombre para evitar errores de posición
        resultado_paginado = obtener_resumenes_paginados(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            categorias=lista_categorias,
            pagina=pagina,
            por_pagina=por_pagina,
            entidad=entidad
        )
        
        return jsonify({'success': True, 'data': resultado_paginado}), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'Error al obtener resúmenes paginados.', 'error': str(e)}), 500


@app.route('/api/reportes/detalle-factura', methods=['GET'])
@log_execution_time
@cache.cached(timeout=300, query_string=True)
def get_detalle_factura_individual():
    """
    Endpoint para el acordeón en la vista de detalle.
    Devuelve los "Ítems de Detalle" para un único `gl_docn`, permitiendo la carga
    perezosa (lazy-loading) de los detalles.
    """
    try:
        docn_str = request.args.get('docn')

        if not docn_str:
            return jsonify({'success': False, 'message': 'Falta el identificador gl_docn.'}), 400

        try:
            docn = int(docn_str)
        except (ValueError, TypeError):
            return jsonify({'success': False, 'message': 'El gl_docn debe ser un número válido.'}), 400
        
        items_detalle = obtener_detalle_especifico_factura(docn)
        
        # Formatea la respuesta a JSON.
        df = pl.DataFrame(items_detalle)
        datos_json = []
        if not df.is_empty():
            datos_json = df.with_columns(
                pl.col(pl.Datetime).dt.strftime('%Y-%m-%d')
            ).fill_null("").to_dicts()

        return jsonify({'success': True, 'data': datos_json}), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'Error al obtener el detalle de la factura.', 'error': str(e)}), 500

@app.route('/api/reportes/buscar-facturas', methods=['POST'])
@log_execution_time
def buscar_facturas_por_id():
    """
    Endpoint para buscar facturas por una lista de formatos de factura completos.
    Acepta un JSON con una lista de IDs de factura (ej: FCR123456) y devuelve los datos
    encontrados y no encontrados.
    """
    try:
        data = request.get_json()
        if not data or 'ids' not in data:
            return jsonify({'success': False, 'message': 'Faltan los identificadores en la petición.'}), 400
        
        lista_ids_factura = data['ids'] # Ahora sabemos que son formatos de factura
        if not isinstance(lista_ids_factura, list):
             return jsonify({'success': False, 'message': 'Los identificadores deben ser una lista.'}), 400

        # Llamamos a la nueva función de lógica que buscrá SOLO por factura
        resultados = buscar_facturas_completas(lista_ids_factura) # Renombrada la función
        
        return jsonify({'success': True, 'data': resultados}), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'Error durante la búsqueda de facturas.', 'error': str(e)}), 500
    
# Punto de entrada para ejecutar la aplicación
if __name__ == '__main__':
    # 'host=0.0.0.0' hace que el servidor sea accesible desde otros dispositivos en la red.
    # 'debug=True' activa el modo de depuración, que recarga el servidor automáticamente con los cambios.
    app.run(host='0.0.0.0', port=5000, debug=True)
    
    