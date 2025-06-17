# app.py
from flask import Flask, jsonify, send_file
from flask_cors import CORS
from dotenv import load_dotenv
from config import settings
import traceback
import datetime
import io

from logic.data_processor import (
    generar_y_comprobar_todas_las_tablas,
    generar_excel_en_memoria
)

load_dotenv()
app = Flask(__name__)
CORS(app, expose_headers=['Content-Disposition'])

# -- CACHÉ EN MEMORIA AMPLIADA --
# Ahora también guardará el buffer del archivo Excel.
cache = {
    "dataframes": None,
    "comprobacion": None,
    "excel_buffer": None,  # <--- Nuevo
    "timestamp": None
}

def _ejecutar_analisis_si_es_necesario():
    """
    Ejecuta el análisis completo solo si la caché está vacía.
    Almacena los DataFrames y el resultado de la comprobación.
    IMPORTANTE: Si se ejecuta, limpia la caché del Excel, ya que los datos base cambiaron.
    """
    if cache["comprobacion"] is None:
        print("Caché de datos vacía. Ejecutando análisis completo...")
        dataframes, comprobacion = generar_y_comprobar_todas_las_tablas()
        cache["dataframes"] = dataframes
        cache["comprobacion"] = comprobacion
        cache["excel_buffer"] = None  # <--- Invalidar el buffer de Excel si los datos cambian
        cache["timestamp"] = datetime.datetime.now()
    else:
        print(f"Usando resultados del análisis en caché generados a las {cache['timestamp']}.")

@app.route('/api/reportes/comprobar-conteos', methods=['GET'])
def comprobar_conteos():
    """Endpoint que ejecuta el análisis (si es necesario) y devuelve el JSON de comprobación."""
    try:
        _ejecutar_analisis_si_es_necesario()
        comprobacion_respuesta = cache["comprobacion"].copy()
        comprobacion_respuesta["cache_timestamp"] = cache["timestamp"].isoformat() if cache["timestamp"] else None
        
        response = {
            'success': True,
            'message': 'Comprobación de conteos finalizada.',
            'data': comprobacion_respuesta
        }
        return jsonify(response), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'Error durante la comprobación.', 'error': str(e)}), 500

@app.route('/api/reportes/descargar-excel', methods=['GET'])
def descargar_excel():
    """
    Endpoint que devuelve el archivo Excel. Lo genera si no está en caché, 
    o lo sirve directamente desde la caché si ya existe.
    """
    try:
        # Asegurarse de que el análisis de datos se ha ejecutado al menos una vez
        _ejecutar_analisis_si_es_necesario()
        
        if cache["dataframes"] is None:
            return jsonify({'success': False, 'message': 'No hay datos para generar el Excel. Ejecute la comprobación primero.'}), 404

        # --- LÓGICA DE CACHÉ PARA EL EXCEL ---
        if cache["excel_buffer"] is None:
            print("Caché de Excel vacía. Generando archivo Excel en memoria...")
            # Genera el archivo Excel en memoria a partir de los DataFrames en caché
            buffer = generar_excel_en_memoria(cache["dataframes"])
            cache["excel_buffer"] = buffer # Almacena el buffer recién creado en la caché
        else:
            print("Sirviendo archivo Excel directamente desde la caché.")
            # Reutiliza el buffer existente desde la caché
            buffer = cache["excel_buffer"]

        # Es CRUCIAL rebobinar el buffer ANTES de cada envío, ya sea nuevo o cacheado,
        # porque la lectura lo deja al final.
        buffer.seek(0)
        
        # Copiamos el buffer a un nuevo objeto BytesIO para enviarlo.
        # Esto previene problemas si múltiples usuarios intentan descargar al mismo tiempo (thread-safety).
        buffer_to_send = io.BytesIO(buffer.getvalue())

        # Envía el archivo al cliente
        return send_file(
            buffer_to_send,
            as_attachment=True,
            download_name= settings.EXCEL_OUTPUT_FILENAME_BASE,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'Error al generar el archivo Excel.', 'error': str(e)}), 500
        
@app.route('/api/clear-cache', methods=['POST'])
def limpiar_cache():
    """Endpoint para invalidar TODA la caché."""
    global cache
    cache = {
        "dataframes": None,
        "comprobacion": None,
        "excel_buffer": None, # <--- También limpiar la caché del Excel
        "timestamp": None
    }
    print("Caché completa (datos y excel) limpiada por petición del cliente.")
    return jsonify({"success": True, "message": "Caché de análisis y Excel limpiada."})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)