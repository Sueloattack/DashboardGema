# app.py
import polars as pl
from flask import Flask, jsonify, send_file, request
from flask_cors import CORS
from dotenv import load_dotenv
import traceback
import datetime
import io

# Importaciones de la lógica y la BD
from config import settings
from db.mySQL_connector import obtener_rango_fechas
from logic.data_processor import (
    generar_y_comprobar_todas_las_tablas,
    generar_excel_en_memoria,
    obtener_resumenes_paginados, 
    obtener_detalle_especifico_factura
)

# Cargar variables de entorno y configurar la app Flask
load_dotenv()
app = Flask(__name__)
# 'expose_headers' es importante para que el JavaScript del frontend pueda
# leer cabeceras como 'Content-Disposition' y obtener el nombre del archivo.
CORS(app, expose_headers=['Content-Disposition'])

@app.route('/api/reportes/rango-fechas', methods=['GET'])
def get_rango_fechas():
    """Devuelve la fecha mínima y máxima para los selectores del frontend."""
    try:
        rango, error = obtener_rango_fechas()
        if error and "No se encontraron fechas" not in error:
            raise Exception(error)
        
        if rango:
            if rango.get('fecha_min'):
                rango['fecha_min'] = rango['fecha_min'].strftime('%Y-%m-%d')
            if rango.get('fecha_max'):
                rango['fecha_max'] = rango['fecha_max'].strftime('%Y-%m-%d')
            
        return jsonify({'success': True, 'data': rango}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'Error al obtener rango de fechas.', 'error': str(e)}), 500

@app.route('/api/reportes/analizar-y-comprobar', methods=['GET'])
def analizar_y_comprobar():
    """
    Endpoint principal para el dashboard. Ejecuta el análisis completo
    basado en un rango de fechas y devuelve el JSON con los resultados.
    """
    try:
        fecha_inicio = request.args.get('fecha_inicio')
        fecha_fin = request.args.get('fecha_fin')
        
        # Llama a la lógica principal pasándole las fechas del filtro
        _, comprobacion = generar_y_comprobar_todas_las_tablas(fecha_inicio, fecha_fin)
        
        # Añade la marca de tiempo al resultado
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
def descargar_excel():
    """
    Genera y devuelve el archivo Excel detallado. Esta API es "sin estado",
    por lo que recalcula los datos basándose en el mismo filtro de fechas.
    """
    try:
        fecha_inicio = request.args.get('fecha_inicio')
        fecha_fin = request.args.get('fecha_fin')
        
        # La lógica de negocio se vuelve a ejecutar con los mismos filtros
        dataframes, _ = generar_y_comprobar_todas_las_tablas(fecha_inicio, fecha_fin)
        
        if not dataframes or all(df.is_empty() for df in dataframes.values()):
            return jsonify({'success': False, 'message': 'No se encontraron datos para generar el Excel con los filtros aplicados.'}), 404
        
        # Construye un nombre de archivo dinámico
        if fecha_inicio and fecha_fin:
            nombre_periodo = f"{fecha_inicio}_a_{fecha_fin}"
        else:
            nombre_periodo = datetime.date.today().isoformat()
        
        nombre_archivo = f"{settings.EXCEL_OUTPUT_FILENAME_BASE}_{nombre_periodo}.xlsx"
        
        print(f"Generando archivo Excel en memoria: {nombre_archivo}")
        buffer = generar_excel_en_memoria(dataframes)
        
        # Usa io.BytesIO para evitar problemas con el puntero del buffer
        buffer_to_send = io.BytesIO(buffer.getvalue())

        return send_file(
            buffer_to_send,
            as_attachment=True,
            download_name=nombre_archivo,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'Error al generar el archivo Excel.', 'error': str(e)}), 500

# --- NUEVO ENDPOINT PARA DETALLES PAGINADOS (REEMPLAZA AL ANTERIOR /datos-detallados) ---
@app.route('/api/reportes/resumenes-paginados', methods=['GET'])
def get_resumenes_paginados():
    """
    Devuelve los RESÚMENES de factura para una o más categorías, con paginación.
    """
    try:
        fecha_inicio = request.args.get('fecha_inicio')
        fecha_fin = request.args.get('fecha_fin')
        # La categoría ahora puede ser una lista separada por comas
        categorias_str = request.args.get('categorias')
        pagina = request.args.get('pagina', 1, type=int)
        por_pagina = 30 # Definimos el tamaño de la página aquí

        if not all([fecha_inicio, fecha_fin, categorias_str]):
            return jsonify({'success': False, 'message': 'Faltan parámetros requeridos (fechas, categorias).'}), 400

        # Convertimos el string de categorías en una lista
        lista_categorias = categorias_str.split(',')

        resultado_paginado = obtener_resumenes_paginados(fecha_inicio, fecha_fin, lista_categorias, pagina, por_pagina)
        
        return jsonify({'success': True, 'data': resultado_paginado}), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'Error al obtener resúmenes paginados.', 'error': str(e)}), 500

# --- NUEVO ENDPOINT PARA EL DESPLEGABLE DE ÍTEMS ---
@app.route('/api/reportes/detalle-factura', methods=['GET'])
def get_detalle_factura_individual():
    """Devuelve los ÍTEMS de detalle para un único gl_docn."""
    try:
        # Ahora solo necesitamos un parámetro: docn
        docn_str = request.args.get('docn')

        if not docn_str:
            return jsonify({'success': False, 'message': 'Falta el identificador gl_docn.'}), 400

        # El gl_docn puede ser numérico o string, lo casteamos a int para ser seguros
        try:
            docn = int(docn_str)
        except (ValueError, TypeError):
            return jsonify({'success': False, 'message': 'El gl_docn debe ser un número válido.'}), 400
        
        items_detalle = obtener_detalle_especifico_factura(docn)

        # Formatear la salida para el JSON (código sin cambios)
        df = pl.DataFrame(items_detalle)
        if df.is_empty():
            datos_json = []
        else:
            datos_json = df.with_columns(
                pl.col(pl.Datetime).dt.strftime('%Y-%m-%d')
            ).fill_null("").to_dicts()

        return jsonify({'success': True, 'data': datos_json})

    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'Error al obtener el detalle de la factura.', 'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)