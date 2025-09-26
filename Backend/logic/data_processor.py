# logic/data_processor.py
"""
Módulo de Lógica de Negocio (El Cerebro de la Aplicación).
Optimizado para rendimiento con Caching y Lazy API de Polars.
"""
# --- Importaciones ---
import math
import polars as pl
import pandas as pd
import io

# Módulos locales
from config import settings
from extensions import cache

# Importar obtener_datos_glosas desde db/mySQL_connector
# NOTA: Asegúrate de que no haya importaciones circulares. Si `db.mySQL_connector` importa
# desde `logic.data_processor`, esta estructura podría dar problemas.
from db.mySQL_connector import obtener_datos_glosas

# ==============================================================================
# SECCIÓN: OBTENCIÓN Y CACHEO DE DATOS
# ==============================================================================

@cache.memoize(timeout=600)
def _obtener_y_limpiar_datos_base_cache(fecha_inicio: str = None, fecha_fin: str = None) -> pl.DataFrame:
    """Función interna y cacheada para obtener y realizar la limpieza inicial de los datos."""
    print(f"¡SIN CACHÉ! Accediendo a la BD para el rango {fecha_inicio} a {fecha_fin}")
    
    registros, error = obtener_datos_glosas(fecha_inicio, fecha_fin)
    if error:
        raise Exception(f"Error en capa de datos al obtener glosas: {error}")
    if not registros:
        print("Advertencia: La consulta a la base de datos no devolvió registros.")
        return pl.DataFrame()

    schema_forzado = {
        settings.COL_FECHA_NOTIFICACION: pl.Date, 
        settings.COL_FECHA_OBJECION: pl.Date, 
        settings.COL_FECHA_RADICADO: pl.Date, 
        settings.COL_FECHA_CONTESTACION: pl.Date,
        settings.COL_CARPETA_CC: pl.Int64,
        settings.COL_VR_GLOSA: pl.Float64,
        "saldocartera": pl.Float64,
    }

    df = pl.DataFrame(registros, schema_overrides=schema_forzado)

    df = df.with_columns(
        pl.col(settings.COL_CARPETA_CC).fill_null(0),
        pl.col(settings.COL_VR_GLOSA).fill_null(0),
        pl.col("saldocartera").fill_null(0),
    )
    
    return df.filter(pl.col(settings.COL_ESTATUS).is_in(settings.VALID_ESTATUS_VALUES))

# ==============================================================================
# SECCIÓN: CREACIÓN DE TABLAS REUTILIZABLES
# ==============================================================================

def crear_tabla_resumen_detalle_polars(df_items: pl.DataFrame) -> pl.DataFrame:
    """
    Función reutilizable que toma un DataFrame de ítems y crea la tabla
    combinada con filas de "Resumen Factura" y "Detalle Ítem".
    """
    if df_items.is_empty():
        return df_items

    df_items_con_extras = df_items.with_columns(
        (pl.col(settings.COL_SERIE).cast(pl.Utf8).fill_null("") + 
         pl.col(settings.COL_N_FACTURA).cast(pl.Utf8).fill_null("")).alias("FACTURA"),
        (pl.col(settings.COL_CARPETA_CC) != 0).alias("_tiene_cc"),
        pl.col(settings.COL_FECHA_RADICADO).is_not_null().alias("_tiene_fr")
    )

    df_resumen = df_items_con_extras.group_by(settings.GROUP_BY_FACTURA + ["FACTURA"]).agg(
        pl.first(settings.COL_ENTIDAD), 
        pl.first(settings.COL_FECHA_NOTIFICACION),
        pl.min(settings.COL_FECHA_OBJECION).alias(settings.COL_FECHA_OBJECION),
        pl.max(settings.COL_FECHA_CONTESTACION).alias(settings.COL_FECHA_CONTESTACION),
        pl.count().cast(pl.UInt32).alias("Total_Items_Factura"),
        (pl.col("_tiene_cc") & pl.col("_tiene_fr")).sum().cast(pl.UInt32).alias("Items_ConCC_ConFR"),
        (pl.col("_tiene_cc") & ~pl.col("_tiene_fr")).sum().cast(pl.UInt32).alias("Items_ConCC_SinFR"),
        (~pl.col("_tiene_cc") & pl.col("_tiene_fr")).sum().cast(pl.UInt32).alias("Items_SinCC_ConFR"),
        (~pl.col("_tiene_cc") & ~pl.col("_tiene_fr")).sum().cast(pl.UInt32).alias("Items_SinCC_SinFR"),
        pl.first("saldocartera").alias(settings.COL_VR_GLOSA),
        pl.first(settings.COL_TIPO).alias(settings.COL_TIPO)
    ).with_columns(
        pl.lit("Resumen Factura").alias("TipoFila"),
        pl.lit(None, dtype=pl.Int64).alias(settings.COL_CARPETA_CC),
        pl.lit(None, dtype=pl.Date).alias(settings.COL_FECHA_RADICADO),
        pl.lit(None, dtype=pl.Utf8).alias(settings.COL_ESTATUS)
    )

    df_detalle = df_items_con_extras.with_columns(
        pl.lit("Detalle Ítem").alias("TipoFila"),
        pl.when(pl.col(settings.COL_CARPETA_CC) == 0).then(None).otherwise(pl.col(settings.COL_CARPETA_CC)).cast(pl.Int64),
        *[pl.lit(None, dtype=pl.UInt32).alias(c) for c in [
            "Total_Items_Factura", "Items_ConCC_ConFR", "Items_ConCC_SinFR", 
            "Items_SinCC_ConFR", "Items_SinCC_SinFR"
        ]]
    ).with_columns(pl.col(settings.COL_ESTATUS).cast(pl.Utf8))
    
    df_combinado = pl.concat([df_resumen, df_detalle], how="diagonal")

    # Un orden simple es suficiente, el resto de la lógica no depende de un orden complejo aquí.
    df_ordenado = df_combinado.sort(settings.GROUP_BY_FACTURA + ["FACTURA", "TipoFila"])
    
    # Selecciona solo las columnas que son relevantes para el reporte final para mantener la consistencia
    columnas_finales_deseadas = [col for col in settings.COLUMN_NAME_MAPPING_EXPORT if col in df_ordenado.columns]
    
    return df_ordenado.select(columnas_finales_deseadas)

# ==============================================================================
# SECCIÓN: LÓGICA DE ENDPOINTS
# ==============================================================================

def generar_y_comprobar_todas_las_tablas(fecha_inicio: str = None, fecha_fin: str = None) -> tuple:
    """
    Función orquestadora principal para el dashboard, con lógica de conteo unificada
    y cálculo de datos para la serie de tiempo de ingresos.
    """
    df_base = _obtener_y_limpiar_datos_base_cache(fecha_inicio, fecha_fin)

    if df_base.is_empty():
        return {}, {"error": "No hay datos en el rango de fechas seleccionado."}

    # 1. Ejecutar el plan de clasificación Lazy
    lazy_df = df_base.lazy()
    lazy_clasificado = lazy_df.with_columns(
        pl.concat_str([pl.col(c).cast(pl.Utf8).fill_null("") for c in settings.GROUP_BY_FACTURA], separator="-").alias("factura_id")
    )
    
    conds = {
        "T1": (pl.col(settings.COL_CARPETA_CC) != 0) & (pl.col(settings.COL_FECHA_RADICADO).is_not_null()),
        "T2": (pl.col(settings.COL_CARPETA_CC) != 0) & (pl.col(settings.COL_FECHA_RADICADO).is_null()),
        "T3": (pl.col(settings.COL_CARPETA_CC) == 0) & (pl.col(settings.COL_FECHA_RADICADO).is_null()),
        "T4": (pl.col(settings.COL_CARPETA_CC) == 0) & (pl.col(settings.COL_FECHA_RADICADO).is_not_null())
    }

    plan_final = lazy_clasificado.with_columns(
        [(cond.sum().over("factura_id") == pl.count().over("factura_id")).alias(f"es_{tipo}") for tipo, cond in conds.items()]
    )

    df_final = plan_final.collect()

    # --- INICIO DE LA LÓGICA DE CONTEO UNIFICADA ---

    # 2. Crear la "Fuente de Verdad": una fila por factura con su categoría
    df_facturas_unicas = df_final.with_columns(
        pl.when(pl.col("es_T1")).then(pl.lit("T1"))
          .when(pl.col("es_T2")).then(pl.lit("T2"))
          .when(pl.col("es_T3")).then(pl.lit("T3"))
          .when(pl.col("es_T4")).then(pl.lit("T4"))
          .otherwise(pl.lit("Mixtas")).alias("CategoriaFactura")
    ).select(
        "factura_id", 
        "saldocartera", 
        "CategoriaFactura", 
        settings.COL_ENTIDAD,
        # --- CORRECCIÓN AQUÍ: Añadimos la columna de fecha que necesitamos más tarde ---
        settings.COL_FECHA_NOTIFICACION
    ).unique(subset="factura_id", keep="first")
    
    s_counts = {}

    # 3. Calcular KPIs desde la fuente unificada
    conteo_por_categoria = df_facturas_unicas.group_by("CategoriaFactura").agg(pl.count().alias("conteo"))
    for row in conteo_por_categoria.iter_rows(named=True):
        categoria = row["CategoriaFactura"].lower()
        s_counts[f"facturas_{categoria}"] = row["conteo"]

    categorias_no_radicadas = ["T2", "T3", "T4", "Mixtas"]
    df_no_radicadas_unicas = df_facturas_unicas.filter(pl.col("CategoriaFactura").is_in(categorias_no_radicadas))
    df_no_radicadas = df_final.join(df_no_radicadas_unicas.select("factura_id"), on="factura_id", how="inner")

    s_counts["valor_total_periodo"] = df_facturas_unicas["saldocartera"].sum() or 0
    s_counts["valor_total_radicado"] = df_facturas_unicas.filter(pl.col("CategoriaFactura") == "T1")["saldocartera"].sum() or 0
    s_counts["valor_total_no_radicado"] = df_facturas_unicas.filter(pl.col("CategoriaFactura") != "T1")["saldocartera"].sum() or 0

    if not df_no_radicadas.is_empty():
        conteo_entidad_df = df_no_radicadas_unicas.group_by(settings.COL_ENTIDAD).agg(pl.count().alias("total_facturas")).sort("total_facturas", descending=True)
        s_counts["conteo_por_entidad"] = conteo_entidad_df.limit(15).to_dicts()
        # --- INICIO DE LA NUEVA LÓGICA ---
        # Calculamos el TOP 10 de entidades por saldo en cartera no radicada
        print("Calculando Top 10 de entidades por saldo no radicado...")
        saldo_entidad_df = df_no_radicadas_unicas.group_by(settings.COL_ENTIDAD).agg(
            pl.sum("saldocartera").alias("total_saldo")
        ).sort("total_saldo", descending=True)
        
        # Guardamos el Top 10 en el diccionario s_counts
        s_counts["saldo_por_entidad_top10"] = saldo_entidad_df.limit(15).to_dicts()
        # --- FIN DE LA NUEVA LÓGICA ---
        s_counts["conteo_por_estatus"] = df_no_radicadas.group_by(settings.COL_ESTATUS).agg(pl.count().alias("total_items")).sort(by=settings.COL_ESTATUS).to_dicts()
    else:
        s_counts["conteo_por_entidad"] = []
        s_counts["saldo_por_entidad_top10"] = []    
        s_counts["conteo_por_estatus"] = []

    # 4. DataFrames para exportación
    dfs = {}
    for cat in ["T1", "T2", "T3", "T4", "Mixtas"]:
        ids_cat = df_facturas_unicas.filter(pl.col("CategoriaFactura") == cat).select("factura_id")
        dfs[cat] = df_final.join(ids_cat, on="factura_id", how="inner")

    # 5. Comprobación de integridad
    s_counts["total_facturas_base"] = df_facturas_unicas.height
    s_counts["suma_categorizadas"] = sum(v for k, v in s_counts.items() if k.startswith('facturas_'))
    s_counts["comprobacion_exitosa"] = s_counts["total_facturas_base"] == s_counts["suma_categorizadas"]

    # --- LÓGICA PARA GRÁFICO DE INGRESO DE GLOSAS (Ahora funcionará) ---
    print("Calculando datos para el gráfico de ingresos...")
    df_ingresos = df_facturas_unicas.select(["factura_id", settings.COL_FECHA_NOTIFICACION]).sort(settings.COL_FECHA_NOTIFICACION)
    
    if df_ingresos.is_empty() or df_ingresos[settings.COL_FECHA_NOTIFICACION].is_null().all():
        s_counts["ingresos_por_periodo"] = []
        s_counts["granularidad_ingresos"] = "Diario"
    else:
        min_date = df_ingresos[settings.COL_FECHA_NOTIFICACION].min()
        max_date = df_ingresos[settings.COL_FECHA_NOTIFICACION].max()
        
        dias_rango = (max_date - min_date).days
        if dias_rango > 365 * 2:
            granularidad_txt, granularidad_polars = "Anual", "1y"
        elif dias_rango > 90:
            granularidad_txt, granularidad_polars = "Mensual", "1mo"
        else:
            granularidad_txt, granularidad_polars = "Diario", "1d"
            
        print(f"Rango de {dias_rango} días. Granularidad seleccionada: {granularidad_txt}")
        df_agrupado = df_ingresos.group_by_dynamic(index_column=settings.COL_FECHA_NOTIFICACION, every=granularidad_polars).agg(pl.count().alias("conteo"))
        
        s_counts["granularidad_ingresos"] = granularidad_txt
        s_counts["ingresos_por_periodo"] = df_agrupado.rename({settings.COL_FECHA_NOTIFICACION: "fecha_agrupada"}).to_dicts()

    dfs["df_base"] = df_final
    return dfs, s_counts

def obtener_resumenes_paginados(fecha_inicio: str, fecha_fin: str, categorias: list, pagina: int, por_pagina: int, entidad: str = None) -> dict:
    """Obtiene resúmenes de facturas, filtra y pagina. Versión corregida."""
    print(f"Obteniendo Resúmenes: Categorías={categorias}, Página={pagina}, Entidad={entidad}")

    df_base = _obtener_y_limpiar_datos_base_cache(fecha_inicio, fecha_fin)

    if df_base.is_empty():
        return {"data": [], "pagina_actual": 1, "total_paginas": 0, "total_registros": 0}
        
    df_resumen_completo = crear_tabla_resumen_detalle_polars(df_base)

    df_resumenes = df_resumen_completo.filter(pl.col("TipoFila") == "Resumen Factura")

    df_resumenes_con_categoria = df_resumenes.with_columns(
        pl.when(pl.col("Items_ConCC_ConFR") == pl.col("Total_Items_Factura")).then(pl.lit("T1"))
          .when(pl.col("Items_ConCC_SinFR") == pl.col("Total_Items_Factura")).then(pl.lit("T2"))
          .when(pl.col("Items_SinCC_SinFR") == pl.col("Total_Items_Factura")).then(pl.lit("T3"))
          .when(pl.col("Items_SinCC_ConFR") == pl.col("Total_Items_Factura")).then(pl.lit("T4"))
          .otherwise(pl.lit("Mixtas")).alias("CategoriaFactura")
    )
    
    df_resumenes_filtrados = df_resumenes_con_categoria
    
    if categorias:
        df_resumenes_filtrados = df_resumenes_filtrados.filter(pl.col("CategoriaFactura").is_in(categorias))

    if entidad:
        df_resumenes_filtrados = df_resumenes_filtrados.filter(pl.col(settings.COL_ENTIDAD) == entidad)
        
    # --- INICIO DE LA NUEVA LÓGICA ---
    
    # Calculamos el saldo total ANTES de paginar
    # Usamos la columna correcta que contiene el saldo ('saldocartera' o 'vr_glosa' en el resumen)
    # Basado en tu función `crear_tabla_resumen...`, el saldo está en 'vr_glosa' para los resúmenes.
    saldo_total_acumulado = df_resumenes_filtrados[settings.COL_VR_GLOSA].sum() or 0
    print(f"Saldo acumulado para esta sección: {saldo_total_acumulado}")
    
    # --- FIN DE LA NUEVA LÓGICA ---
        
    total_registros = len(df_resumenes_filtrados)
    if total_registros == 0:
        return {"data": [], "pagina_actual": pagina, "total_paginas": 0, "total_registros": 0}
    
    total_paginas = math.ceil(total_registros / por_pagina)
    offset = (pagina - 1) * por_pagina
    df_pagina = df_resumenes_filtrados.slice(offset, por_pagina)
    
    datos_dict = df_pagina.with_columns(
        pl.col(pl.Date).dt.strftime("%Y-%m-%d")
    ).fill_null("").to_dicts()

    return {
        "data": datos_dict, 
        "pagina_actual": pagina, 
        "total_paginas": total_paginas, 
        "total_registros": total_registros,
        "saldo_total_acumulado": saldo_total_acumulado
        }

def obtener_detalle_especifico_factura(docn: int) -> list:
    """Obtiene los ítems de detalle para un único gl_docn."""
    print(f"Obteniendo detalle para gl_docn: {docn}")
    df_base_completa = _obtener_y_limpiar_datos_base_cache(None, None)
    
    df_items_factura = df_base_completa.filter(pl.col(settings.COL_GL_DOCN) == docn)
    
    if df_items_factura.is_empty():
        return []

    df_detalle_final = crear_tabla_resumen_detalle_polars(df_items_factura).filter(
        pl.col("TipoFila") == "Detalle Ítem"
    )
    
    return df_detalle_final.to_dicts()

def _create_factura_id_column(df: pl.DataFrame) -> pl.DataFrame:
    """Añade una columna 'factura_id' combinando serie y número de factura."""
    return df.with_columns(
        (pl.col(settings.COL_SERIE).cast(pl.Utf8).fill_null("") + 
         pl.col(settings.COL_N_FACTURA).cast(pl.Utf8).fill_null("")).alias("factura_id")
    )


def buscar_facturas_completas(lista_ids_factura_str: list) -> dict:
    """Busca facturas por una lista de formatos completos y preserva los duplicados de la entrada."""
    df_base = _obtener_y_limpiar_datos_base_cache(None, None)
    
    if df_base.is_empty():
        return {"encontrados": [], "no_encontrados": lista_ids_factura_str, "saldo_total_acumulado": 0}

    df_base_con_factura_id = _create_factura_id_column(df_base)
    
    # Limpiamos la entrada del usuario
    ids_busqueda_limpios = [str(item).strip() for item in lista_ids_factura_str if str(item).strip()]
    ids_busqueda_unicos = list(set(ids_busqueda_limpios))

    # Buscamos solo los IDs únicos para ser eficientes
    df_encontrados_unicos = df_base_con_factura_id.filter(
        pl.col("factura_id").is_in(ids_busqueda_unicos)
    )
    
    if df_encontrados_unicos.is_empty():
        return {"encontrados": [], "no_encontrados": ids_busqueda_limpios, "saldo_total_acumulado": 0}

    # --- Lógica para Replicar Duplicados ---
    # Convertimos los resultados únicos a un diccionario para fácil acceso
    resultados_dict = {
        row["factura_id"]: df_encontrados_unicos.filter(pl.col("factura_id") == row["factura_id"])
        for row in df_encontrados_unicos.select("factura_id").unique().iter_rows(named=True)
    }

    # Construimos el DataFrame final respetando el orden y duplicados de la entrada
    resultados_finales_list = []
    ids_encontrados_set = set()
    for fact_id in ids_busqueda_limpios:
        if fact_id in resultados_dict:
            resultados_finales_list.append(resultados_dict[fact_id])
            ids_encontrados_set.add(fact_id)

    df_encontrados_items = pl.concat(resultados_finales_list) if resultados_finales_list else pl.DataFrame()

    df_tabla_final = crear_tabla_resumen_detalle_polars(df_encontrados_items)
    
    # No encontrados son los que no estaban en el diccionario de resultados
    no_encontrados = [fact_id for fact_id in ids_busqueda_limpios if fact_id not in ids_encontrados_set]
    
    df_resumenes = df_tabla_final.filter(pl.col("TipoFila") == "Resumen Factura")
    saldo_acumulado = df_resumenes[settings.COL_VR_GLOSA].sum() or 0
    
    return {
        "encontrados": df_tabla_final.to_dicts(),
        "no_encontrados": no_encontrados,
        "saldo_total_acumulado": saldo_acumulado
    }

def generar_excel_en_memoria(dataframes: dict) -> io.BytesIO:
    """
    Genera el archivo Excel a partir de los DataFrames categorizados,
    eliminando la parte de la hora de las fechas a nivel de datos.
    """
    buffer = io.BytesIO()
    nombres_hojas = {"T1": "Radicadas", "T2": "Con CC y Sin FR", "T3": "Sin CC y Sin FR", "T4": "Sin CC y Con FR", "Mixtas": "Mixtas"}
    
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        workbook = writer.book
        # Este formato ahora servirá como un extra, pero la clave es el cambio en los datos.
        formato_fecha = workbook.add_format({'num_format': 'dd/mm/yyyy'})
        
        sheets_to_process = {k: v for k, v in dataframes.items() if k in nombres_hojas}
        
        for key_df, df_items_polars in sheets_to_process.items():
            sheet_name = nombres_hojas.get(key_df, key_df)
            print(f"Procesando hoja '{sheet_name}' para Excel...")

            if not df_items_polars.is_empty():
                df_reporte = crear_tabla_resumen_detalle_polars(df_items_polars)
                df_pandas = df_reporte.to_pandas()
                df_pandas.rename(columns=settings.COLUMN_NAME_MAPPING_EXPORT, inplace=True)

                # --- INICIO DE LA SOLUCIÓN DEFINITIVA ---
                
                # 1. Identificamos las columnas de fecha por su nombre ya renombrado.
                columnas_de_fecha_excel = [
                    col for col in df_pandas.columns if 'fecha' in col.lower()
                ]

                # 2. Iteramos sobre esas columnas para TRUNCAR la hora.
                for col_fecha in columnas_de_fecha_excel:
                    # Convertimos a datetime de pandas.
                    df_pandas[col_fecha] = pd.to_datetime(df_pandas[col_fecha], errors='coerce')
                    # LA LÍNEA MÁGICA: .dt.date extrae solo la parte de la fecha,
                    # descartando la hora. Los valores nulos (NaT) se mantienen.
                    df_pandas[col_fecha] = df_pandas[col_fecha].dt.date

                # --- FIN DE LA SOLUCIÓN DEFINITIVA ---

                # Ahora escribimos el DataFrame con los datos ya limpios.
                df_pandas.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Aplicamos formato y auto-ancho como mejora visual.
                worksheet = writer.sheets[sheet_name]
                for idx, col_name in enumerate(df_pandas.columns):
                    max_len = max(df_pandas[col_name].astype(str).map(len).max(), len(col_name)) + 2
                    if col_name in columnas_de_fecha_excel:
                        worksheet.set_column(idx, idx, 12, formato_fecha) # 12 es un ancho fijo bueno para fechas
                    else:
                        worksheet.set_column(idx, idx, max_len)
                        
                print(f"Hoja '{sheet_name}' escrita y formateada.")
            else:
                print(f"Hoja '{sheet_name}' omitida por estar vacía.")

    buffer.seek(0)
    return buffer

def generar_excel_busqueda_en_memoria(lista_ids_factura: list) -> io.BytesIO:
    """
    Genera un archivo Excel en memoria para las facturas específicas de una búsqueda.
    """
    # 1. Obtener los datos completos para los IDs de factura proporcionados.
    # La función `buscar_facturas_completas` ya nos da la estructura que necesitamos.
    resultados_busqueda = buscar_facturas_completas(lista_ids_factura)
    df_encontrados_polars = pl.DataFrame(resultados_busqueda['encontrados'])

    if df_encontrados_polars.is_empty():
        # Si no se encontró nada, devolvemos un buffer vacío o podríamos lanzar un error.
        return io.BytesIO()

    # 2. Preparar el buffer y el ExcelWriter.
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        workbook = writer.book
        formato_fecha = workbook.add_format({'num_format': 'dd/mm/yyyy'})
        sheet_name = "Resultado Búsqueda"

        # 3. Convertir a Pandas y renombrar columnas.
        # No necesitamos `crear_tabla_resumen_detalle_polars` porque `buscar_facturas_completas` ya lo hace.
        df_pandas = df_encontrados_polars.to_pandas()
        df_pandas.rename(columns=settings.COLUMN_NAME_MAPPING_EXPORT, inplace=True)

        # 4. Limpiar y formatear fechas (exactamente como en la otra función).
        columnas_de_fecha_excel = [col for col in df_pandas.columns if 'fecha' in col.lower()]
        for col_fecha in columnas_de_fecha_excel:
            df_pandas[col_fecha] = pd.to_datetime(df_pandas[col_fecha], errors='coerce').dt.date

        # 5. Escribir en la hoja de Excel.
        df_pandas.to_excel(writer, sheet_name=sheet_name, index=False)

        # 6. Auto-ajustar el ancho de las columnas.
        worksheet = writer.sheets[sheet_name]
        for idx, col_name in enumerate(df_pandas.columns):
            max_len = max(df_pandas[col_name].astype(str).map(len).max(), len(col_name)) + 2
            if col_name in columnas_de_fecha_excel:
                worksheet.set_column(idx, idx, 12, formato_fecha)
            else:
                worksheet.set_column(idx, idx, max_len)
        
        print(f"Hoja '{sheet_name}' para la búsqueda ha sido escrita y formateada.")

    # 7. Devolver el buffer para ser enviado como archivo.
    buffer.seek(0)
    return buffer