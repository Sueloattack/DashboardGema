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
    para garantizar la consistencia entre los KPIs y las gráficas.
    """
    df_base = _obtener_y_limpiar_datos_base_cache(fecha_inicio, fecha_fin)

    if df_base.is_empty():
        return {}, {"error": "No hay datos en el rango de fechas seleccionado."}

    # 1. Ejecutar el plan de clasificación Lazy para determinar a qué categoría pertenece cada factura.
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
        [(cond.sum().over("factura_id") == pl.count().over("factura_id")).alias(f"es_{tipo}") 
         for tipo, cond in conds.items()]
    )

    df_final = plan_final.collect()

    # --- INICIO DE LA LÓGICA DE CONTEO UNIFICADA ---

    # 2. Crear LA ÚNICA FUENTE DE VERDAD para la categorización.
    #    Generamos un DataFrame con una fila por factura y su categoría final.
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
        settings.COL_ENTIDAD
    ).unique(subset="factura_id", keep="first")
    
    s_counts = {}

    # 3. Calcular todos los KPIs y conteos a partir de esta fuente unificada.
    # Conteo por cada categoría para los KPIs del donut chart
    conteo_por_categoria = df_facturas_unicas.group_by("CategoriaFactura").agg(pl.count().alias("conteo"))
    for row in conteo_por_categoria.iter_rows(named=True):
        categoria = row["CategoriaFactura"].lower()
        s_counts[f"facturas_{categoria}"] = row["conteo"]

    # Definimos las categorías "No Radicadas"
    categorias_no_radicadas = ["T2", "T3", "T4", "Mixtas"]
    
    # DataFrame con solo las facturas no radicadas. Esta es la base para la gráfica y el conteo de estatus.
    df_no_radicadas = df_final.join(
        df_facturas_unicas.filter(pl.col("CategoriaFactura").is_in(categorias_no_radicadas)),
        on="factura_id",
        how="inner"
    )

    # Cálculo de valores
    s_counts["valor_total_periodo"] = df_facturas_unicas["saldocartera"].sum() or 0
    s_counts["valor_total_radicado"] = df_facturas_unicas.filter(pl.col("CategoriaFactura") == "T1")["saldocartera"].sum() or 0
    s_counts["valor_total_no_radicado"] = df_facturas_unicas.filter(pl.col("CategoriaFactura") != "T1")["saldocartera"].sum() or 0

    # Conteo por entidad para la gráfica de barras
    if not df_no_radicadas.is_empty():
        conteo_entidad_df = df_no_radicadas.select(["factura_id", settings.COL_ENTIDAD]).unique().group_by(settings.COL_ENTIDAD).agg(pl.count().alias("total_facturas")).sort("total_facturas", descending=True)
        s_counts["conteo_por_entidad"] = conteo_entidad_df.limit(20).to_dicts() # Se mantiene el límite para visualización
        
        # Conteo por estatus (usa el mismo df_no_radicadas)
        s_counts["conteo_por_estatus"] = df_no_radicadas.group_by(settings.COL_ESTATUS).agg(pl.count().alias("total_items")).sort(by=settings.COL_ESTATUS).to_dicts()
        
        # --- Verificación en la consola del servidor ---
        kpi_no_radicadas = sum(s_counts.get(f"facturas_{cat.lower()}", 0) for cat in categorias_no_radicadas)
        suma_grafica = conteo_entidad_df["total_facturas"].sum()
        print("="*50)
        print(f"VERIFICACIÓN: KPI No Radicadas = {kpi_no_radicadas}")
        print(f"VERIFICACIÓN: Suma total de la gráfica de entidades = {suma_grafica}")
        print(f"¿Coinciden?: {kpi_no_radicadas == suma_grafica}")
        print("="*50)
        
    else:
        s_counts["conteo_por_entidad"] = []
        s_counts["conteo_por_estatus"] = []

    # 4. Creación de los DataFrames para la exportación a Excel
    dfs = {}
    for cat in ["T1", "T2", "T3", "T4", "Mixtas"]:
        ids_cat = df_facturas_unicas.filter(pl.col("CategoriaFactura") == cat).select("factura_id")
        dfs[cat] = df_final.join(ids_cat, on="factura_id", how="inner")

    # 5. Comprobación final de integridad
    s_counts["total_facturas_base"] = df_facturas_unicas.height
    s_counts["suma_categorizadas"] = sum(v for k, v in s_counts.items() if k.startswith('facturas_'))
    s_counts["comprobacion_exitosa"] = s_counts["total_facturas_base"] == s_counts["suma_categorizadas"]

    # --- (Omitida la lógica del gráfico de ingresos por brevedad) ---

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
        
    total_registros = len(df_resumenes_filtrados)
    if total_registros == 0:
        return {"data": [], "pagina_actual": pagina, "total_paginas": 0, "total_registros": 0}
    
    total_paginas = math.ceil(total_registros / por_pagina)
    offset = (pagina - 1) * por_pagina
    df_pagina = df_resumenes_filtrados.slice(offset, por_pagina)
    
    datos_dict = df_pagina.with_columns(
        pl.col(pl.Date).dt.strftime("%Y-%m-%d")
    ).fill_null("").to_dicts()

    return {"data": datos_dict, "pagina_actual": pagina, "total_paginas": total_paginas, "total_registros": total_registros}

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

def generar_excel_en_memoria(dataframes: dict) -> io.BytesIO:
    """Genera el archivo Excel a partir de los DataFrames categorizados."""
    buffer = io.BytesIO()
    nombres_hojas = {"T1": "Radicadas", "T2": "Con CC y Sin FR", "T3": "Sin CC y Sin FR", "T4": "Sin CC y Con FR", "Mixtas": "Mixtas"}
    
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        workbook = writer.book
        formato_fecha = workbook.add_format({'num_format': 'dd/mm/yyyy'})
        
        sheets_to_process = {k: v for k, v in dataframes.items() if k in nombres_hojas}
        
        for key_df, df_items_polars in sheets_to_process.items():
            sheet_name = nombres_hojas.get(key_df, key_df)
            print(f"Procesando hoja '{sheet_name}' para Excel...")

            if not df_items_polars.is_empty():
                df_reporte = crear_tabla_resumen_detalle_polars(df_items_polars)
                df_pandas = df_reporte.to_pandas()
                df_pandas = df_pandas.rename(columns=settings.COLUMN_NAME_MAPPING_EXPORT)
                
                df_pandas.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Formato post-escritura para mejor control
                worksheet = writer.sheets[sheet_name]
                for idx, col in enumerate(df_pandas):
                    series = df_pandas[col]
                    max_len = max((
                        series.astype(str).map(len).max(),
                        len(str(series.name))
                    )) + 2
                    if 'fecha' in str(series.name).lower():
                        worksheet.set_column(idx, idx, max_len, formato_fecha)
                    else:
                        worksheet.set_column(idx, idx, max_len)
                
                print(f"Hoja '{sheet_name}' escrita y formateada.")
            else:
                print(f"Hoja '{sheet_name}' omitida por estar vacía.")

    buffer.seek(0)
    return buffer