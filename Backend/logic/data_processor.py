# logic/data_processor.py
"""
Módulo de Lógica de Negocio (El Cerebro de la Aplicación).
Optimizado para rendimiento con Caching y Lazy API de Polars.
"""

# --- Importaciones ---
import polars as pl
import pandas as pd
import io
import datetime

# Módulos locales
from config import settings
from db.mySQL_connector import obtener_datos_glosas
from extensions import cache  # Importar la instancia de cache desde extensions.py

# ==============================================================================
# SECCIÓN: OBTENCIÓN Y CACHEO DE DATOS
# ==============================================================================

@cache.memoize(timeout=600) # Cachea por 10 minutos
def _obtener_y_limpiar_datos_base_cache(fecha_inicio: str = None, fecha_fin: str = None) -> pl.DataFrame:
    """
    Función interna y cacheada. Su único trabajo es hacer la consulta
    pesada a la BD, realizar la limpieza básica y devolver un DataFrame de Polars.
    """
    print(f"¡SIN CACHÉ! Accediendo a la BD para el rango {fecha_inicio} a {fecha_fin}")
    
    # 1. Obtener datos crudos
    registros, error = obtener_datos_glosas(fecha_inicio, fecha_fin)
    if error:
        raise Exception(f"Error en capa de datos al obtener glosas: {error}")
    
    if not registros:
        print("Advertencia: La consulta a la base de datos no devolvió registros.")
        return pl.DataFrame()

    # 2. Convertir a DataFrame de Polars y limpiar
    df = pl.DataFrame(registros)
    
    columnas_fecha = [
        c for c in [
            settings.COL_FECHA_NOTIFICACION, 
            settings.COL_FECHA_OBJECION, 
            settings.COL_FECHA_RADICADO, 
            settings.COL_FECHA_CONTESTACION
        ] if c in df.columns
    ]
    
    df = df.with_columns(
        [pl.col(c).cast(pl.Datetime, strict=False) for c in columnas_fecha] +
        [
            pl.col(settings.COL_CARPETA_CC).cast(pl.Int64, strict=False).fill_null(0),
            pl.col(settings.COL_VR_GLOSA).cast(pl.Float64, strict=False).fill_null(0),
            pl.col("saldocartera").cast(pl.Float64, strict=False).fill_null(0),
        ]
    )
    
    return df.filter(pl.col(settings.COL_ESTATUS).is_in(settings.VALID_ESTATUS_VALUES))

# ==============================================================================
# SECCIÓN: LÓGICA DE PROCESAMIENTO PRINCIPAL (CON LAZY API)
# ==============================================================================

def generar_y_comprobar_todas_las_tablas(fecha_inicio: str = None, fecha_fin: str = None) -> tuple:
    """
    Función orquestadora principal que realiza el análisis completo para el dashboard.
    Utiliza la Lazy API de Polars para un rendimiento óptimo.
    """
    # 1. Obtener datos (posiblemente de la caché)
    df_base = _obtener_y_limpiar_datos_base_cache(fecha_inicio, fecha_fin)

    if df_base.is_empty():
        # Devuelve una estructura vacía consistente
        return {}, {"error": "No hay datos en el rango de fechas seleccionado."}

    # 2. Iniciar el Plan de Ejecución Lazy
    lazy_df = df_base.lazy()

    # 3. Construir el plan de transformaciones
    lazy_clasificado = lazy_df.with_columns(
        pl.concat_str(
            [pl.col(c).cast(pl.Utf8).fill_null("") for c in settings.GROUP_BY_FACTURA], 
            separator="-"
        ).alias("factura_id")
    ).with_columns(
        # Usar pl.Categorical mejora el rendimiento de las agrupaciones
        pl.col(settings.COL_ESTATUS).cast(pl.Categorical)
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

    # 4. Ejecutar el plan (collect)
    df_final = plan_final.collect()
    
    # 5. Generar DataFrames y Estadísticas a partir del resultado materializado
    dfs, s_counts, ids_puras_total = {}, {}, []
    for tipo in conds:
        dfs[tipo] = df_final.filter(pl.col(f"es_{tipo}"))
        ids_puras = dfs[tipo].select("factura_id").unique()
        s_counts[f"facturas_{tipo.lower()}"] = len(ids_puras)
        if not ids_puras.is_empty(): 
            ids_puras_total.append(ids_puras)

    ids_puras_df = pl.concat(ids_puras_total).unique() if ids_puras_total else pl.DataFrame({"factura_id": []})
    dfs["Mixtas"] = df_final.join(ids_puras_df, on="factura_id", how="anti")
    s_counts["facturas_mixtas"] = dfs["Mixtas"]["factura_id"].n_unique()

    # ... (El resto de la lógica de agregación y KPIs se mantiene similar, pero opera sobre df_final)
    # Esta parte ya es rápida porque opera en memoria.
    
    # --- Agregados y Estadísticas (KPIs para el Dashboard) ---
    df_saldos_unicos = df_final.select(["factura_id", "saldocartera"]).unique(subset="factura_id")
    s_counts["valor_total_periodo"] = df_saldos_unicos["saldocartera"].sum() or 0

    s_counts["valor_total_radicado"] = 0
    if not dfs["T1"].is_empty():
        s_counts["valor_total_radicado"] = dfs["T1"].select(["factura_id", "saldocartera"]).unique(subset="factura_id")["saldocartera"].sum() or 0

    ids_t1 = dfs.get("T1", pl.DataFrame()).select("factura_id").unique()
    df_no_radicadas = df_final.join(ids_t1, on="factura_id", how="anti")

    s_counts["valor_total_no_radicado"] = 0
    s_counts["conteo_por_entidad"] = []
    s_counts["conteo_por_estatus"] = []
    if not df_no_radicadas.is_empty():
        s_counts["valor_total_no_radicado"] = df_no_radicadas.select(["factura_id", "saldocartera"]).unique(subset="factura_id")["saldocartera"].sum() or 0
        s_counts["conteo_por_entidad"] = df_no_radicadas.group_by(settings.COL_ENTIDAD).agg(pl.n_unique("factura_id").alias("total_facturas")).sort("total_facturas", descending=True).limit(15).to_dicts()
    
    df_conteo_estatus = df_no_radicadas.group_by(settings.COL_ESTATUS).agg(pl.count().alias("total_items"))
    
    s_counts["conteo_por_estatus"] = df_conteo_estatus.sort(by=pl.col(settings.COL_ESTATUS)).to_dicts()

    # --- Comprobación de integridad ---
    s_counts["total_facturas_base"] = df_final["factura_id"].n_unique()
    s_counts["suma_categorizadas"] = sum(s_counts.get(f"facturas_{t.lower()}", 0) for t in list(conds.keys()) + ["mixtas"])
    s_counts["comprobacion_exitosa"] = s_counts["total_facturas_base"] == s_counts["suma_categorizadas"]

    # --- Lógica para el Gráfico de Ingresos por Periodo ---
    df_ingresos = df_final.select(["factura_id", settings.COL_FECHA_NOTIFICACION]).unique(subset="factura_id").sort(settings.COL_FECHA_NOTIFICACION)
    
    if df_ingresos.is_empty() or df_ingresos[settings.COL_FECHA_NOTIFICACION].is_null().all():
        s_counts["ingresos_por_periodo"] = []
        s_counts["granularidad_ingresos"] = "Diario"
    else:
        # Decidir la granularidad (diaria, mensual, anual) basado en el rango de fechas
        min_date = df_ingresos[settings.COL_FECHA_NOTIFICACION].min()
        max_date = df_ingresos[settings.COL_FECHA_NOTIFICACION].max()
        
        if min_date is None or max_date is None:
            s_counts["ingresos_por_periodo"] = []
            s_counts["granularidad_ingresos"] = "Diario"
        else:
            dias_rango = (max_date - min_date).days
            if dias_rango > 365 * 2:
                granularidad = "year"
                df_agrupado = df_ingresos.group_by_dynamic(settings.COL_FECHA_NOTIFICACION, every="1y").agg(pl.count().alias("conteo"))
            elif dias_rango > 90:
                granularidad = "month"
                df_agrupado = df_ingresos.group_by_dynamic(settings.COL_FECHA_NOTIFICACION, every="1mo").agg(pl.count().alias("conteo"))
            else:
                granularidad = "day"
                df_agrupado = df_ingresos.group_by_dynamic(settings.COL_FECHA_NOTIFICACION, every="1d").agg(pl.count().alias("conteo"))

            s_counts["ingresos_por_periodo"] = df_agrupado.rename({settings.COL_FECHA_NOTIFICACION: "fecha_agrupada"}).to_dicts()
            s_counts["granularidad_ingresos"] = granularidad.capitalize()

    # --- Devolver los resultados ---
    dfs["df_base"] = df_final
    return dfs, s_counts


def crear_tabla_resumen_detalle_polars(df_items: pl.DataFrame) -> pl.DataFrame:
    # ... (código original sin cambios)
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
        (pl.when(pl.col("_tiene_cc") & pl.col("_tiene_fr")).then(1).otherwise(0)).sum().cast(pl.UInt32).alias("Items_ConCC_ConFR"),
        (pl.when(pl.col("_tiene_cc") & ~pl.col("_tiene_fr")).then(1).otherwise(0)).sum().cast(pl.UInt32).alias("Items_ConCC_SinFR"),
        (pl.when(~pl.col("_tiene_cc") & pl.col("_tiene_fr")).then(1).otherwise(0)).sum().cast(pl.UInt32).alias("Items_SinCC_ConFR"),
        (pl.when(~pl.col("_tiene_cc") & ~pl.col("_tiene_fr")).then(1).otherwise(0)).sum().cast(pl.UInt32).alias("Items_SinCC_SinFR"),
        pl.first("saldocartera").alias(settings.COL_VR_GLOSA),
        pl.first(settings.COL_TIPO).alias(settings.COL_TIPO)
    ).with_columns(
        pl.lit("Resumen Factura").alias("TipoFila"),
        pl.lit(None, dtype=pl.Int64).alias(settings.COL_CARPETA_CC),
        pl.lit(None, dtype=pl.Datetime).alias(settings.COL_FECHA_RADICADO),
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

    # Asegurarse de que la columna de estatus en df_resumen también sea Utf8 antes de concatenar
    df_resumen = df_resumen.with_columns(pl.col(settings.COL_ESTATUS).cast(pl.Utf8))

    df_combinado = pl.concat([df_resumen, df_detalle], how="diagonal")
    df_ordenado = df_combinado.with_columns([
        pl.when(pl.col("TipoFila") == "Resumen Factura").then(0).otherwise(1).alias("orden_tipo"),
        pl.when(pl.col(settings.COL_ESTATUS) == 'C1').then(1)
          .when(pl.col(settings.COL_ESTATUS) == 'C2').then(2)
          .when(pl.col(settings.COL_ESTATUS) == 'C3').then(3)
          .when(pl.col(settings.COL_ESTATUS) == 'CO').then(4)
          .when(pl.col(settings.COL_ESTATUS) == 'AI').then(5)
          .otherwise(99).alias("orden_estatus")
    ]).sort(
        settings.GROUP_BY_FACTURA + ["FACTURA", "orden_tipo", settings.COL_FECHA_OBJECION, "orden_estatus"]
    ).drop("orden_tipo", "orden_estatus", "_tiene_cc", "_tiene_fr")
    orden_final_columnas = [
        settings.COL_FECHA_NOTIFICACION, "FACTURA", settings.COL_GL_DOCN,
        settings.COL_ENTIDAD, settings.COL_FECHA_OBJECION, settings.COL_FECHA_CONTESTACION,
        settings.COL_CARPETA_CC, settings.COL_FECHA_RADICADO, settings.COL_ESTATUS,
        settings.COL_VR_GLOSA, settings.COL_TIPO, "TipoFila", "Total_Items_Factura",
        "Items_ConCC_ConFR", "Items_ConCC_SinFR", "Items_SinCC_ConFR", "Items_SinCC_SinFR"
    ]
    for col in orden_final_columnas:
        if col not in df_ordenado.columns:
            df_ordenado = df_ordenado.with_columns(pl.lit(None).alias(col))
    return df_ordenado.select(orden_final_columnas)

def obtener_resumenes_paginados(fecha_inicio: str, fecha_fin: str, categorias: list, pagina: int, por_pagina: int, entidad: str = None):
    print(f"Obteniendo Resúmenes: Categorías={categorias}, Página={pagina}, Entidad={entidad}")
    dataframes_clasificados, _ = generar_y_comprobar_todas_las_tablas(fecha_inicio, fecha_fin)
    df_base_original = dataframes_clasificados.get("df_base", pl.DataFrame())
    if df_base_original.is_empty():
        return {"data": [], "pagina_actual": 1, "total_paginas": 0, "total_registros": 0}
    ids_a_mostrar_list = [
        dataframes_clasificados[cat].select("factura_id") 
        for cat in categorias 
        if cat in dataframes_clasificados and not dataframes_clasificados[cat].is_empty()
    ]
    if not ids_a_mostrar_list:
        return {"data": [], "pagina_actual": 1, "total_paginas": 0, "total_registros": 0}
    df_ids_a_mostrar = pl.concat(ids_a_mostrar_list).unique()
    df_items_completos = df_base_original.join(df_ids_a_mostrar, on="factura_id", how="inner")
    if df_items_completos.is_empty():
        return {"data": [], "pagina_actual": 1, "total_paginas": 0, "total_registros": 0}
    print(f"Generando resúmenes para {df_items_completos.select('factura_id').n_unique()} facturas únicas...")
    df_reporte_completo = crear_tabla_resumen_detalle_polars(df_items_completos)
    df_resumenes = df_reporte_completo.filter(pl.col("TipoFila") == "Resumen Factura")
    if entidad:
        print(f"Filtrando {len(df_resumenes)} resúmenes por entidad: '{entidad}'")
        df_resumenes = df_resumenes.filter(
            pl.col(settings.COL_ENTIDAD) == entidad
        )
        print(f"Resúmenes restantes después del filtro: {len(df_resumenes)}")
    total_registros = len(df_resumenes)
    total_paginas = (total_registros + por_pagina - 1) // por_pagina if por_pagina > 0 else 1
    offset = (pagina - 1) * por_pagina
    df_pagina = df_resumenes.slice(offset, por_pagina)
    datos_dict = df_pagina.with_columns(pl.col(pl.Datetime).dt.strftime("%Y-%m-%dT%H:%M:%S")).to_dicts()
    return {"data": datos_dict, "pagina_actual": pagina, "total_paginas": total_paginas, "total_registros": total_registros}

def obtener_detalle_especifico_factura(docn: int) -> list:
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
    buffer = io.BytesIO()
    nombres_hojas = {
        "T1": "Radicadas",
        "T2": "Con CC y Sin FR",
        "T3": "Sin CC y Sin FR",
        "T4": "Sin CC y Con FR",
        "Mixtas": "Mixtas",
    }
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        workbook = writer.book
        formato_fecha = workbook.add_format({'num_format': 'dd/mm/yyyy'})
        formato_moneda = workbook.add_format({'num_format': '$#,##0'})
        sheets_to_process = {k: v for k, v in dataframes.items() if k != "df_base"}
        for key_df, df_items_polars in sheets_to_process.items():
            sheet_name = nombres_hojas.get(key_df, key_df)
            print(f"Procesando hoja '{sheet_name}' para Excel...")
            if not df_items_polars.is_empty():
                df_reporte = crear_tabla_resumen_detalle_polars(df_items_polars)
                mapping_local = {**settings.COLUMN_NAME_MAPPING_EXPORT, "FACTURA": "Factura"}
                df_pandas = df_reporte.to_pandas(use_pyarrow_extension_array=True)
                df_pandas.rename(columns={k: v for k, v in mapping_local.items() if k in df_pandas.columns}, inplace=True)
                for col in df_pandas.columns:
                    if 'fecha' in col.lower():
                        df_pandas[col] = pd.to_datetime(df_pandas[col], errors='coerce')
                worksheet = workbook.add_worksheet(sheet_name)
                writer.sheets[sheet_name] = worksheet
                for col_num, column_title in enumerate(df_pandas.columns):
                    worksheet.write(0, col_num, column_title)
                for row_num, row in enumerate(df_pandas.itertuples(index=False), start=1):
                    for col_num, value in enumerate(row):
                        col_name = df_pandas.columns[col_num].lower()
                        if pd.isna(value):
                            worksheet.write(row_num, col_num, None)
                        elif isinstance(value, pd.Timestamp):
                            worksheet.write_datetime(row_num, col_num, value, formato_fecha)
                        elif 'fecha' in col_name:
                            try:
                                dt = pd.to_datetime(value)
                                if not pd.isna(dt):
                                    worksheet.write_datetime(row_num, col_num, dt, formato_fecha)
                                else:
                                    worksheet.write(row_num, col_num, None)
                            except:
                                worksheet.write(row_num, col_num, str(value))
                        elif mapping_local.get(settings.COL_VR_GLOSA, '').lower() == df_pandas.columns[col_num].lower():
                            try:
                                worksheet.write_number(row_num, col_num, float(value), formato_moneda)
                            except:
                                worksheet.write(row_num, col_num, value)
                        else:
                            worksheet.write(row_num, col_num, value)
                print(f"Hoja '{sheet_name}' escrita y formateada.")
            else:
                print(f"Hoja '{sheet_name}' omitida por estar vacía.")
    buffer.seek(0)
    return buffer
