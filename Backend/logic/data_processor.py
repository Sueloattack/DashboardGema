import polars as pl
import pandas as pd
import io
from config import settings
from db.mySQL_connector import obtener_datos_glosas
import datetime

def obtener_y_limpiar_datos_base(fecha_inicio: str = None, fecha_fin: str = None) -> pl.DataFrame:
    """Carga, limpia y retorna los datos como un DataFrame de Polars."""
    print("Iniciando carga y limpieza de datos...")
    registros, error = obtener_datos_glosas(fecha_inicio, fecha_fin)
    if error: raise Exception(f"Error en capa de datos: {error}")
    if not registros:
        print("Advertencia: No se encontraron registros.")
        return pl.DataFrame()
    df = pl.DataFrame(registros)
    columnas_fecha = [c for c in [settings.COL_FECHA_NOTIFICACION, settings.COL_FECHA_OBJECION, settings.COL_FECHA_RADICADO, settings.COL_FECHA_CONTESTACION] if c in df.columns]
    df = df.with_columns(
        [pl.col(c).cast(pl.Datetime, strict=False) for c in columnas_fecha] + [
            pl.col(settings.COL_CARPETA_CC).cast(pl.Int64, strict=False).fill_null(0),
            pl.col(settings.COL_VR_GLOSA).cast(pl.Float64, strict=False).fill_null(0),
            pl.col("saldocartera").cast(pl.Float64, strict=False).fill_null(0),
        ]
    )
    return df.filter(pl.col(settings.COL_ESTATUS).is_in(settings.VALID_ESTATUS_VALUES))

def crear_tabla_resumen_detalle_polars(df_items: pl.DataFrame) -> pl.DataFrame:
    """Transforma un DataFrame de ítems en una estructura Resumen/Detalle (versión final robusta)."""
    if df_items.is_empty():
        return df_items
    
    # 1. Crear columnas auxiliares para facilitar los cálculos
    df_items_con_extras = df_items.with_columns(
        (pl.col(settings.COL_SERIE).cast(pl.Utf8).fill_null("") + 
         pl.col(settings.COL_N_FACTURA).cast(pl.Utf8).fill_null("")).alias("FACTURA"),
        (pl.col(settings.COL_CARPETA_CC) != 0).alias("_tiene_cc"),
        pl.col(settings.COL_FECHA_RADICADO).is_not_null().alias("_tiene_fr")
    )
    
    # 2. Agrupar y crear las filas de Resumen con tipado explícito
    df_resumen = df_items_con_extras.group_by(settings.GROUP_BY_FACTURA + ["FACTURA"]).agg(
        pl.first(settings.COL_ENTIDAD), 
        pl.first(settings.COL_FECHA_NOTIFICACION),
        pl.min(settings.COL_FECHA_OBJECION).alias(settings.COL_FECHA_OBJECION),
        pl.max(settings.COL_FECHA_CONTESTACION).alias(settings.COL_FECHA_CONTESTACION),
        
        # --- CORRECCIÓN CLAVE AQUÍ: Forzamos el tipo de dato de los conteos ---
        # El resultado de count() y sum() sobre booleanos se convierte explícitamente a UInt32
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

    # 3. Preparar las filas de Detalle (el código aquí ya era correcto)
    df_detalle = df_items_con_extras.with_columns(
        pl.lit("Detalle Ítem").alias("TipoFila"),
        pl.when(pl.col(settings.COL_CARPETA_CC) == 0).then(None).otherwise(pl.col(settings.COL_CARPETA_CC)).cast(pl.Int64),
        *[pl.lit(None, dtype=pl.UInt32).alias(c) for c in [
            "Total_Items_Factura", "Items_ConCC_ConFR", "Items_ConCC_SinFR", 
            "Items_SinCC_ConFR", "Items_SinCC_SinFR"
        ]]
    )
    
    # 4. Concatenar y Ordenar (Ahora no dará SchemaError)
    df_combinado = pl.concat([df_resumen, df_detalle], how="diagonal")
    
    df_ordenado = df_combinado.with_columns([
        pl.when(pl.col("TipoFila") == "Resumen Factura").then(0).otherwise(1).alias("orden_tipo"),
        pl.when(pl.col(settings.COL_ESTATUS) == 'C1').then(1).when(pl.col(settings.COL_ESTATUS) == 'C2').then(2).when(pl.col(settings.COL_ESTATUS) == 'C3').then(3).when(pl.col(settings.COL_ESTATUS) == 'CO').then(4).when(pl.col(settings.COL_ESTATUS) == 'AI').then(5).otherwise(99).alias("orden_estatus")
    ]).sort(
        settings.GROUP_BY_FACTURA + ["FACTURA", "orden_tipo", settings.COL_FECHA_OBJECION, "orden_estatus"]
    ).drop("orden_tipo", "orden_estatus", "_tiene_cc", "_tiene_fr")
    
    # 5. Selección final y orden de columnas
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

# ==============================================================================
#      INICIO DE LA FUNCIÓN COMPLETA Y FINAL
# ==============================================================================

def generar_y_comprobar_todas_las_tablas(fecha_inicio: str = None, fecha_fin: str = None) -> tuple:
    df_base = obtener_y_limpiar_datos_base(fecha_inicio, fecha_fin)

    # --- Manejo del caso de DataFrame vacío ---
    if df_base.is_empty():
        keys_resumen = [
            "total_facturas_base", "suma_categorizadas", "valor_total_periodo", 
            "valor_total_no_radicado", "valor_total_radicado", "facturas_t1", 
            "facturas_t2", "facturas_t3", "facturas_t4", "facturas_mixtas"
        ]
        empty_dfs = {
            "T1": pl.DataFrame(), "T2": pl.DataFrame(), "T3": pl.DataFrame(), 
            "T4": pl.DataFrame(), "Mixtas": pl.DataFrame(), "df_base": pl.DataFrame()
        }
        empty_resumen = {
            "error": "No hay datos...", 
            "comprobacion_exitosa": True, 
            **{k: 0 for k in keys_resumen}, 
            "conteo_por_entidad": [], 
            "conteo_por_estatus": [],
            # Añadir las nuevas claves vacías también
            "ingresos_por_periodo": [],
            "granularidad_ingresos": "ninguna"
        }
        return empty_dfs, empty_resumen

    # --- Clasificación de facturas (Lógica existente) ---
    df_base = df_base.with_columns(
        pl.concat_str([pl.col(c).cast(pl.Utf8).fill_null("") for c in settings.GROUP_BY_FACTURA], separator="-").alias("factura_id")
    )
    conds = {
        "T1": (pl.col(settings.COL_CARPETA_CC) != 0) & (pl.col(settings.COL_FECHA_RADICADO).is_not_null()),
        "T2": (pl.col(settings.COL_CARPETA_CC) != 0) & (pl.col(settings.COL_FECHA_RADICADO).is_null()), 
        "T3": (pl.col(settings.COL_CARPETA_CC) == 0) & (pl.col(settings.COL_FECHA_RADICADO).is_null()), 
        "T4": (pl.col(settings.COL_CARPETA_CC) == 0) & (pl.col(settings.COL_FECHA_RADICADO).is_not_null())
    }
    df_base = df_base.with_columns([(cond.sum().over("factura_id") == pl.count().over("factura_id")).alias(f"es_{tipo}") for tipo, cond in conds.items()])
    
    dfs, s_counts, ids_puras_total = {}, {}, []
    for tipo in conds:
        dfs[tipo] = df_base.filter(pl.col(f"es_{tipo}"))
        ids_puras = dfs[tipo].select("factura_id").unique()
        s_counts[f"facturas_{tipo.lower()}"] = len(ids_puras)
        if not ids_puras.is_empty(): ids_puras_total.append(ids_puras)
    
    ids_puras_df = pl.concat(ids_puras_total).unique() if ids_puras_total else pl.DataFrame({"factura_id": []})
    dfs["Mixtas"] = df_base.join(ids_puras_df, on="factura_id", how="anti")
    s_counts["facturas_mixtas"] = dfs["Mixtas"].select("factura_id").n_unique()

    # --- Agregados y Estadísticas (Lógica existente) ---
    df_saldos_unicos = df_base.select(["factura_id", "saldocartera"]).unique(subset="factura_id")
    s_counts["valor_total_periodo"] = df_saldos_unicos["saldocartera"].sum() or 0

    if not dfs["T1"].is_empty():
        s_counts["valor_total_radicado"] = dfs["T1"].select(["factura_id", "saldocartera"]).unique(subset="factura_id")["saldocartera"].sum() or 0
    else:
        s_counts["valor_total_radicado"] = 0

    ids_t1 = dfs.get("T1", pl.DataFrame()).select("factura_id").unique()
    df_no_radicadas = df_base.join(ids_t1, on="factura_id", how="anti")
    if not df_no_radicadas.is_empty():
        s_counts["valor_total_no_radicado"] = df_no_radicadas.select(["factura_id", "saldocartera"]).unique(subset="factura_id")["saldocartera"].sum() or 0
        s_counts["conteo_por_entidad"] = df_no_radicadas.group_by(settings.COL_ENTIDAD).agg(pl.n_unique("factura_id").alias("total_facturas")).sort("total_facturas", descending=True).limit(15).to_dicts()
        s_counts["conteo_por_estatus"] = df_no_radicadas.group_by(settings.COL_ESTATUS).agg(pl.count().alias("total_items")).sort("total_items", descending=True).to_dicts()
    else:
        s_counts["valor_total_no_radicado"] = 0
        s_counts["conteo_por_entidad"] = []
        s_counts["conteo_por_estatus"] = []

    s_counts["total_facturas_base"] = df_base.select("factura_id").n_unique()
    s_counts["suma_categorizadas"] = s_counts["facturas_t1"] + s_counts["facturas_t2"] + s_counts["facturas_t3"] + s_counts["facturas_t4"] + s_counts["facturas_mixtas"]
    s_counts["comprobacion_exitosa"] = s_counts["total_facturas_base"] == s_counts["suma_categorizadas"]
    
    
    # --- LÓGICA REVISADA Y CORREGIDA PARA EL GRÁFICO DE INGRESOS ---
    # 1. Preparar rango y granularidad
    formato_fecha = "%Y-%m-%d"
    d_inicio = datetime.datetime.strptime(fecha_inicio, formato_fecha)
    d_fin = datetime.datetime.strptime(fecha_fin, formato_fecha)
    diferencia_dias = (d_fin - d_inicio).days
    
    if diferencia_dias <= 90:
        granularidad_agg = "1d"
        granularidad_label = 'Diaria'
    elif 90 < diferencia_dias <= 730:
        granularidad_agg = "1mo" # 'mo' es el código para mes
        granularidad_label = 'Mensual'
    else:
        granularidad_agg = "1y" # 'y' es el código para año
        granularidad_label = 'Anual'

    # 2. Agrupar los datos reales utilizando una fecha truncada
    df_facturas_unicas = df_base.unique(subset=["factura_id"], keep="first")
    
    # Creamos una columna nueva con la fecha truncada al inicio de su período
    df_con_fecha_truncada = df_facturas_unicas.with_columns(
        pl.col(settings.COL_FECHA_NOTIFICACION).dt.truncate(granularidad_agg).alias("fecha_truncada")
    )
    
    # Agrupamos por esta nueva columna truncada
    ingresos_reales_agrupados = (
        df_con_fecha_truncada
        .group_by("fecha_truncada")
        .agg(pl.n_unique("factura_id").alias("conteo"))
        .rename({"fecha_truncada": "fecha"}) # Renombramos para el join
        .with_columns(pl.col("fecha").cast(pl.Date)) # Nos aseguramos que sea tipo Date
        .sort("fecha") # Ordenar por fecha es buena práctica aquí
    )

    # 3. Crear el rango completo de fechas, también truncado
    rango_completo_generado = pl.date_range(
        start=d_inicio,
        end=d_fin,
        interval=granularidad_agg,
        eager=True
    ).alias("fecha")
    
    df_rango_completo = pl.DataFrame(rango_completo_generado).with_columns(
        pl.col("fecha").dt.truncate(granularidad_agg)
    ).unique() # Usamos .unique() para asegurar tener solo los puntos de inicio de período

    # 4. Unir el rango completo con los datos reales
    df_final_grafico = df_rango_completo.join(
        ingresos_reales_agrupados, 
        on="fecha", 
        how="left"
    )

    # 5. Rellenar nulos con 0 y preparar para el frontend
    ingresos_para_frontend = (
        df_final_grafico
        .with_columns(
            pl.col("conteo").fill_null(0).cast(pl.Int32),
            # Convertimos la fecha (tipo Date) a string en formato ISO para ApexCharts
            pl.col("fecha").dt.strftime("%Y-%m-%dT00:00:00").alias("fecha_agrupada")
        )
        .select(["fecha_agrupada", "conteo"])
        .sort("fecha_agrupada")
    )

    # 6. Añadir al diccionario de estadísticas final
    s_counts["ingresos_por_periodo"] = ingresos_para_frontend.to_dicts()
    s_counts["granularidad_ingresos"] = granularidad_label
    
    # --- FIN DE LA LÓGICA DEL NUEVO GRÁFICO ---


    # --- Devolver los resultados ---
    dfs["df_base"] = df_base
    return dfs, s_counts

# ==============================================================================
#      FIN DE LA FUNCIÓN COMPLETA Y FINAL
# ==============================================================================

def obtener_resumenes_paginados(fecha_inicio: str, fecha_fin: str, categorias: list, pagina: int, por_pagina: int):
    print(f"Obteniendo Resúmenes Paginados: Categorías={categorias}, Página={pagina}")
    
    dataframes_clasificados, _ = generar_y_comprobar_todas_las_tablas(fecha_inicio, fecha_fin)
    
    # --- CORRECCIÓN CLAVE: Ahora el KeyError no ocurrirá ---
    df_base_original = dataframes_clasificados.get("df_base", pl.DataFrame())
    if df_base_original.is_empty():
        return {"data": [], "pagina_actual": 1, "total_paginas": 0, "total_registros": 0}
        
    ids_a_mostrar_list = [dataframes_clasificados[cat].select("factura_id") for cat in categorias if cat in dataframes_clasificados and not dataframes_clasificados[cat].is_empty()]
    
    if not ids_a_mostrar_list:
        return {"data": [], "pagina_actual": 1, "total_paginas": 0, "total_registros": 0}
        
    df_ids_a_mostrar = pl.concat(ids_a_mostrar_list).unique()
    
    df_items_completos = df_base_original.join(df_ids_a_mostrar, on="factura_id", how="inner")
    
    if df_items_completos.is_empty():
        return {"data": [], "pagina_actual": 1, "total_paginas": 0, "total_registros": 0}
        
    df_resumenes = crear_tabla_resumen_detalle_polars(df_items_completos).filter(pl.col("TipoFila") == "Resumen Factura")
    
    total_registros = len(df_resumenes)
    total_paginas = (total_registros + por_pagina - 1) // por_pagina if por_pagina > 0 else 1
    offset = (pagina - 1) * por_pagina
    df_pagina = df_resumenes.slice(offset, por_pagina)
    
    datos_dict = df_pagina.with_columns(pl.col(pl.Datetime).dt.strftime("%Y-%m-%dT%H:%M:%S")).to_dicts()

    return {"data": datos_dict, "pagina_actual": pagina, "total_paginas": total_paginas, "total_registros": total_registros}

# --- obtener_detalle_especifico_factura (CORREGIDA) ---
def obtener_detalle_especifico_factura(docn):
    print(f"Obteniendo detalle para gl_docn: {docn}")

    # Es mucho más simple y consistente si reutilizamos el código principal
    # con un filtro aplicado. Traemos TODOS los datos porque no sabemos el rango de fechas
    # del gl_docn específico.
    df_base_completa = obtener_y_limpiar_datos_base(None, None)
    
    # Filtramos para obtener solo los ítems de esta factura (gl_docn)
    df_items_factura = df_base_completa.filter(pl.col(settings.COL_GL_DOCN) == docn)
    if df_items_factura.is_empty(): return []

    # Generamos la vista Resumen/Detalle COMPLETA para esta factura y devolvemos solo el detalle
    df_detalle_final = crear_tabla_resumen_detalle_polars(df_items_factura).filter(pl.col("TipoFila") == "Detalle Ítem")
    
    return df_detalle_final.to_dicts()

# En logic/data_processor.py

def generar_excel_en_memoria(dataframes: dict) -> io.BytesIO:
    buffer = io.BytesIO()

    # === INICIO DE LA SOLUCIÓN ===
    # Creamos una copia del diccionario para no modificar el original.
    # Luego, usamos .pop() para eliminar la clave "df_base" antes de procesar.
    # El segundo argumento, None, evita un error si la clave no existiera.
    sheets_to_process = dataframes.copy()
    sheets_to_process.pop("df_base", None)
    # === FIN DE LA SOLUCIÓN ===

    # Mapeo de nombres de hojas
    nombres_hojas = {
        "T1": "Radicadas",
        "T2": "Con CC y Sin FR",
        "T3": "Sin CC y Sin FR",
        "T4": "Sin CC y Con FR",
        "Mixtas": "Mixtas", # Añadido por si se usaba el nombre 'Mixtas' directamente
        # Los nombres que ya usabas como "Fallas_Punteo", etc., se mantendrán
        # si los devuelves en el diccionario de dataframes.
    }

    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        workbook = writer.book

        # Formatos personalizados
        formato_fecha = workbook.add_format({'num_format': 'dd/mm/yyyy'})
        formato_moneda = workbook.add_format({'num_format': '$#,##0'})

        # Ahora iteramos sobre nuestro diccionario filtrado 'sheets_to_process'
        for key_df, df_items_polars in sheets_to_process.items():
            # El resto de la función permanece exactamente igual
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
                        elif settings.COLUMN_NAME_MAPPING_EXPORT.get(settings.COL_VR_GLOSA, '').lower() == df_pandas.columns[col_num].lower():
                             try:
                                worksheet.write_number(row_num, col_num, float(value), formato_moneda)
                             except (ValueError, TypeError):
                                worksheet.write(row_num, col_num, value)
                        else:
                            worksheet.write(row_num, col_num, value)

                print(f"Hoja '{sheet_name}' escrita y formateada.")
            else:
                print(f"Hoja '{sheet_name}' omitida por estar vacía.")

    buffer.seek(0)
    return buffer