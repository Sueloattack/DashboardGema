# logic/data_processor.py
import polars as pl
import pandas as pd
import io
from config import settings
from db.mySQL_connector import obtener_datos_glosas

# --- FUNCIONES AUXILIARES DE BASE (Completas) ---

def obtener_y_limpiar_datos_base() -> pl.DataFrame:
    """Función central que carga datos, los limpia y los devuelve listos para el análisis."""
    print("Iniciando carga de datos desde MySQL...")
    registros, error = obtener_datos_glosas(settings.COLUMNS_FROM_DB)
    
    if error:
        raise Exception(f"Error en la capa de datos: {error}")
    if not registros:
        print("Advertencia: No se encontraron registros.")
        return pl.DataFrame(schema={col: pl.Utf8 for col in settings.COLUMNS_FROM_DB})

    df_original = pl.DataFrame(registros)
    
    df_limpio = df_original.with_columns([
        pl.col([settings.COL_FECHA_OBJECION, settings.COL_FECHA_RADICADO, settings.COL_FECHA_CONTESTACION]).cast(pl.Datetime, strict=False),
        pl.col(settings.COL_CARPETA_CC).cast(pl.Int64, strict=False).fill_null(0),
        pl.col(settings.COL_VR_GLOSA).cast(pl.Float64, strict=False).fill_null(0)
    ])
    
    df_base_valido = df_limpio.filter(
        pl.col(settings.COL_ESTATUS).is_in(settings.VALID_ESTATUS_VALUES)
    )
    
    print(f"Datos base listos. {len(df_base_valido)} ítems con estatus válidos.")
    return df_base_valido

def obtener_facturas_completas_por_condicion(df_base: pl.DataFrame, condicion: pl.Expr, columnas_grupo_factura: list[str]) -> pl.DataFrame:
    """Identifica y devuelve los ítems de facturas donde TODOS sus ítems cumplen una condición."""
    if df_base.is_empty():
        return df_base
        
    df_facturas_completas = df_base.filter(
        (condicion.sum().over(columnas_grupo_factura)) == (pl.count().over(columnas_grupo_factura))
    )
    return df_facturas_completas

def crear_tabla_resumen_detalle_polars(df_items: pl.DataFrame, columnas_grupo_factura: list[str]) -> pl.DataFrame:
    """Transforma un DataFrame de ítems en una estructura Resumen/Detalle (versión muy ajustada)."""
    if df_items.is_empty(): return df_items
    
    df_items_con_factura = df_items.with_columns(
        (pl.col(settings.COL_SERIE).cast(pl.Utf8).fill_null("") + 
         pl.col(settings.COL_N_FACTURA).cast(pl.Utf8).fill_null("")).alias("FACTURA")
    )
    
    cond_con_cc_con_fr = (pl.col(settings.COL_CARPETA_CC) != 0) & (pl.col(settings.COL_FECHA_RADICADO).is_not_null())
    cond_con_cc_sin_fr = (pl.col(settings.COL_CARPETA_CC) != 0) & (pl.col(settings.COL_FECHA_RADICADO).is_null())
    cond_sin_cc_con_fr = (pl.col(settings.COL_CARPETA_CC) == 0) & (pl.col(settings.COL_FECHA_RADICADO).is_not_null())
    cond_sin_cc_sin_fr = (pl.col(settings.COL_CARPETA_CC) == 0) & (pl.col(settings.COL_FECHA_RADICADO).is_null())

    df_resumen = df_items_con_factura.group_by(columnas_grupo_factura + ["FACTURA"]).agg([
        pl.count().alias("Total_Items_Factura"),
        cond_con_cc_con_fr.sum().alias("Items_ConCC_ConFR"),
        cond_con_cc_sin_fr.sum().alias("Items_ConCC_SinFR"),
        cond_sin_cc_con_fr.sum().alias("Items_SinCC_ConFR"),
        cond_sin_cc_sin_fr.sum().alias("Items_SinCC_SinFR"),
        pl.first(settings.COL_FECHA_OBJECION),
        pl.first(settings.COL_FECHA_CONTESTACION),
        pl.first(settings.COL_ENTIDAD)
    ]).with_columns([
        pl.lit("Resumen Factura").alias("TipoFila"),
        pl.lit(None, dtype=pl.Float64).alias(settings.COL_VR_GLOSA), # Corregido a Float64 para coincidir
        pl.lit(None, dtype=pl.Int64).alias(settings.COL_CARPETA_CC)
    ])
    
    df_detalle = df_items_con_factura.with_columns([
        pl.lit("Detalle Ítem").alias("TipoFila"),
        pl.when(pl.col(settings.COL_CARPETA_CC) == 0)
          .then(pl.lit(None))
          .otherwise(pl.col(settings.COL_CARPETA_CC))
          .cast(pl.Int64)
          .alias(settings.COL_CARPETA_CC)
    ])
    
    df_combinado = pl.concat([df_resumen, df_detalle], how="diagonal")
    
    df_final_ordenado = df_combinado.with_columns([
        pl.when(pl.col("TipoFila") == "Resumen Factura").then(pl.lit(0)).otherwise(pl.lit(1)).alias("OrdenTipoFila"),
        pl.when(pl.col(settings.COL_ESTATUS) == 'C1').then(pl.lit(1)).when(pl.col(settings.COL_ESTATUS) == 'C2').then(pl.lit(2)).when(pl.col(settings.COL_ESTATUS) == 'C3').then(pl.lit(3)).when(pl.col(settings.COL_ESTATUS) == 'CO').then(pl.lit(4)).when(pl.col(settings.COL_ESTATUS) == 'AI').then(pl.lit(5)).otherwise(pl.lit(99)).alias("OrdenEstatus")
    ]).sort(
        columnas_grupo_factura + ["FACTURA", "OrdenTipoFila", settings.COL_FECHA_OBJECION, "OrdenEstatus"]
    ).drop("OrdenTipoFila", "OrdenEstatus")
    
    orden_columnas_final = [
        settings.COL_SERIE, settings.COL_N_FACTURA, "FACTURA", settings.COL_GL_DOCN, settings.COL_ENTIDAD,
        settings.COL_FECHA_OBJECION, settings.COL_FECHA_CONTESTACION, settings.COL_CARPETA_CC, settings.COL_FECHA_RADICADO,
        settings.COL_ESTATUS, settings.COL_VR_GLOSA, settings.COL_TIPO, "Total_Items_Factura", 
        "Items_ConCC_ConFR", "Items_ConCC_SinFR", "Items_SinCC_ConFR", "Items_SinCC_SinFR", "TipoFila"
    ]
    for col_name in orden_columnas_final:
        if col_name not in df_final_ordenado.columns: df_final_ordenado = df_final_ordenado.with_columns(pl.lit(None).alias(col_name))
    return df_final_ordenado.select(orden_columnas_final)

# --- NUEVAS FUNCIONES AUXILIARES PARA LA JERARQUÍA ---
def _detectar_inconsistencias(df_base: pl.DataFrame) -> pl.DataFrame:
    """Detecta facturas con datos inconsistentes y devuelve sus IDs."""
    print("-> Nivel 1: Buscando inconsistencias...")
    df_relevante = df_base.filter(pl.col(settings.COL_ESTATUS).str.starts_with('C'))
    
    df_conteo_unicos = df_relevante.group_by(settings.GROUP_BY_FACTURA + [settings.COL_ESTATUS]).agg(
        pl.n_unique(settings.COL_CARPETA_CC).alias("CC_Unicos"),
        pl.n_unique(settings.COL_FECHA_RADICADO).alias("FR_Unicos")
    )
    
    df_ids_inconsistentes = df_conteo_unicos.filter(
        (pl.col("CC_Unicos") > 1) | (pl.col("FR_Unicos") > 1)
    ).select(settings.GROUP_BY_FACTURA).unique()
    
    print(f"    - Encontradas {len(df_ids_inconsistentes)} facturas con inconsistencias.")
    return df_ids_inconsistentes

def _detectar_facturas_puras(df_base: pl.DataFrame) -> dict:
    """Detecta facturas 100% en una categoría y devuelve un diccionario con sus IDs."""
    print("-> Nivel 2: Buscando facturas puras...")
    condiciones = {
        "T1": (pl.col(settings.COL_CARPETA_CC) != 0) & (pl.col(settings.COL_FECHA_RADICADO).is_not_null()),
        "T2": (pl.col(settings.COL_CARPETA_CC) != 0) & (pl.col(settings.COL_FECHA_RADICADO).is_null()),
        "T3": (pl.col(settings.COL_CARPETA_CC) == 0) & (pl.col(settings.COL_FECHA_RADICADO).is_null()),
        "T4": (pl.col(settings.COL_CARPETA_CC) == 0) & (pl.col(settings.COL_FECHA_RADICADO).is_not_null()),
    }
    
    facturas_puras_ids = {}
    for nombre, cond in condiciones.items():
        df_items_puros = obtener_facturas_completas_por_condicion(df_base, cond, settings.GROUP_BY_FACTURA)
        facturas_puras_ids[nombre] = df_items_puros.select(settings.GROUP_BY_FACTURA).unique()
        print(f"    - Encontradas {len(facturas_puras_ids[nombre])} facturas puras de tipo {nombre}.")
        
    return facturas_puras_ids

def _separar_fallas_y_mixtas(df_base_mixtas: pl.DataFrame, umbral: float = 0.85) -> tuple:
    """Analiza facturas mixtas y las separa en Fallas de Punteo y Mixtas Reales."""
    print("-> Nivel 3 y 4: Separando Fallas de Punteo de Mixtas Reales...")
    if df_base_mixtas.is_empty():
        print("    - No hay facturas mixtas que analizar.")
        schema_ids = {col: pl.Utf8 for col in settings.GROUP_BY_FACTURA} # Cambiado a pl.Utf8 que es más genérico
        return pl.DataFrame(schema=schema_ids), pl.DataFrame(schema=schema_ids)
        
    # --- CORRECCIÓN CLAVE AQUÍ: Reescribir el .agg() para ser más explícito ---
    
    # Definimos las condiciones por separado para legibilidad
    cond_t1 = (pl.col(settings.COL_CARPETA_CC) != 0) & (pl.col(settings.COL_FECHA_RADICADO).is_not_null())
    cond_t2 = (pl.col(settings.COL_CARPETA_CC) != 0) & (pl.col(settings.COL_FECHA_RADICADO).is_null())
    cond_t3 = (pl.col(settings.COL_CARPETA_CC) == 0) & (pl.col(settings.COL_FECHA_RADICADO).is_null())
    cond_t4 = (pl.col(settings.COL_CARPETA_CC) == 0) & (pl.col(settings.COL_FECHA_RADICADO).is_not_null())

    df_analisis_mixtas = df_base_mixtas.group_by(settings.GROUP_BY_FACTURA).agg(
        pl.count().alias("Total_Items"),
        cond_t1.sum().alias("T1_count"),
        cond_t2.sum().alias("T2_count"),
        cond_t3.sum().alias("T3_count"),
        cond_t4.sum().alias("T4_count"),
    )
    
    df_con_dominancia = df_analisis_mixtas.with_columns(
        # Usamos los nombres de columna ya creados
        pl.max_horizontal(["T1_count", "T2_count", "T3_count", "T4_count"]).alias("Max_Count_Categoria")
    ).with_columns(
        (pl.col("Max_Count_Categoria") / pl.col("Total_Items")).alias("Porcentaje_Dominancia")
    )
    
    df_fallas_punteo_ids = df_con_dominancia.filter(pl.col("Porcentaje_Dominancia") >= umbral).select(settings.GROUP_BY_FACTURA)
    df_mixtas_reales_ids = df_con_dominancia.filter(pl.col("Porcentaje_Dominancia") < umbral).select(settings.GROUP_BY_FACTURA)
    
    print(f"    - Encontradas {len(df_fallas_punteo_ids)} Fallas de Punteo.")
    print(f"    - Encontradas {len(df_mixtas_reales_ids)} Mixtas Reales.")
    
    return df_fallas_punteo_ids, df_mixtas_reales_ids

# --- FUNCIÓN ORQUESTADORA PRINCIPAL REESTRUCTURADA ---
def generar_y_comprobar_todas_las_tablas():
    """Ejecuta el pipeline completo siguiendo la jerarquía de clasificación."""
    print("INICIANDO PROCESO COMPLETO CON JERARQUÍA...")
    
    df_base = obtener_y_limpiar_datos_base()
    if df_base.is_empty():
        return None, {"error": "No se encontraron datos válidos para procesar."}

    facturas_restantes_ids = df_base.select(settings.GROUP_BY_FACTURA).unique()
    dataframes_finales = {}
    summary_counts = {}
    
    # Nivel 1: Inconsistencias
    df_inconsistencias_ids = _detectar_inconsistencias(df_base)
    dataframes_finales["Inconsistencias"] = df_base.join(df_inconsistencias_ids, on=settings.GROUP_BY_FACTURA, how="inner")
    summary_counts["facturas_inconsistencias"] = len(df_inconsistencias_ids)
    facturas_restantes_ids = facturas_restantes_ids.join(df_inconsistencias_ids, on=settings.GROUP_BY_FACTURA, how="anti")
    df_base_nivel2 = df_base.join(facturas_restantes_ids, on=settings.GROUP_BY_FACTURA, how="inner")

    # Nivel 2: Puras
    facturas_puras_ids_dict = _detectar_facturas_puras(df_base_nivel2)
    ids_puras_consolidadas = []
    for tipo, ids in facturas_puras_ids_dict.items():
        dataframes_finales[tipo] = df_base_nivel2.join(ids, on=settings.GROUP_BY_FACTURA, how="inner")
        summary_counts[f"facturas_{tipo.lower()}"] = len(ids)
        ids_puras_consolidadas.append(ids)
    
    df_ids_puras_totales = pl.concat(ids_puras_consolidadas).unique()
    facturas_restantes_ids = facturas_restantes_ids.join(df_ids_puras_totales, on=settings.GROUP_BY_FACTURA, how="anti")
    df_base_nivel3 = df_base.join(facturas_restantes_ids, on=settings.GROUP_BY_FACTURA, how="inner")
    
    # Nivel 3 y 4: Fallas y Mixtas Reales
    df_fallas_ids, df_mixtas_ids = _separar_fallas_y_mixtas(df_base_nivel3)
    dataframes_finales["Fallas_Punteo"] = df_base_nivel3.join(df_fallas_ids, on=settings.GROUP_BY_FACTURA, how="inner")
    dataframes_finales["Mixtas_Reales"] = df_base_nivel3.join(df_mixtas_ids, on=settings.GROUP_BY_FACTURA, how="inner")
    summary_counts["facturas_fallas_punteo"] = len(df_fallas_ids)
    summary_counts["facturas_mixtas_reales"] = len(df_mixtas_ids)
    
    # Comprobación final
    total_base = df_base.select(settings.GROUP_BY_FACTURA).n_unique()
    suma_categorizadas = sum(summary_counts.values())
    
    summary_counts["total_facturas_base"] = total_base
    summary_counts["suma_categorizadas"] = suma_categorizadas
    summary_counts["comprobacion_exitosa"] = total_base == suma_categorizadas
    
    print("PROCESO COMPLETO FINALIZADO.")
    return dataframes_finales, summary_counts

# --- generar_excel_en_memoria (actualizado para los nuevos nombres de DFs) ---
def generar_excel_en_memoria(dataframes_procesados):
    """Crea un archivo Excel en memoria con formato personalizado."""
    buffer = io.BytesIO()

    nombres_hojas = {
        "T1": "T1_Radicadas",
        "T2": "T2_ConCC_SinFR",
        "T3": "T3_SinCC_SinFR",
        "T4": "T4_SinCC_ConFR",
        "Fallas_Punteo": "Fallas_Punteo",
        "Mixtas_Reales": "Mixtas_Reales",
        "Inconsistencias": "Inconsistencias"
    }
    
    with pd.ExcelWriter(buffer, engine='xlsxwriter', date_format='yyyy-mm-dd', datetime_format='yyyy-mm-dd') as writer:
        workbook = writer.book
        formato_fecha = workbook.add_format({'num_format': 'yyyy-mm-dd'})

        for key_df, df_items_polars in dataframes_procesados.items():
            sheet_name = nombres_hojas.get(key_df, key_df)
            print(f"Procesando hoja '{sheet_name}' para Excel...")

            if not df_items_polars.is_empty():
                df_reporte = crear_tabla_resumen_detalle_polars(df_items_polars, settings.GROUP_BY_FACTURA)
                
                mapping_export_local = settings.COLUMN_NAME_MAPPING_EXPORT.copy()
                mapping_export_local["FACTURA"] = "Factura Completa"
                
                df_pandas = df_reporte.to_pandas(use_pyarrow_extension_array=True)
                columnas_renombradas = {k: v for k, v in mapping_export_local.items() if k in df_pandas.columns}
                df_pandas.rename(columns=columnas_renombradas, inplace=True)
                
                df_pandas.to_excel(writer, sheet_name=sheet_name, index=False)
                
                worksheet = writer.sheets[sheet_name]
                header_list = list(df_pandas.columns)
                
                fechas_a_formatear = [
                    mapping_export_local.get(settings.COL_FECHA_OBJECION),
                    mapping_export_local.get(settings.COL_FECHA_CONTESTACION),
                    mapping_export_local.get(settings.COL_FECHA_RADICADO)
                ]
                
                for col_name in fechas_a_formatear:
                    if col_name and col_name in header_list:
                        col_idx = header_list.index(col_name)
                        worksheet.set_column(col_idx, col_idx, 12, formato_fecha)
                
                print(f"Hoja '{sheet_name}' escrita y formateada.")
            else:
                print(f"Hoja '{sheet_name}' omitida por estar vacía.")
            
    buffer.seek(0)
    return buffer