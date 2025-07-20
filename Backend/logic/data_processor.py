# logic/data_processor.py

"""
Módulo de Lógica de Negocio (El Cerebro de la Aplicación).

Este archivo contiene toda la lógica para procesar los datos de las glosas.
Sus responsabilidades principales son:
1.  Obtener datos crudos de la capa de acceso a datos (`db`).
2.  Limpiar y estandarizar los datos usando la biblioteca Polars.
3.  Clasificar las facturas en categorías definidas (T1, T2, T3, T4, Mixtas).
4.  Realizar cálculos y agregaciones para generar KPIs y estadísticas para el dashboard.
5.  Crear estructuras de datos jerárquicas (resumen/detalle) para reportes.
6.  Generar el archivo Excel final en memoria para su descarga.
"""

# --- Importaciones ---
import polars as pl
import pandas as pd
import io
import datetime

# Módulos locales
from config import settings
from db.mySQL_connector import obtener_datos_glosas


def obtener_y_limpiar_datos_base(fecha_inicio: str = None, fecha_fin: str = None) -> pl.DataFrame:
    """
    Carga los datos desde la base de datos, los limpia y los prepara para el análisis.

    Esta función actúa como el primer paso en cualquier flujo de procesamiento. Llama al
    conector de la base de datos, convierte los resultados en un DataFrame de Polars,
    y realiza la limpieza y conversión de tipos de datos esenciales.

    Args:
        fecha_inicio (str, optional): La fecha de inicio para el filtro de datos. Formato 'YYYY-MM-DD'.
        fecha_fin (str, optional): La fecha de fin para el filtro de datos. Formato 'YYYY-MM-DD'.

    Raises:
        Exception: Si ocurre un error al obtener los datos de la base de datos.

    Returns:
        pl.DataFrame: Un DataFrame de Polars limpio y con los tipos de datos correctos,
                      listo para ser procesado.
    """
    print("Iniciando carga y limpieza de datos base...")
    
    # 1. Obtener datos crudos de la capa de base de datos
    registros, error = obtener_datos_glosas(fecha_inicio, fecha_fin)
    if error:
        raise Exception(f"Error en capa de datos al obtener glosas: {error}")
    
    # Si la consulta no devuelve registros, retornar un DataFrame vacío para evitar errores.
    if not registros:
        print("Advertencia: La consulta a la base de datos no devolvió registros.")
        return pl.DataFrame()

    # 2. Convertir a DataFrame de Polars
    df = pl.DataFrame(registros)

    # 3. Limpieza y conversión de tipos (Casting)
    # Es crucial asegurar que cada columna tenga el tipo de dato correcto antes de operar.
    columnas_fecha = [
        c for c in [
            settings.COL_FECHA_NOTIFICACION, 
            settings.COL_FECHA_OBJECION, 
            settings.COL_FECHA_RADICADO, 
            settings.COL_FECHA_CONTESTACION
        ] if c in df.columns
    ]
    
    df = df.with_columns(
        # Convertir columnas de fecha a tipo Datetime. `strict=False` las convierte a Nulo si el formato es inválido.
        [pl.col(c).cast(pl.Datetime, strict=False) for c in columnas_fecha] +
        # Convertir columnas numéricas, rellenando valores nulos con 0.
        [
            pl.col(settings.COL_CARPETA_CC).cast(pl.Int64, strict=False).fill_null(0),
            pl.col(settings.COL_VR_GLOSA).cast(pl.Float64, strict=False).fill_null(0),
            pl.col("saldocartera").cast(pl.Float64, strict=False).fill_null(0),
        ]
    )

    # 4. Filtrar solo los ítems con estatus válidos definidos en la configuración.
    return df.filter(pl.col(settings.COL_ESTATUS).is_in(settings.VALID_ESTATUS_VALUES))


def crear_tabla_resumen_detalle_polars(df_items: pl.DataFrame) -> pl.DataFrame:
    """
    Transforma un DataFrame de ítems de glosa en una estructura jerárquica de Resumen y Detalle.

    Por cada factura, esta función genera una fila de "Resumen Factura" con datos agregados
    y luego añade las filas de "Detalle Ítem" originales. Es la base para los reportes en Excel
    y las tablas paginadas.

    Args:
        df_items (pl.DataFrame): Un DataFrame que contiene los ítems de glosa a procesar.

    Returns:
        pl.DataFrame: Un DataFrame ordenado con filas de Resumen y Detalle.
    """
    # Si el DataFrame de entrada está vacío, devolverlo directamente.
    if df_items.is_empty():
        return df_items

    # --- PASO 1: Preparación - Crear columnas auxiliares para facilitar los cálculos ---
    df_items_con_extras = df_items.with_columns(
        # Crear una columna "FACTURA" unificada para agrupar y mostrar.
        (pl.col(settings.COL_SERIE).cast(pl.Utf8).fill_null("") + 
         pl.col(settings.COL_N_FACTURA).cast(pl.Utf8).fill_null("")).alias("FACTURA"),
        
        # Crear banderas booleanas para simplificar las condiciones de conteo.
        (pl.col(settings.COL_CARPETA_CC) != 0).alias("_tiene_cc"),
        pl.col(settings.COL_FECHA_RADICADO).is_not_null().alias("_tiene_fr")
    )
    
    # --- PASO 2: Agregación - Crear las filas de "Resumen Factura" ---
    df_resumen = df_items_con_extras.group_by(settings.GROUP_BY_FACTURA + ["FACTURA"]).agg(
        # Obtener el primer valor para columnas que son iguales para toda la factura.
        pl.first(settings.COL_ENTIDAD), 
        pl.first(settings.COL_FECHA_NOTIFICACION),
        
        # Obtener fechas extremas de los ítems de la factura.
        pl.min(settings.COL_FECHA_OBJECION).alias(settings.COL_FECHA_OBJECION),
        pl.max(settings.COL_FECHA_CONTESTACION).alias(settings.COL_FECHA_CONTESTACION),
        
        # Conteo total de ítems en la factura.
        pl.count().cast(pl.UInt32).alias("Total_Items_Factura"),

        # Conteos condicionales basados en las banderas booleanas. El `cast` a UInt32 es
        # crucial para asegurar que el tipo de dato coincida con las columnas placeholder
        # en la tabla de detalle, evitando errores de esquema al concatenar.
        (pl.when(pl.col("_tiene_cc") & pl.col("_tiene_fr")).then(1).otherwise(0)).sum().cast(pl.UInt32).alias("Items_ConCC_ConFR"),
        (pl.when(pl.col("_tiene_cc") & ~pl.col("_tiene_fr")).then(1).otherwise(0)).sum().cast(pl.UInt32).alias("Items_ConCC_SinFR"),
        (pl.when(~pl.col("_tiene_cc") & pl.col("_tiene_fr")).then(1).otherwise(0)).sum().cast(pl.UInt32).alias("Items_SinCC_ConFR"),
        (pl.when(~pl.col("_tiene_cc") & ~pl.col("_tiene_fr")).then(1).otherwise(0)).sum().cast(pl.UInt32).alias("Items_SinCC_SinFR"),

        # Para el resumen, el valor de la glosa es el saldo total de la cartera de la factura.
        pl.first("saldocartera").alias(settings.COL_VR_GLOSA),
        pl.first(settings.COL_TIPO).alias(settings.COL_TIPO)
    ).with_columns(
        # Añadir columnas que solo existen en la fila de Resumen.
        pl.lit("Resumen Factura").alias("TipoFila"),
        # Crear columnas nulas placeholder para que el esquema coincida con el detalle.
        pl.lit(None, dtype=pl.Int64).alias(settings.COL_CARPETA_CC),
        pl.lit(None, dtype=pl.Datetime).alias(settings.COL_FECHA_RADICADO),
        pl.lit(None, dtype=pl.Utf8).alias(settings.COL_ESTATUS)
    )

    # --- PASO 3: Preparación - Adecuar las filas de "Detalle Ítem" ---
    df_detalle = df_items_con_extras.with_columns(
        # Añadir la columna de tipo de fila.
        pl.lit("Detalle Ítem").alias("TipoFila"),
        # Mostrar nulo en lugar de 0 para la carpeta CC.
        pl.when(pl.col(settings.COL_CARPETA_CC) == 0).then(None).otherwise(pl.col(settings.COL_CARPETA_CC)).cast(pl.Int64),
        # Crear columnas de conteo nulas, ya que solo aplican al resumen.
        *[pl.lit(None, dtype=pl.UInt32).alias(c) for c in [
            "Total_Items_Factura", "Items_ConCC_ConFR", "Items_ConCC_SinFR", 
            "Items_SinCC_ConFR", "Items_SinCC_SinFR"
        ]]
    )
    
    # --- PASO 4: Combinación y Ordenamiento ---
    # `how="diagonal"` concatena los DataFrames, uniendo columnas con el mismo nombre.
    df_combinado = pl.concat([df_resumen, df_detalle], how="diagonal")
    
    # Ordenar de forma jerárquica: por factura, luego resumen antes que detalle, y finalmente por fecha y estatus.
    df_ordenado = df_combinado.with_columns([
        # Columna de orden para poner "Resumen Factura" (0) antes de "Detalle Ítem" (1).
        pl.when(pl.col("TipoFila") == "Resumen Factura").then(0).otherwise(1).alias("orden_tipo"),
        # Columna de orden para el estatus de los ítems.
        pl.when(pl.col(settings.COL_ESTATUS) == 'C1').then(1)
          .when(pl.col(settings.COL_ESTATUS) == 'C2').then(2)
          .when(pl.col(settings.COL_ESTATUS) == 'C3').then(3)
          .when(pl.col(settings.COL_ESTATUS) == 'CO').then(4)
          .when(pl.col(settings.COL_ESTATUS) == 'AI').then(5)
          .otherwise(99).alias("orden_estatus")
    ]).sort(
        settings.GROUP_BY_FACTURA + ["FACTURA", "orden_tipo", settings.COL_FECHA_OBJECION, "orden_estatus"]
    ).drop("orden_tipo", "orden_estatus", "_tiene_cc", "_tiene_fr") # Eliminar columnas de orden auxiliares.
    
    # --- PASO 5: Selección Final de Columnas ---
    # Definir el orden final deseado para las columnas del reporte.
    orden_final_columnas = [
        settings.COL_FECHA_NOTIFICACION, "FACTURA", settings.COL_GL_DOCN,
        settings.COL_ENTIDAD, settings.COL_FECHA_OBJECION, settings.COL_FECHA_CONTESTACION,
        settings.COL_CARPETA_CC, settings.COL_FECHA_RADICADO, settings.COL_ESTATUS,
        settings.COL_VR_GLOSA, settings.COL_TIPO, "TipoFila", "Total_Items_Factura",
        "Items_ConCC_ConFR", "Items_ConCC_SinFR", "Items_SinCC_ConFR", "Items_SinCC_SinFR"
    ]
    
    # Asegurarse de que todas las columnas existan, añadiéndolas como nulas si no.
    for col in orden_final_columnas:
        if col not in df_ordenado.columns:
            df_ordenado = df_ordenado.with_columns(pl.lit(None).alias(col))
    
    # Retornar el DataFrame final con las columnas en el orden correcto.
    return df_ordenado.select(orden_final_columnas)


def generar_y_comprobar_todas_las_tablas(fecha_inicio: str = None, fecha_fin: str = None) -> tuple:
    """
    Función orquestadora principal que realiza el análisis completo para el dashboard.

    Este es el motor de análisis que:
    1.  Carga y limpia los datos.
    2.  Clasifica cada factura completa en una categoría (T1-T4, Mixtas).
    3.  Calcula todas las estadísticas y KPIs para el frontend (conteos, sumas, datos para gráficos).
    4.  Realiza una comprobación de integridad para asegurar que todas las facturas fueron clasificadas.

    Args:
        fecha_inicio (str, optional): La fecha de inicio del análisis.
        fecha_fin (str, optional): La fecha de fin del análisis.

    Returns:
        tuple: Una tupla conteniendo:
            - dict: Un diccionario de DataFrames (`dfs`), donde cada clave es una categoría ('T1', 'Mixtas', etc.).
            - dict: Un diccionario con todas las estadísticas (`s_counts`) para la API.
    """
    df_base = obtener_y_limpiar_datos_base(fecha_inicio, fecha_fin)

    # --- Manejo del caso de DataFrame vacío ---
    if df_base.is_empty():
        # Define una estructura de respuesta vacía pero consistente si no hay datos.
        keys_resumen = [
            "total_facturas_base", "suma_categorizadas", "valor_total_periodo", 
            "valor_total_no_radicado", "valor_total_radicado", "facturas_t1", 
            "facturas_t2", "facturas_t3", "facturas_t4", "facturas_mixtas"
        ]
        empty_dfs = {"T1": pl.DataFrame(), "T2": pl.DataFrame(), "T3": pl.DataFrame(), 
                     "T4": pl.DataFrame(), "Mixtas": pl.DataFrame(), "df_base": pl.DataFrame()}
        empty_resumen = {
            "error": "No hay datos en el rango de fechas seleccionado.",
            "comprobacion_exitosa": True, 
            **{k: 0 for k in keys_resumen}, 
            "conteo_por_entidad": [], 
            "conteo_por_estatus": [],
            "ingresos_por_periodo": [],
            "granularidad_ingresos": "ninguna"
        }
        return empty_dfs, empty_resumen

    # --- Clasificación de facturas (Lógica principal) ---
    # 1. Crear un ID único para cada factura para poder agrupar sus ítems.
    df_base = df_base.with_columns(
        pl.concat_str(
            [pl.col(c).cast(pl.Utf8).fill_null("") for c in settings.GROUP_BY_FACTURA], 
            separator="-"
        ).alias("factura_id")
    )
    
    # 2. Definir las condiciones para cada categoría.
    conds = {
        "T1": (pl.col(settings.COL_CARPETA_CC) != 0) & (pl.col(settings.COL_FECHA_RADICADO).is_not_null()), # Radicada
        "T2": (pl.col(settings.COL_CARPETA_CC) != 0) & (pl.col(settings.COL_FECHA_RADICADO).is_null()),      # Con CC, Sin FR
        "T3": (pl.col(settings.COL_CARPETA_CC) == 0) & (pl.col(settings.COL_FECHA_RADICADO).is_null()),      # Sin CC, Sin FR
        "T4": (pl.col(settings.COL_CARPETA_CC) == 0) & (pl.col(settings.COL_FECHA_RADICADO).is_not_null()) # Sin CC, Con FR
    }
    
    # 3. Marcar cada factura si *TODOS* sus ítems cumplen una condición (facturas "puras").
    # La expresión `.sum().over("factura_id") == pl.count().over("factura_id")` cuenta los ítems que cumplen
    # la condición dentro de cada grupo de factura, y lo compara con el total de ítems del grupo.
    df_base = df_base.with_columns(
        [(cond.sum().over("factura_id") == pl.count().over("factura_id")).alias(f"es_{tipo}") 
         for tipo, cond in conds.items()]
    )
    
    # 4. Filtrar y contar las facturas puras.
    dfs, s_counts, ids_puras_total = {}, {}, []
    for tipo in conds:
        dfs[tipo] = df_base.filter(pl.col(f"es_{tipo}"))
        ids_puras = dfs[tipo].select("factura_id").unique()
        s_counts[f"facturas_{tipo.lower()}"] = len(ids_puras)
        if not ids_puras.is_empty(): 
            ids_puras_total.append(ids_puras)
    
    # 5. Las facturas "Mixtas" son todas aquellas que no cayeron en ninguna categoría pura.
    ids_puras_df = pl.concat(ids_puras_total).unique() if ids_puras_total else pl.DataFrame({"factura_id": []})
    dfs["Mixtas"] = df_base.join(ids_puras_df, on="factura_id", how="anti")
    s_counts["facturas_mixtas"] = dfs["Mixtas"].select("factura_id").n_unique()

    # --- Agregados y Estadísticas (KPIs para el Dashboard) ---
    # Usar `.unique(subset="factura_id")` es clave para no sumar el `saldocartera` múltiples veces.
    df_saldos_unicos = df_base.select(["factura_id", "saldocartera"]).unique(subset="factura_id")
    s_counts["valor_total_periodo"] = df_saldos_unicos["saldocartera"].sum() or 0

    s_counts["valor_total_radicado"] = 0
    if not dfs["T1"].is_empty():
        s_counts["valor_total_radicado"] = dfs["T1"].select(["factura_id", "saldocartera"]).unique(subset="factura_id")["saldocartera"].sum() or 0

    # Para los no radicados, consideramos todo lo que no esté en T1.
    ids_t1 = dfs.get("T1", pl.DataFrame()).select("factura_id").unique()
    df_no_radicadas = df_base.join(ids_t1, on="factura_id", how="anti")

    s_counts["valor_total_no_radicado"] = 0
    s_counts["conteo_por_entidad"] = []
    s_counts["conteo_por_estatus"] = []
    if not df_no_radicadas.is_empty():
        s_counts["valor_total_no_radicado"] = df_no_radicadas.select(["factura_id", "saldocartera"]).unique(subset="factura_id")["saldocartera"].sum() or 0
        s_counts["conteo_por_entidad"] = df_no_radicadas.group_by(settings.COL_ENTIDAD).agg(pl.n_unique("factura_id").alias("total_facturas")).sort("total_facturas", descending=True).limit(15).to_dicts()
    
    df_conteo_estatus = df_no_radicadas.group_by(settings.COL_ESTATUS).agg(pl.count().alias("total_items"))

    # Añadimos una columna temporal para definir el orden deseado
    df_conteo_estatus_ordenado = df_conteo_estatus.with_columns(
        pl.when(pl.col(settings.COL_ESTATUS) == 'C1').then(1)
        .when(pl.col(settings.COL_ESTATUS) == 'C2').then(2)
        .when(pl.col(settings.COL_ESTATUS) == 'C3').then(3)
        .when(pl.col(settings.COL_ESTATUS) == 'CO').then(4)
        .when(pl.col(settings.COL_ESTATUS) == 'AI').then(5)
        .otherwise(99).alias("orden_key") # Poner cualquier otro al final
    ).sort("orden_key").drop("orden_key") # Ordenamos por la clave y luego la eliminamos

    # Convertimos el resultado ya ordenado a diccionario para enviarlo al frontend
    s_counts["conteo_por_estatus"] = df_conteo_estatus_ordenado.to_dicts()
    
    # --- Comprobación de integridad ---
    s_counts["total_facturas_base"] = df_base.select("factura_id").n_unique()
    s_counts["suma_categorizadas"] = s_counts["facturas_t1"] + s_counts["facturas_t2"] + s_counts["facturas_t3"] + s_counts["facturas_t4"] + s_counts["facturas_mixtas"]
    s_counts["comprobacion_exitosa"] = s_counts["total_facturas_base"] == s_counts["suma_categorizadas"]
    
    # --- Lógica para el Gráfico de Ingresos por Periodo (Gráfico de Series de Tiempo) ---
    # 1. Determinar la granularidad del gráfico (diaria, mensual, anual) según el rango de fechas.
    formato_fecha = "%Y-%m-%d"
    d_inicio = datetime.datetime.strptime(fecha_inicio, formato_fecha)
    d_fin = datetime.datetime.strptime(fecha_fin, formato_fecha)
    diferencia_dias = (d_fin - d_inicio).days
    
    if diferencia_dias <= 90:
        granularidad_agg, granularidad_label = "1d", 'Diaria'  # 1 day
    elif 90 < diferencia_dias <= 730:
        granularidad_agg, granularidad_label = "1mo", 'Mensual' # 1 month
    else:
        granularidad_agg, granularidad_label = "1y", 'Anual'   # 1 year

    # 2. Agrupar los datos reales. `dt.truncate` estandariza cada fecha al inicio de su período (día, mes o año).
    df_facturas_unicas = df_base.unique(subset=["factura_id"], keep="first")
    ingresos_reales_agrupados = (
        df_facturas_unicas.with_columns(
            pl.col(settings.COL_FECHA_NOTIFICACION).dt.truncate(granularidad_agg).alias("fecha_truncada")
        )
        .group_by("fecha_truncada")
        .agg(pl.n_unique("factura_id").alias("conteo"))
        .rename({"fecha_truncada": "fecha"})
        .with_columns(pl.col("fecha").cast(pl.Date))
        .sort("fecha")
    )

    # 3. Crear un rango completo de fechas con la misma granularidad. Esto asegura que se muestren
    #    los períodos sin datos (con valor 0) en el gráfico.
    rango_completo_generado = pl.date_range(start=d_inicio, end=d_fin, interval=granularidad_agg, eager=True).alias("fecha")
    df_rango_completo = pl.DataFrame(rango_completo_generado).with_columns(
        pl.col("fecha").dt.truncate(granularidad_agg)
    ).unique()

    # 4. Unir el rango completo con los datos reales usando un `left join`.
    df_final_grafico = df_rango_completo.join(ingresos_reales_agrupados, on="fecha", how="left")

    # 5. Preparar datos para el frontend con lógica condicional
    ingresos_para_frontend = (
        df_final_grafico.with_columns(
            # Esta lógica que convierte los 0 en null para el modo diario sigue siendo correcta
            pl.when(granularidad_label == 'Diaria')
              .then(
                  pl.when((pl.col("conteo") == 0) | pl.col("conteo").is_null())
                    .then(None) 
                    .otherwise(pl.col("conteo"))
              )
              .otherwise(
                  pl.col("conteo").fill_null(0)
              )
              .cast(pl.Int32)
              .alias("conteo"),
            
            pl.col("fecha").dt.strftime("%Y-%m-%dT00:00:00").alias("fecha_agrupada")
        )
        .select(["fecha_agrupada", "conteo"])
        .sort("fecha_agrupada")
    )

    # --- INICIO DEL NUEVO CAMBIO ---
    
    # 6. Filtrar los valores nulos SOLO para la vista diaria
    # De esta forma, el frontend solo recibe los puntos que debe dibujar
    ingresos_final_para_api = ingresos_para_frontend
    if granularidad_label == 'Diaria':
        ingresos_final_para_api = ingresos_para_frontend.filter(
            pl.col("conteo").is_not_null()
        )
    
    # 7. Añadir el resultado limpio al diccionario de estadísticas final
    s_counts["ingresos_por_periodo"] = ingresos_final_para_api.to_dicts()
    s_counts["granularidad_ingresos"] = granularidad_label
    
    # --- FIN DEL NUEVO CAMBIO ---

    # --- Devolver los resultados ---
    dfs["df_base"] = df_base
    return dfs, s_counts

def obtener_resumenes_paginados(fecha_inicio: str, fecha_fin: str, categorias: list, pagina: int, por_pagina: int, entidad: str = None):
    """
    Obtiene las filas de "Resumen de Factura" de forma paginada.
    Puede filtrar opcionalmente por una entidad específica, aplicando el filtro
    al final sobre las filas de resumen ya generadas.
    """
    print(f"Obteniendo Resúmenes: Categorías={categorias}, Página={pagina}, Entidad={entidad}")
    
    # 1. Obtener todos los datos y clasificaciones
    dataframes_clasificados, _ = generar_y_comprobar_todas_las_tablas(fecha_inicio, fecha_fin)
    
    df_base_original = dataframes_clasificados.get("df_base", pl.DataFrame())
    if df_base_original.is_empty():
        return {"data": [], "pagina_actual": 1, "total_paginas": 0, "total_registros": 0}

    # 2. Recolectar IDs de facturas de las categorías solicitadas
    ids_a_mostrar_list = [
        dataframes_clasificados[cat].select("factura_id") 
        for cat in categorias 
        if cat in dataframes_clasificados and not dataframes_clasificados[cat].is_empty()
    ]
    if not ids_a_mostrar_list:
        return {"data": [], "pagina_actual": 1, "total_paginas": 0, "total_registros": 0}
    
    df_ids_a_mostrar = pl.concat(ids_a_mostrar_list).unique()
    
    # 3. Obtener todos los ítems de las facturas que coinciden
    df_items_completos = df_base_original.join(df_ids_a_mostrar, on="factura_id", how="inner")
    
    if df_items_completos.is_empty():
        return {"data": [], "pagina_actual": 1, "total_paginas": 0, "total_registros": 0}
        
    # 4. Generar la vista Resumen/Detalle COMPLETA para todas las entidades en estas categorías
    print(f"Generando resúmenes para {df_items_completos.select('factura_id').n_unique()} facturas únicas...")
    df_reporte_completo = crear_tabla_resumen_detalle_polars(df_items_completos)

    # 5. Filtrar solo las filas de Resumen para trabajar con ellas
    df_resumenes = df_reporte_completo.filter(pl.col("TipoFila") == "Resumen Factura")
    
    # 6. --- CAMBIO DE LÓGICA CRÍTICO ---
    # AHORA filtramos la tabla de RESÚMENES por la entidad, si existe.
    if entidad:
        print(f"Filtrando {len(df_resumenes)} resúmenes por entidad: '{entidad}'")
        df_resumenes = df_resumenes.filter(
            pl.col(settings.COL_ENTIDAD) == entidad
        )
        print(f"Resúmenes restantes después del filtro: {len(df_resumenes)}")

    # 7. Paginación y retorno del resultado final ya filtrado
    total_registros = len(df_resumenes)
    total_paginas = (total_registros + por_pagina - 1) // por_pagina if por_pagina > 0 else 1
    offset = (pagina - 1) * por_pagina
    df_pagina = df_resumenes.slice(offset, por_pagina)
    
    datos_dict = df_pagina.with_columns(pl.col(pl.Datetime).dt.strftime("%Y-%m-%dT%H:%M:%S")).to_dicts()

    return {"data": datos_dict, "pagina_actual": pagina, "total_paginas": total_paginas, "total_registros": total_registros}


def obtener_detalle_especifico_factura(docn: int) -> list:
    """
    Obtiene todos los "Ítems de Detalle" para un `gl_docn` (factura) específico.

    Está diseñado para ser llamado por el frontend para la "carga perezosa" (lazy loading)
    del detalle cuando un usuario expande una fila de resumen.

    Args:
        docn (int): El identificador `gl_docn` de la factura.

    Returns:
        list: Una lista de diccionarios, donde cada diccionario es un ítem de detalle de la factura.
    """
    print(f"Obteniendo detalle para gl_docn: {docn}")

    # Es más simple y consistente reutilizar el flujo principal que crear una nueva consulta a la BD.
    # Se obtienen todos los datos (sin filtro de fecha) para asegurar que encontramos el `gl_docn`.
    df_base_completa = obtener_y_limpiar_datos_base(None, None)
    
    # Filtrar el DataFrame completo para obtener solo los ítems de la factura solicitada.
    df_items_factura = df_base_completa.filter(pl.col(settings.COL_GL_DOCN) == docn)
    if df_items_factura.is_empty():
        return []

    # Reutilizar la función de formato y devolver solo las filas de tipo "Detalle Ítem".
    df_detalle_final = crear_tabla_resumen_detalle_polars(df_items_factura).filter(
        pl.col("TipoFila") == "Detalle Ítem"
    )
    
    return df_detalle_final.to_dicts()


def generar_excel_en_memoria(dataframes: dict) -> io.BytesIO:
    """
    Genera un archivo Excel con múltiples hojas a partir de un diccionario de DataFrames.
    Omite la hoja 'df_base' y aplica formato a fechas y montos.
    """
    import pandas as pd
    import io

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

        # ❌ Eliminar df_base antes de iterar
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
