# config/settings.py

# --- Columnas y Mapeos de la NUEVA ESTRUCTURA de BD ---
# Lista completa de todas las columnas que traeremos con el JOIN
COLUMNS_FROM_DB = [
    # De glo_cab_test
    "fechanotificacion", "tipo", "nom_entidad", "fc_serie", "fc_docn", "saldocartera",
    # De glo_det
    "fecha_gl", "gl_docn", "estatus1", "vr_glosa", "freg", "gr_docn", "fecha_rep"
]

VALID_ESTATUS_VALUES = ["AI", "C1", "C2", "C3", "CO"]

# --- Mapeo de constantes internas a los nombres de columna REALES de la BD ---
# Esta sección es clave. La lógica interna sigue usando estos alias.
COL_FECHA_NOTIFICACION = "fechanotificacion"
COL_FECHA_OBJECION = "fecha_gl"             # de glo_det
COL_ENTIDAD = "nom_entidad"                 # de glo_cab_test
COL_GL_DOCN = "gl_docn"                     # Común a ambas
COL_SERIE = "fc_serie"                      # de glo_cab_test
COL_N_FACTURA = "fc_docn"                   # de glo_cab_test
COL_VR_GLOSA = "vr_glosa"                   # de glo_det
COL_TIPO = "tipo"                           # de glo_cab_test
COL_FECHA_CONTESTACION = "freg"             # de glo_det
COL_CARPETA_CC = "gr_docn"                  # de glo_det
COL_FECHA_RADICADO = "fecha_rep"            # de glo_det
COL_ESTATUS = "estatus1"                    # de glo_det

# Agrupaciones y nombres para exportación
GROUP_BY_FACTURA = [COL_SERIE, COL_N_FACTURA, COL_GL_DOCN]
EXCEL_OUTPUT_FILENAME_BASE = "Reporte_de_Radicaciones"
COLUMN_NAME_MAPPING_EXPORT = {
    # Nombres internos a nombres de columna en Excel
    #COL_SERIE: "Serie Factura",
    #COL_N_FACTURA: "N° Factura",
    COL_FECHA_NOTIFICACION : "Fecha de notificación",
    "FACTURA": "Factura", # Columna creada dinámicamente
    COL_GL_DOCN: "No. Paciente",
    COL_ENTIDAD: "Entidad",
    #"fechanotificacion": "Fecha Notificación FC", # Nueva columna de cabecera
    COL_FECHA_OBJECION: "Fecha Objeción", # Aclaramos que es del ítem
    COL_FECHA_CONTESTACION: "Fecha Contestación",
    COL_CARPETA_CC : "Cuenta de cobro",
    COL_FECHA_RADICADO: "Fecha Radicado",
    COL_ESTATUS: "Estatus",
    COL_VR_GLOSA: "Valor Glosa",
    COL_TIPO: "Tipo",
    "Total_Items_Factura": "Total Ítems en Factura",
    "Items_ConCC_ConFR": "Con CC y Con FR",
    "Items_ConCC_SinFR": "Con CC y Sin FR",
    "Items_SinCC_ConFR": "Sin CC y Con FR",
    "Items_SinCC_SinFR": "Sin CC y Sin FR",
    "TipoFila": "Tipo de Fila",
}