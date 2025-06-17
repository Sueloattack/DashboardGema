# --- Columnas y Valores ---
COLUMNS_FROM_DB = [
    "fecha_gl", "nom_entidad", "gl_docn", "fc_serie", "fc_docn",
    "vr_glosa", "tipo", "freg", "gr_docn", "fecha_rep", "estatus1",
]

VALID_ESTATUS_VALUES = ["AI", "C1", "C2", "C3", "CO"]

# --- Alias de Columnas para Lógica Interna ---
COL_FECHA_OBJECION = "fecha_gl"
COL_ENTIDAD = "nom_entidad"
COL_GL_DOCN = "gl_docn"
COL_SERIE = "fc_serie"
COL_N_FACTURA = "fc_docn"
COL_VR_GLOSA = "vr_glosa"
COL_TIPO = "tipo"
COL_FECHA_CONTESTACION = "freg"
COL_CARPETA_CC = "gr_docn"
COL_FECHA_RADICADO = "fecha_rep"
COL_ESTATUS = "estatus1"

# --- Agrupaciones y Mapeos ---
GROUP_BY_FACTURA = [COL_SERIE, COL_N_FACTURA, COL_GL_DOCN]

EXCEL_OUTPUT_FILENAME_BASE = "Reporte_de_Radicaciones"

COLUMN_NAME_MAPPING_EXPORT = {
    # Columnas de identificación y fecha (aparecerán primero)
    COL_SERIE: "Serie Factura",
    COL_N_FACTURA: "N° Factura",
    COL_GL_DOCN: "No. Paciente (Gl_docn)",
    COL_ENTIDAD: "Entidad",
    COL_FECHA_OBJECION: "Fecha Objeción",
    COL_FECHA_CONTESTACION: "Fecha Contestación",
    COL_FECHA_RADICADO: "Fecha Radicado",
    # Columnas de estado y valor
    COL_CARPETA_CC: "Cuenta de Cobro",
    COL_ESTATUS: "Estatus",
    COL_VR_GLOSA: "Valor Glosa Ítem",
    COL_TIPO: "Tipo Ítem",
    # Columnas calculadas en el resumen
    "Total_Items_Factura": "Total Ítems en Factura",
    "Items_ConCC_ConFR": "Ítems Con CC y Con FR",
    "Items_ConCC_SinFR": "Ítems Con CC y Sin FR",
    "Items_SinCC_ConFR": "Ítems Sin CC y Con FR",
    "Items_SinCC_SinFR": "Ítems Sin CC y Sin FR",
    # Columna de tipo de fila
    "TipoFila": "Tipo de Fila",
}