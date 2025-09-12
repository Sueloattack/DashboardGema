<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard de Análisis de Glosas</title>
    <!-- Google Fonts: Inter -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
    <!-- Bootstrap CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <!-- Font Awesome para iconos -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
    <!-- Estilos personalizados (después de Bootstrap) -->
    <link rel="stylesheet" href="css/style.css">
    <!-- ApexCharts -->
    <script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>
</head>
<body>
    <div class="container-fluid p-4">
        <!-- Cabecera -->
    <header class="d-flex flex-wrap align-items-center justify-content-between pb-3 mb-4 border-bottom">
        
        <!-- Elemento Izquierdo: El Título -->
        <h1 class="h3 mb-0 fw-bold text-dark">Dashboard de Análisis de Glosas</h1>
        
        <!-- Elemento Derecho: Un contenedor para todos los controles -->
        <div class="d-flex align-items-center gap-3">
            
            <!-- Control 1: Filtro de Fechas -->
            <div class="d-flex align-items-center gap-2 p-2 border rounded-3" id="date-range-filter" style="visibility: hidden;">
                <label for="fecha_inicio" class="form-label mb-0">Desde:</label>
                <input type="date" id="fecha_inicio" name="fecha_inicio" class="form-control form-control-sm">
                <label for="fecha_fin" class="form-label mb-0">Hasta:</label>
                <input type="date" id="fecha_fin" name="fecha_fin" class="form-control form-control-sm">
            </div>

            <!-- Control 2: Grupo de Botones de Acción -->
            <div class="input-group">
                <button id="buscar-factura-btn" class="btn btn-outline-secondary" data-bs-toggle="modal" data-bs-target="#modal-busqueda-facturas" title="Buscar por Factura Completa">
                    <i class="fas fa-search"></i>
                </button>
                <button id="run-analysis-btn" class="btn btn-primary d-flex align-items-center gap-2" data-bs-toggle="tooltip" data-bs-placement="bottom" title="Ejecuta el análisis de glosas para el rango de fechas seleccionado.">
                    <span class="spinner-border spinner-border-sm" role="status" aria-hidden="true" style="display: none;"></span>
                    Ejecutar Análisis
                </button>
                <button id="download-excel-btn" class="btn btn-success" disabled data-bs-toggle="tooltip" data-bs-placement="bottom" title="Descarga un reporte detallado en formato Excel.">
                    Descargar Reporte
                </button>
            </div>

        </div>
    </header>

        <!-- Notificaciones -->
        <div id="notification-area" class="alert" style="display: none;"></div>

        <!-- Mensaje Inicial -->
        <div id="initial-message" class="alert alert-info text-center">
            Selecciona un rango de fechas y haz clic en "Ejecutar Análisis" para cargar los datos.
        </div>

        <!-- Skeleton Loader -->
        <div id="skeleton-loader">
            <div class="row g-4">
                <div class="col-12">
                    <div class="skeleton-card skeleton-kpi"></div>
                </div>
                <div class="col-md-6"><div class="skeleton-card"></div></div>
                <div class="col-md-6"><div class="skeleton-card"></div></div>
                <div class="col-md-6"><div class="skeleton-card"></div></div>
                <div class="col-md-6"><div class="skeleton-card"></div></div>
                <div class="col-12"><div class="skeleton-card"></div></div>
            </div>
        </div>

        <!-- Contenido Principal del Dashboard -->
        <main id="dashboard-main-grid" class="row g-4" style="visibility: hidden;">
            <!-- Fila 1: KPIs -->
            <div class="col-12">
                <div class="card h-100">
                    <div class="card-body">
                        <h2 class="card-title text-center" data-bs-toggle="tooltip" data-bs-placement="top" title="Resumen general de glosas en el período seleccionado.">Resumen general</h2>
                        <div class="row text-center">
                            <div class="col-md-4">
                                <div class="stat-group-card">
                                    <h3 id="stat-total" class="stat-value">--</h3>
                                    <p class="kpi-label" data-bs-toggle="tooltip" data-bs-placement="bottom" title="Número total de glosas en el período.">Total glosas</p>
                                    <h3 id="stat-valor-total" class="stat-currency">--</h3>
                                    <p class="kpi-label" data-bs-toggle="tooltip" data-bs-placement="bottom" title="Valor total de las glosas en el período.">Valor total</p>
                                </div>
                            </div>
                            <div class="col-md-4">
                                <div class="stat-group-card">
                                    <h3 id="stat-radicadas" class="stat-value">--</h3>
                                    <p class="kpi-label" data-bs-toggle="tooltip" data-bs-placement="bottom" title="Glosas con cuenta de cobro y fecha de radicado.">Glosas radicadas</p>
                                    <h3 id="stat-valor-radicado" class="stat-currency">--</h3>
                                    <p class="kpi-label" data-bs-toggle="tooltip" data-bs-placement="bottom" title="Valor total de las glosas radicadas.">Valor radicado</p>
                                </div>
                            </div>
                            <div class="col-md-4">
                                <div class="stat-group-card">
                                    <h3 id="stat-no-radicadas" class="stat-value">--</h3>
                                    <p class="kpi-label" data-bs-toggle="tooltip" data-bs-placement="bottom" title="Glosas que aún no han sido radicadas completamente o errores.">Glosas no radicadas</p>
                                    <h3 id="stat-valor-no-radicado" class="stat-currency">--</h3>
                                    <p class="kpi-label" data-bs-toggle="tooltip" data-bs-placement="bottom" title="Valor total de las glosas no radicadas.">Valor no radicado</p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Fila 2: Gráficos Donut -->
            <div class="col-lg-6">
                <div class="card h-100">
                    <div class="card-body d-flex flex-column">
                        <h2 class="card-title">
                            Estado general de respuestas de glosas
                            <i class="fas fa-question-circle ms-2" data-bs-toggle="tooltip" data-bs-placement="top" title="Distribución de glosas radicadas vs. no radicadas."></i>
                        </h2>
                        <div id="chart-general" class="flex-grow-1"></div>
                    </div>
                </div>
            </div>
            <div class="col-lg-6">
                <div class="card h-100">
                    <div class="card-body d-flex flex-column">
                        <h2 class="card-title">
                            Desglose de respuestas de Glosas no radicadas
                            <i class="fas fa-question-circle ms-2" data-bs-toggle="tooltip" data-bs-placement="top" title="Desglose de las glosas no radicadas por tipo."></i>
                        </h2>
                        <div id="chart-puras" class="flex-grow-1"></div>
                    </div>
                </div>
            </div>

            <!-- Fila 3: Tabla y Gráfico de Línea -->
            <div class="col-lg-6">
                <div class="card h-100">
                    <div class="card-body d-flex flex-column">
                        <h2 class="card-title">
                            Ítems de respuestas de glosas pendientes por radicar
                            <i class="fas fa-question-circle ms-2" data-bs-toggle="tooltip" data-bs-placement="top" title="Ítems de glosa pendientes de radicación."></i>
                        </h2>
                        <div id="estatus-table-container" class="table-responsive flex-grow-1"></div>
                    </div>
                </div>
            </div>
            <div class="col-lg-6">
                <div class="card h-100">
                    <div class="card-body d-flex flex-column">
                        <h2 id="ingresos-chart-title" class="card-title">
                            Ingreso de glosas
                            <i class="fas fa-question-circle ms-2" data-bs-toggle="tooltip" data-bs-placement="top" title="Cantidad de glosas ingresadas por período de notificación."></i>
                        </h2>
                        <div id="chart-ingresos" class="flex-grow-1"></div>
                    </div>
                </div>
            </div>

            <!-- Fila 4: Top Entidades -->
            <div class="col-12">
                <div class="card">
                    <div class="card-body">
                        <h2 class="card-title d-flex justify-content-between align-items-center">
                            <span> <!-- Contenedor para el título -->
                                Top 15 de entidades con mayor cantidad de respuestas de glosas no radicadas
                                <i class="fas fa-question-circle ms-2" data-bs-toggle="tooltip" data-bs-placement="top" title="Top 15 entidades con respuestas de glosas no radicadas"></i>
                            </span>
                            <!-- ========= NUEVO BOTÓN DE FILTRO ========= -->
                            <button id="filtro-entidad-conteo-btn" class="btn btn-sm btn-outline-secondary" type="button" data-bs-toggle="modal" data-bs-target="#modal-filtro-conteo" disabled>
                                <i class="fas fa-filter"></i> Filtrar
                            </button>
                            <!-- ======================================= -->
                        </h2>
                        <div id="chart-entidades"></div>
                    </div>
                </div>
            </div>

            <!-- ========= INICIO DE LA NUEVA SECCIÓN ========= -->
            <!-- Fila 5: Top Entidades por Saldo -->
            <div class="col-12">
                <div class="card">
                    <div class="card-body">
                        <h2 class="card-title d-flex justify-content-between align-items-center">
                            <span> <!-- Contenedor para el título -->
                                Top 15 de entidades no radicadas por saldo en cartera 
                                <i class="fas fa-question-circle ms-2" data-bs-toggle="tooltip" data-bs-placement="top" title="Top 15 entidades con mayor saldo en cartera acumulado"></i>
                            </span>
                            <!-- ========= NUEVO BOTÓN DE FILTRO ========= -->
                            <button id="filtro-entidad-saldo-btn" class="btn btn-sm btn-outline-secondary" type="button" data-bs-toggle="modal" data-bs-target="#modal-filtro-saldo" disabled>
                                <i class="fas fa-filter"></i> Filtrar
                            </button>
                            <!-- ======================================= -->
                        </h2>
                        <div id="chart-saldo-entidades"></div>
                    </div>
                </div>
            </div>

            <!-- ========= FIN DE LA NUEVA SECCIÓN ========= -->
        </main>
    </div>

    <!-- ======================= INICIO DE LOS MODALES DE FILTRO (PON ESTO AL FINAL DEL BODY) ======================= -->
    <!-- Modal para el Filtro de Entidades por Cantidad de Facturas -->
    <div class="modal fade" id="modal-filtro-conteo" tabindex="-1" aria-labelledby="modalFiltroConteoLabel" aria-hidden="true">
        <div class="modal-dialog modal-dialog-scrollable">
            <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title" id="modalFiltroConteoLabel">Filtrar Entidades por Cantidad</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
                
                <!-- Filtros por valor -->
                <div class="input-group input-group-sm mb-3">
                    <span class="input-group-text">Facturas mayor o igual a</span>
                    <input type="number" class="form-control" id="min-conteo-input" placeholder="Ej: 100">
                </div>
                <div class="input-group input-group-sm mb-3">
                    <span class="input-group-text">Facturas menor o igual a</span>
                    <input type="number" class="form-control" id="max-conteo-input" placeholder="Ej: 10">
                </div>
                <hr> <!-- Un separador visual -->
                
                <!-- Contenedor para la lista de checkboxes -->
                <div id="modal-filtro-conteo-body">
                    <!-- Los checkboxes se generarán aquí con JavaScript -->
                    <p>Ejecuta un análisis para ver las entidades disponibles.</p>
                </div>

            </div>
            <div class="modal-footer">
                <button type="button" id="seleccionar-todas-conteo" class="btn btn-link">Seleccionar Todas</button>
                <button type="button" id="deseleccionar-todas-conteo" class="btn btn-link">Deseleccionar Todas</button>
                <button type="button" class="btn btn-primary" data-bs-dismiss="modal">Aplicar Filtro</button>
            </div>
            </div>
        </div>
    </div>

    <!-- Modal para el Filtro de Entidades por Saldo en Cartera -->
    <div class="modal fade" id="modal-filtro-saldo" tabindex="-1" aria-labelledby="modalFiltroSaldoLabel" aria-hidden="true">
        <div class="modal-dialog modal-dialog-scrollable">
            <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title" id="modalFiltroSaldoLabel">Filtrar Entidades por Saldo</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">

                <!-- Filtros por valor -->
                <div class="input-group input-group-sm mb-3">
                    <span class="input-group-text">Saldo mayor o igual a</span>
                    <input type="text" inputmode="numeric" class="form-control" id="min-saldo-input" placeholder="Ej: 1.000.000">
                </div>
                <div class="input-group input-group-sm mb-3">
                    <span class="input-group-text">Saldo menor o igual a</span>
                    <input type="text" inputmode="numeric" class="form-control" id="max-saldo-input" placeholder="Ej: 500.000">
                </div>
                <hr> <!-- Un separador visual -->

                <!-- Contenedor para la lista de checkboxes -->
                <div id="modal-filtro-saldo-body">
                    <!-- Los checkboxes se generarán aquí con JavaScript -->
                    <p>Ejecuta un análisis para ver las entidades disponibles.</p>
                </div>

            </div>
            <div class="modal-footer">
                <button type="button" id="seleccionar-todas-saldo" class="btn btn-link">Seleccionar Todas</button>
                <button type="button" id="deseleccionar-todas-saldo" class="btn btn-link">Deseleccionar Todas</button>
                <button type="button" class="btn btn-primary" data-bs-dismiss="modal">Aplicar Filtro</button>
            </div>
            </div>
        </div>
    </div>

    <div class="modal fade" id="modal-busqueda-facturas" tabindex="-1" aria-labelledby="modalBusquedaLabel" aria-hidden="true">
        <div class="modal-dialog">
            <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title" id="modalBusquedaLabel">Búsqueda por Factura Completa</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
                <p>Pega una lista de ID de Facturas (ej: FCR123456), una por línea.</p>
                <textarea id="busqueda-textarea" class="form-control" rows="10"></textarea>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancelar</button>
                <button type="button" id="ejecutar-busqueda-btn" class="btn btn-primary">Buscar</button>
            </div>
            </div>
        </div>
    </div>

    <!-- Bootstrap JS -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
    <!-- App Logic -->
    <script type="module" src="js/app.js"></script>
</body>
</html>