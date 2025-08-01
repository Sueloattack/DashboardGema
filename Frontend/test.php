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
            <h1 class="h3 mb-0 fw-bold text-dark">Dashboard de Análisis de Glosas</h1>
            <div class="d-flex align-items-center gap-2 p-2 border rounded-3" id="date-range-filter" style="visibility: hidden;">
                <label for="fecha_inicio" class="form-label mb-0">Desde:</label>
                <input type="date" id="fecha_inicio" name="fecha_inicio" class="form-control form-control-sm">
                <label for="fecha_fin" class="form-label mb-0">Hasta:</label>
                <input type="date" id="fecha_fin" name="fecha_fin" class="form-control form-control-sm">
            </div>
            <div class="d-flex gap-2">
                <button id="run-analysis-btn" class="btn btn-primary d-flex align-items-center gap-2" data-bs-toggle="tooltip" data-bs-placement="bottom" title="Ejecuta el análisis de glosas para el rango de fechas seleccionado.">
                    <span class="spinner-border spinner-border-sm" role="status" aria-hidden="true" style="display: none;"></span>
                    Ejecutar Análisis
                </button>
                <button id="download-excel-btn" class="btn btn-success" disabled data-bs-toggle="tooltip" data-bs-placement="bottom" title="Descarga un reporte detallado en formato Excel.">Descargar Reporte</button>
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
                        <h2 class="card-title text-center" data-bs-toggle="tooltip" data-bs-placement="top" title="Resumen general de las glosas en el período seleccionado.">Resumen general</h2>
                        <div class="row text-center">
                            <div class="col-md-4">
                                <div class="stat-group-card">
                                    <h3 id="stat-total" class="stat-value">--</h3>
                                    <p data-bs-toggle="tooltip" data-bs-placement="bottom" title="Número total de facturas de glosa en el período.">Total facturas</p>
                                    <h3 id="stat-valor-total" class="stat-currency">--</h3>
                                    <p data-bs-toggle="tooltip" data-bs-placement="bottom" title="Valor total de las glosas en el período.">Valor total</p>
                                </div>
                            </div>
                            <div class="col-md-4">
                                <div class="stat-group-card">
                                    <h3 id="stat-radicadas" class="stat-value">--</h3>
                                    <p data-bs-toggle="tooltip" data-bs-placement="bottom" title="Facturas de glosa que han sido radicadas.">Facturas radicadas</p>
                                    <h3 id="stat-valor-radicado" class="stat-currency">--</h3>
                                    <p data-bs-toggle="tooltip" data-bs-placement="bottom" title="Valor total de las glosas radicadas.">Valor radicado</p>
                                </div>
                            </div>
                            <div class="col-md-4">
                                <div class="stat-group-card">
                                    <h3 id="stat-no-radicadas" class="stat-value">--</h3>
                                    <p data-bs-toggle="tooltip" data-bs-placement="bottom" title="Facturas de glosa que aún no han sido radicadas.">Facturas no radicadas</p>
                                    <h3 id="stat-valor-no-radicado" class="stat-currency">--</h3>
                                    <p data-bs-toggle="tooltip" data-bs-placement="bottom" title="Valor total de las glosas no radicadas.">Valor no radicado</p>
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
                        <h2 class="card-title">
                            Top de entidades con respuestas de glosas no radicadas
                            <i class="fas fa-question-circle ms-2" data-bs-toggle="tooltip" data-bs-placement="top" title="Las entidades con mayor número de glosas no radicadas."></i>
                        </h2>
                        <div id="chart-entidades"></div>
                    </div>
                </div>
            </div>
        </main>
    </div>

    <!-- Bootstrap JS -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
    <!-- App Logic -->
    <script type="module" src="js/app.js"></script>
</body>
</html>