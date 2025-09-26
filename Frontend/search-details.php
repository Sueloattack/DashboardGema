<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Resultados de Búsqueda - Dashboard de Glosas</title>
    <!-- CSS (igual que los otros) -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
    <link rel="stylesheet" href="css/style.css">
</head>
<body>
    <div class="container-fluid p-4">
        <!-- Cabecera -->
        <header class="d-flex flex-wrap align-items-center justify-content-between pb-3 mb-4 border-bottom">
            <h1 id="details-title" class="h3 mb-0">Cargando resultados...</h1>
            <div class="d-flex align-items-center gap-3">
                <div id="saldo-acumulado-container" class="text-end" style="visibility: hidden;">
                    <span class="text-muted">Saldo Acumulado</span>
                    <p id="saldo-acumulado-valor" class="h4 mb-0 fw-bold">--</p>
                </div>
                <button id="download-excel-button" class="btn btn-success" style="display: none;">Descargar Excel</button>
                <a href="#" id="back-link" class="btn btn-secondary">← Volver al Dashboard</a>
            </div>
        </header>

        <!-- Área de Notificaciones -->
        <div id="notification-area"></div>

        <!-- Loader -->
        <div id="skeleton-table-loader" style="display: none;"> <!-- Oculto por defecto -->
             <div class="card"><div class="card-body" style="height: 400px;"></div></div>
        </div>

        <!-- Contenido Principal de Detalles (este contenedor es importante) -->
        <main id="details-main-content">
            <div class="card data-table-card mt-4">
                <div class="card-body">
                    <div id="data-table" class="table-responsive">
                        <!-- La tabla será generada aquí por el script -->
                    </div>
                </div>
            </div>
        </main>
    </div>

    <!-- Scripts -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
    <script type="module" src="js/search-details.js"></script>
</body>
</html>