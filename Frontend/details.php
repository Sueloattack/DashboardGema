<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Detalle de Reporte - Dashboard de Glosas</title>
    <!-- Bootstrap CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <!-- Estilos personalizados (después de Bootstrap) -->
    <link rel="stylesheet" href="css/style.css">
</head>
<body>
    <div class="container-fluid p-4">
        <!-- Cabecera de Detalles -->
        <header class="d-flex flex-wrap align-items-center justify-content-between pb-3 mb-4 border-bottom">
            <h1 id="details-title" class="h3 mb-0">Cargando detalles...</h1>
            <a href="#" id="back-link" class="btn btn-secondary">← Volver al Dashboard</a>
        </header>

        <!-- Notificaciones -->
        <div id="notification-area" class="alert" style="display: none;"></div>

        <!-- Skeleton Loader para la tabla -->
        <div id="skeleton-table-loader" class="card" style="display: none;">
            <div class="card-body">
                <div class="skeleton-table-header"></div>
                <div class="skeleton-table-row"></div>
                <div class="skeleton-table-row"></div>
                <div class="skeleton-table-row"></div>
                <div class="skeleton-table-row"></div>
                <div class="skeleton-table-row"></div>
                <div class="skeleton-table-row"></div>
                <div class="skeleton-table-row"></div>
                <div class="skeleton-table-row"></div>
                <div class="skeleton-table-row"></div>
                <div class="skeleton-table-row"></div>
            </div>
        </div>

        <!-- Contenido Principal de Detalles -->
        <main id="details-main-content" style="display: none;">
            <div class="card data-table-card" id="data-table-card">
                <div class="card-body">
                    <div id="data-table" class="table-responsive">
                        <!-- La tabla HTML será generada aquí por details.js -->
                    </div>
                    <div id="pagination-container" class="d-flex justify-content-center mt-3">
                        <!-- Los controles de paginación serán generados aquí por details.js -->
                    </div>
                </div>
            </div>
        </main>
    </div>

    <!-- Bootstrap JS -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
    <!-- App Logic -->
    <script type="module" src="js/details.js"></script>
</body>
</html>
