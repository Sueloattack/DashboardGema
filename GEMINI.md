# DashboardGema

## Descripción del Proyecto

Este proyecto es un dashboard de análisis de "glosas" diseñado para visualizar KPIs y datos agregados.

*   **Arquitectura Actual:**
    *   **Backend:** API REST con Flask (Python), Polars y MySQL.
    *   **Frontend:** PHP y Vanilla JavaScript con ApexCharts.

*   **Propósito:** Permitir a los usuarios analizar datos de glosas a través de un dashboard interactivo, con filtros por fecha y la capacidad de descargar reportes en Excel.

---

## Plan de Mejora Activo

El objetivo actual es refactorizar y optimizar el proyecto para mejorar el rendimiento del backend y modernizar la experiencia de usuario del frontend.

### Fase 1: Optimización del Backend

-   [ ] **1. Medir Rendimiento:** Implementar un decorador en `app.py` para medir y registrar en consola el tiempo de ejecución de cada endpoint en milisegundos.
-   [ ] **2. Implementar Caching Estratégico:** Modificar `logic/data_processor.py` para cachear la consulta inicial a la base de datos (`_obtener_datos_crudos_cache`) y evitar accesos repetidos a la BD.
-   [ ] **3. Adoptar Lazy API de Polars:** Refactorizar la lógica de procesamiento de datos para usar la API "lazy" de Polars (`.lazy()`), construir un plan de ejecución y ejecutarlo con `.collect()` para máxima eficiencia.
-   [ ] **4. Usar Tipos de Datos Eficientes:** Dentro del plan de Polars, convertir columnas de baja cardinalidad (ej: "estado_glosa") a tipo `Categorical` para reducir el uso de memoria y acelerar las operaciones.

### Fase 2: Modernización del Frontend (PHP/JS Vanilla)

-   [ ] **1. Refactorizar JavaScript:** Modularizar el código en `app.js` y `details.js` para separar responsabilidades (ej. lógica de API, manipulación del DOM, inicialización de gráficos).
-   [ ] **2. Mejorar la Interfaz de Usuario (UI):** Modernizar el CSS en `style.css` para mejorar la apariencia visual y la responsividad del dashboard.
-   [ ] **3. Optimizar la Carga de Datos:** Implementar estados de carga visuales (ej. spinners o "esqueletos") mientras se obtienen los datos del backend para mejorar la experiencia de usuario (UX).
-   [ ] **4. Componentes PHP Reutilizables:** Refactorizar el código PHP en `test.php` y `details.php` para extraer partes comunes (como el encabezado o el pie de página) en archivos `include` separados.

---

## Próximos Pasos (Consideraciones Futuras)

Una vez completado el plan de mejora actual, se podrían considerar los siguientes puntos:

*   **Testing:** Añadir suites de pruebas unitarias e de integración para el backend.
*   **Autenticación:** Proteger la API con un sistema de autenticación (ej: JWT).
*   **Gestión de Dependencias:** Usar `pip-tools` para gestionar las dependencias de Python de forma más robusta.
*   **CI/CD:** Configurar un pipeline de integración y despliegue continuo.