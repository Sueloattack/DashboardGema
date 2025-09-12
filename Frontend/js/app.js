/* ==========================================================================
   js/app.js - Lógica Principal del Dashboard de Análisis de Glosas
   --------------------------------------------------------------------------
   Este script gestiona la interactividad del dashboard principal.
   ========================================================================== */

import { fetchApi } from './api.js';
import { showNotification, formatarMoneda } from './utils.js';
let fullEntidadData = [];
let fullSaldoData = [];

document.addEventListener('DOMContentLoaded', () => {

    // ==========================================================================
    // SECCIÓN 1: REFERENCIAS AL DOM
    // ==========================================================================
    const runBtn = document.getElementById('run-analysis-btn');
    const downloadBtn = document.getElementById('download-excel-btn');
    const dateFilter = document.getElementById('date-range-filter');
    const fechaInicioInput = document.getElementById('fecha_inicio');
    const fechaFinInput = document.getElementById('fecha_fin');
    const mainGrid = document.getElementById('dashboard-main-grid');
    const notificationArea = document.getElementById('notification-area');
    const initialMessage = document.getElementById('initial-message');
    const skeletonLoader = document.getElementById('skeleton-loader');
    const filtroConteoBtn = document.getElementById('filtro-entidad-conteo-btn');
    const modalFiltroConteoBody = document.getElementById('modal-filtro-conteo-body');

    const filtroSaldoBtn = document.getElementById('filtro-entidad-saldo-btn');
    const modalFiltroSaldoBody = document.getElementById('modal-filtro-saldo-body');

    const statTotal = document.getElementById('stat-total');
    const statValorTotal = document.getElementById('stat-valor-total');
    const statRadicadas = document.getElementById('stat-radicadas');
    const statValorRadicado = document.getElementById('stat-valor-radicado');
    const statNoRadicadas = document.getElementById('stat-no-radicadas');
    const statValorNoRadicado = document.getElementById('stat-valor-no-radicado');
    const estatusTableContainer = document.getElementById('estatus-table-container');
    const ingresosChartTitle = document.getElementById('ingresos-chart-title');
    const ejecutarBusquedaBtn = document.getElementById('ejecutar-busqueda-btn');
    const busquedaTextarea = document.getElementById('busqueda-textarea');

    const apexChartsSpanishLocale = {
        name: 'es',
        options: {
            months: ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'],
            shortMonths: ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'],
            days: ['Domingo', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado'],
            shortDays: ['Dom', 'Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb'],
            toolbar: {
                download: 'Descargar SVG',
                selection: 'Selección',
                selectionZoom: 'Zoom de Selección',
                zoomIn: 'Acercar',
                zoomOut: 'Alejar',
                pan: 'Navegación',
                reset: 'Restablecer Zoom'
            }
        }
    };

    // ==========================================================================
    // SECCIÓN 2: NAVEGACIÓN
    // ==========================================================================

    const irADetalle = (categorias, entidad = null) => {
        // LEEMOS LAS FECHAS DIRECTAMENTE DE LOS INPUTS DEL DASHBOARD
        const fechaInicio = fechaInicioInput.value;
        const fechaFin = fechaFinInput.value;

        if (!fechaInicio || !fechaFin) {
            showNotification("Debes ejecutar un análisis primero para ver los detalles.", 'error');
            return;
        }

        // AÑADIMOS LAS FECHAS A LOS PARÁMETROS DE LA URL
        const params = new URLSearchParams({
            fecha_inicio: fechaInicio,
            fecha_fin: fechaFin,
            categorias: Array.isArray(categorias) ? categorias.join(',') : categorias
        });

        if(entidad){
            params.append('entidad', entidad);
        }
        // La URL de destino ahora contendrá todo el contexto necesario
        window.location.href = `details.php?${params.toString()}`;
    };

    // ==========================================================================
    // SECCIÓN 3: GESTIÓN DE GRÁFICOS (APEXCHARTS)
    // ==========================================================================

    const charts = {
        general: null,
        detalle: null,
        entidades: null,
        saldoEntidades: null,
        ingresos: null,

        init() {
            const saldoBarOptions = {
                series: [{ data: [] }], // Quitamos 'name'
                chart: {
                    type: 'bar',
                    height: 400,
                    events: {
                        // Este evento se dispara al hacer CLIC en una BARRA
                        dataPointSelection: (evt, chartCtx, config) => {
                            const entidadSeleccionada = config.w.globals.labels[config.dataPointIndex];
                            irADetalle(['T2', 'T3', 'T4', 'Mixtas'], entidadSeleccionada);
                        }
                    }
                },
                plotOptions: { 
                    bar: { 
                        borderRadius: 4, 
                        horizontal: false, 
                        columnWidth: '50%', 
                        distributed: true 
                    } 
                },
                // --- INICIO DE LA CORRECCIÓN DE LEYENDA ---
                legend: {
                    show: true,
                    position: 'bottom',
                    horizontalAlign: 'center',
                    itemMargin: { horizontal: 10, vertical: 5 },
                    // La misma configuración para activar el clic
                    onItemClick: {
                        toggleDataSeries: true
                    },
                    onItemHover: {
                        highlightDataSeries: false
                    }
                },
                // --- FIN DE LA CORRECCIÓN DE LEYENDA ---
                dataLabels: { enabled: false },
                xaxis: {
                    type: 'category',
                    labels: { rotate: -45, trim: true },
                    title: { text: 'Entidades' }
                },
                yaxis: {
                    title: { text: 'Saldo en Cartera' },
                    labels: { formatter: (val) => formatarMoneda(val) }
                },
                tooltip: { y: { formatter: (val) => formatarMoneda(val), title: { formatter: () => 'Saldo:' } } },
                noData: { text: 'Sin datos para mostrar.' }
            };

            const generalPieOptions = {
                series: [],
                labels: [],
                chart: {
                    type: 'donut',
                    height: 350,
                    events: {
                        dataPointSelection: (evt, chartCtx, config) => {
                            const label = config.w.config.labels[config.dataPointIndex].trim();
                            if (label === 'Radicadas') {
                                irADetalle('T1');
                            } else if (label === 'No Radicadas') {
                                irADetalle(['T2', 'T3', 'T4', 'Mixtas']);
                            }
                        }
                    }
                },
                legend: { position: 'bottom' },
                noData: { text: 'Selecciona un rango de fechas y ejecuta el análisis.' },
                plotOptions: { pie: { donut: { labels: { show: true, total: { show: true, label: 'Total glosas' } } } } },
                dataLabels: { formatter: (val) => `${val.toFixed(1)}%` },
            };

            const detallePieOptions = {
                series: [],
                labels: [],
                chart: {
                    type: 'donut',
                    height: 350,
                    events: {
                        dataPointSelection: (evt, chartCtx, config) => {
                            const label = config.w.config.labels[config.dataPointIndex];
                            const categoriaMap = {
                                'Con CC y Sin FR': 'T2',
                                'Sin CC y Sin FR': 'T3',
                                'Sin CC y Con FR': 'T4',
                                'Mixtas': 'Mixtas'
                            };
                            if (categoriaMap[label]) {
                                irADetalle(categoriaMap[label]);
                            }
                        }
                    }
                },
                legend: { position: 'bottom' },
                noData: { text: 'Sin datos para mostrar.' },
                plotOptions: { pie: { donut: { labels: { show: true, total: { show: true, label: 'Total' } } } } },
                dataLabels: { formatter: (val) => `${val.toFixed(1)}%` },
            };

            const barOptions = {
            series: [{ data: [] }], // Quitamos 'name'
            chart: { 
                type: 'bar', 
                height: 400, 
                toolbar: { show: false }, 
                events: {
                    // Este evento se dispara al hacer CLIC en una BARRA
                    dataPointSelection: (evt, chartCtx, config) => {
                        const entidadSeleccionada = config.w.config.series[0].data[config.dataPointIndex].x;
                        irADetalle(['T2', 'T3', 'T4', 'Mixtas'], entidadSeleccionada);
                    }
                }
            },
            plotOptions: { 
                bar: { 
                    borderRadius: 4, 
                    horizontal: true, 
                    distributed: true 
                } 
            },
            // --- INICIO DE LA CORRECCIÓN DE LEYENDA ---
            legend: {
                show: true,           
                position: 'bottom',    
                horizontalAlign: 'center', 
                itemMargin: { horizontal: 10, vertical: 5 },
                // Esta configuración es la clave:
                // le decimos a la leyenda que SU acción de clic es mostrar/ocultar
                onItemClick: {
                    toggleDataSeries: true
                },
                // Y le decimos que pasar el mouse por encima NO resalte las barras
                // para que no interfiera con el tooltip
                onItemHover: {
                    highlightDataSeries: false
                }
            },
            // --- FIN DE LA CORRECCIÓN DE LEYENDA ---
            dataLabels: { enabled: true, formatter: val => val.toLocaleString('es') },
            xaxis: { title: { text: 'Cantidad de glosas' } },
            tooltip: { y: { formatter: val => val.toLocaleString('es'), title: { formatter: () => 'Cantidad:' } } },
            noData: { text: 'Sin datos para mostrar.' },
        };

            const ingresosLineOptions = {
                series: [{ name: 'Glosas Ingresadas', data: [] }],
                chart: {
                    type: 'area',
                    height: 400,
                    locales: [apexChartsSpanishLocale],
                    defaultLocale: 'es',
                    zoom: { enabled: true },
                    toolbar: { show: true },
                },
                stroke: { curve: 'smooth', width: 2 },
                markers: { size: 4 },
                xaxis: {
                    type: 'datetime',
                    title: { text: 'Fecha de Notificación' },
                    labels: { datetimeUTC: false } 
                },
                yaxis: { min:0.5, title: { text: 'N° de Glosas' }, labels: { formatter: (val) => val.toFixed(0) } },
                tooltip: { x: { format: 'dd MMMM yyyy' } },
                noData: { text: 'Sin datos para mostrar.' }
            };

            this.general = new ApexCharts(document.querySelector("#chart-general"), generalPieOptions);
            this.detalle = new ApexCharts(document.querySelector("#chart-puras"), detallePieOptions);
            this.entidades = new ApexCharts(document.querySelector("#chart-entidades"), barOptions);
            this.saldoEntidades = new ApexCharts(document.querySelector("#chart-saldo-entidades"), saldoBarOptions);
            this.ingresos = new ApexCharts(document.querySelector("#chart-ingresos"), ingresosLineOptions);

            this.general.render();
            this.detalle.render();
            this.entidades.render();
            this.saldoEntidades.render();
            this.ingresos.render();
        },

        update(data) {
            const T1 = data.facturas_t1 || 0;
            const T2 = data.facturas_t2 || 0;
            const T3 = data.facturas_t3 || 0;
            const T4 = data.facturas_t4 || 0;
            const Mixtas = data.facturas_mixtas || 0;
            const TotalNoRadicadas = T2 + T3 + T4 + Mixtas;
            fullEntidadData = data.conteo_por_entidad || [];
            fullSaldoData = data.saldo_por_entidad_top10 || [];

            statTotal.textContent = (data.total_facturas_base || 0).toLocaleString('es');
            statRadicadas.textContent = T1.toLocaleString('es');
            statNoRadicadas.textContent = TotalNoRadicadas.toLocaleString('es');
            statValorTotal.textContent = formatarMoneda(data.valor_total_periodo);
            statValorRadicado.textContent = formatarMoneda(data.valor_total_radicado);
            statValorNoRadicado.textContent = formatarMoneda(data.valor_total_no_radicado);

            setupEntityFilter('conteo', fullEntidadData, modalFiltroConteoBody, this.entidades, 'total_facturas');
            setupEntityFilter('saldo', fullSaldoData, modalFiltroSaldoBody, this.saldoEntidades, 'total_saldo');
            
            // Habilitar los botones de filtro ahora que hay datos
            filtroConteoBtn.disabled = fullEntidadData.length === 0;
            filtroSaldoBtn.disabled = fullSaldoData.length === 0;

            // Actualizamos los gráficos con la vista inicial (todos seleccionados)
            this.entidades.updateSeries([{ data: formatChartData(fullEntidadData, 'nom_entidad', 'total_facturas') }]);
            this.saldoEntidades.updateSeries([{ data: formatChartData(fullSaldoData, 'nom_entidad', 'total_saldo') }]);

            this.general.updateOptions({
                series: [T1, TotalNoRadicadas],
                labels: [' Radicadas ', ' No Radicadas ']
            });

            const seriesDetalle = [T2, T3, T4, Mixtas];
            const labelsDetalle = ['Con CC y Sin FR', 'Sin CC y Sin FR', 'Sin CC y Con FR', 'Mixtas'];
            const finalSeries = [], finalLabels = [];
            
            seriesDetalle.forEach((val, i) => {
                if (val > 0) {
                    finalSeries.push(val);
                    finalLabels.push(labelsDetalle[i]);
                }
            });
            this.detalle.updateOptions({ series: finalSeries, labels: finalLabels });

            const entidadData = data.conteo_por_entidad || [];
            if (entidadData.length > 0) {
                const formattedEntidadData = entidadData.map(item => ({ x: item.nom_entidad, y: item.total_facturas }));
                this.entidades.updateSeries([{ data: formattedEntidadData }]);
            } else {
                this.entidades.updateSeries([{ data: [] }]);
            }

                // ===== LÓGICA DE ACTUALIZACIÓN DEL NUEVO GRÁFICO =====
            const saldoData = data.saldo_por_entidad_top10 || [];
            if (saldoData.length > 0) {
                // Formateamos los datos para que ApexCharts los entienda (x: etiqueta, y: valor)
                const formattedSaldoData = saldoData.map(item => ({
                    x: item.nom_entidad,
                    y: item.total_saldo
                }));
                this.saldoEntidades.updateSeries([{ data: formattedSaldoData }]);
            } else {
                this.saldoEntidades.updateSeries([{ data: [] }]);
            }

            const estatusData = data.conteo_por_estatus || [];
            if (estatusData.length > 0) {
                let tableHTML = `<div class="table-wrapper"><table class="estatus-table">
                    <thead><tr><th>Estatus</th><th>Total Ítems</th></tr></thead>
                    <tbody>`;
                estatusData.forEach(item => {
                    tableHTML += `<tr>
                        <td>${item.estatus1 || 'N/A'}</td>
                        <td>${(item.total_items || 0).toLocaleString('es')}</td>
                    </tr>`;
                });
                tableHTML += `</tbody></table></div>`;
                estatusTableContainer.innerHTML = tableHTML;
            } else {
                estatusTableContainer.innerHTML = `<div class="table-wrapper"><p>No hay ítems pendientes por radicar en este período.</p></div>`;
            }

            const ingresosData = data.ingresos_por_periodo || [];
            if (ingresosData.length > 0) {
                const seriesData = ingresosData.map(item => [new Date(item.fecha_agrupada).getTime(), item.conteo]);
                this.ingresos.updateSeries([{ data: seriesData }]);
                
                // Traducción de la granularidad
                let granularidadTraducida = data.granularidad_ingresos;
                if (granularidadTraducida === 'Year') granularidadTraducida = 'Anual';
                else if (granularidadTraducida === 'Month') granularidadTraducida = 'Mensual';
                else if (granularidadTraducida === 'Day') granularidadTraducida = 'Diario';

                ingresosChartTitle.textContent = `Ingreso de Glosas (${granularidadTraducida})`;
            } else {
                this.ingresos.updateSeries([{ data: [] }]);
                ingresosChartTitle.textContent = `Ingreso de Glosas`;
            }
        },
        
        hide() { mainGrid.style.visibility = 'hidden'; },
        show() { mainGrid.style.visibility = 'visible'; }
    };

    // ==========================================================================
    // SECCIÓN 4: LÓGICA DE INICIALIZACIÓN Y MANEJADORES DE EVENTOS
    // ==========================================================================

    async function initializeDashboard() {
        // Inicializar tooltips de Bootstrap
        const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]')
        const tooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl))

        const urlParams = new URLSearchParams(window.location.search);
        const fechaInicioUrl = urlParams.get('fecha_inicio');
        const fechaFinUrl = urlParams.get('fecha_fin');

        try {
            const result = await fetchApi('/reportes/rango-fechas');
            
            if (result.success && result.data.fecha_min && result.data.fecha_max) {
                fechaInicioInput.value = fechaInicioUrl || result.data.fecha_min;
                fechaFinInput.value = fechaFinUrl || result.data.fecha_max;
                
                fechaInicioInput.min = result.data.fecha_min;
                fechaFinInput.max = result.data.fecha_max;
                fechaInicioInput.max = result.data.fecha_max;
                fechaFinInput.min = result.data.fecha_min;
                
                dateFilter.style.visibility = 'visible';

                if (fechaInicioUrl && fechaFinUrl) {
                    runBtn.click();
                }
            } else {
                showNotification(result.message || 'No se pudo determinar el rango de fechas desde la BD.', 'error');
            }
        } catch (e) {
            showNotification(`Error de conexión al inicializar: ${e.message}`, 'error');
        }
    }

    runBtn.addEventListener('click', async () => {
        const fechaInicio = fechaInicioInput.value;
        const fechaFin = fechaFinInput.value;
        const btnLoader = runBtn.querySelector('.spinner-border');
        
        if (fechaInicio && fechaFin) {
                sessionStorage.setItem('lastDateRange', JSON.stringify({ fecha_inicio: fechaInicio, fecha_fin: fechaFin }));
            }

        runBtn.disabled = true;
        if (btnLoader) {
            btnLoader.style.display = 'inline-block';
        } else {
            console.error("Error: Spinner element not found for run-analysis-btn.");
        }
        runBtn.style.cursor = 'wait';
        downloadBtn.disabled = true;
        initialMessage.style.display = 'none';
        notificationArea.style.display = 'none';
        charts.hide(); // Oculta el grid de datos reales
        skeletonLoader.style.display = 'block'; // Muestra el esqueleto

        const params = new URLSearchParams({
            fecha_inicio: fechaInicioInput.value,
            fecha_fin: fechaFinInput.value
        });
        
        try {
            const result = await fetchApi(`/reportes/analizar-y-comprobar?${params.toString()}`);
            
            if (!result.success) {
                throw new Error(result.message);
            }
            if (result.data.error) {
                showNotification(result.data.error, 'info');
                skeletonLoader.style.display = 'none'; // Oculta el esqueleto si no hay datos
                return;
            }
            
            charts.update(result.data);
            charts.show();
            downloadBtn.disabled = false;

        } catch (e) {
            showNotification(e.message, 'error');

        } finally {
            runBtn.disabled = false;
            if (btnLoader) {
                btnLoader.style.display = 'none';
            }
            runBtn.style.cursor = 'pointer';
            skeletonLoader.style.display = 'none'; // Siempre oculta el esqueleto al final
        }
    });

    downloadBtn.addEventListener('click', async () => {
        downloadBtn.disabled = true;
        downloadBtn.textContent = 'Generando...';

        const params = new URLSearchParams({
            fecha_inicio: fechaInicioInput.value,
            fecha_fin: fechaFinInput.value
        });

        try {
            // Nota: fetchApi no funciona aquí porque necesitamos el blob, no el JSON
            const response = await fetch(`http://192.168.1.10:5000/api/reportes/descargar-excel?${params.toString()}`);
            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.message || `Error del servidor: HTTP ${response.status}`);
            }
            const blob = await response.blob();
            
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = `Reporte_Glosas_${fechaInicioInput.value}_a_${fechaFinInput.value}.xlsx`;
            
            document.body.appendChild(a);
            a.click();
            
            window.URL.revokeObjectURL(url);
            a.remove();

        } catch (e) {
            showNotification(`No se pudo descargar el archivo: ${e.message}`, 'error');

        } finally {
            downloadBtn.disabled = false;
            downloadBtn.textContent = 'Descargar Reporte';
        }
    });

    // --- 3. CREA DOS NUEVAS FUNCIONES AYUDANTES FUERA DEL OBJETO `charts` PERO DENTRO DEL EVENT LISTENER ---
    /** Formatea los datos de la API al formato {x, y} que ApexCharts necesita */
    const formatChartData = (data, xKey, yKey) => {
        return data.map(item => ({
            x: item[xKey],
            y: item[yKey]
        }));
    };

    /**
     * Configura un modal de filtro de entidades, sus checkboxes y eventos.
     * @param {string} type - 'conteo' o 'saldo' para diferenciar IDs.
     * @param {Array} data - La lista completa de entidades de la API.
     * @param {HTMLElement} modalBody - El elemento del DOM donde se inyectarán los checkboxes.
     * @param {ApexCharts.ApexOptions} chartInstance - La instancia del gráfico a actualizar.
     * @param {string} valueKey - La clave del valor a mostrar ('total_facturas' o 'total_saldo').
     */
    const setupEntityFilter = (type, data, modalBody, chartInstance, valueKey) => {
        if (!data || data.length === 0) {
            modalBody.innerHTML = '<p>No hay entidades para filtrar en este período.</p>';
            return;
        }

        // --- Referencias a los nuevos inputs ---
        const minInput = document.getElementById(`min-${type}-input`);
        const maxInput = document.getElementById(`max-${type}-input`);

        // --- Generar checkboxes (esto no cambia) ---
        let checkboxesHTML = data.map((entidad, index) => `
            <div class="form-check">
                <input class="form-check-input filter-checkbox-${type}" type="checkbox" value="${entidad.nom_entidad}" id="check-${type}-${index}" checked>
                <label class="form-check-label" for="check-${type}-${index}">
                    ${entidad.nom_entidad}
                </label>
            </div>
        `).join('');
        modalBody.innerHTML = checkboxesHTML;

    const formatCurrencyInput = (inputElement) => {
        // Obtenemos el valor y quitamos todo lo que no sea un dígito
        let value = inputElement.value.replace(/[^\d]/g, '');
        if (value) {
            // Convertimos a número y usamos tu función existente para formatear
            const numberValue = parseInt(value, 10);
            inputElement.value = formatarMoneda(numberValue);
        } else {
            inputElement.value = '';
        }
    };

     // --- FUNCIÓN CENTRAL DE FILTRADO (AHORA ES MÁS INTELIGENTE) ---
    const applyAllFilters = () => {
        // Leemos los valores numéricos SIEMPRE
        const minVal = parseFloat(minInput.value.replace(/[^\d]/g, '')) || 0;
        const maxVal = parseFloat(maxInput.value.replace(/[^\d]/g, '')) || Infinity;
        
        // Leemos los checkboxes marcados SIEMPRE
        const selectedEntities = Array.from(document.querySelectorAll(`.filter-checkbox-${type}:checked`))
                                    .map(cb => cb.value);

        // Aplicamos AMBOS filtros a la vez
        const filteredData = data.filter(item => {
            const isSelectedByCheckbox = selectedEntities.includes(item.nom_entidad);
            const value = item[valueKey];
            const isInValueRange = value >= minVal && value <= maxVal;
            
            // La entidad debe cumplir ambas condiciones para ser mostrada
            return isSelectedByCheckbox && isInValueRange;
        });
        
        chartInstance.updateSeries([{ data: formatChartData(filteredData, 'nom_entidad', valueKey) }]);
    };
    
    // Asignamos applyAllFilters a TODOS los eventos de cambio
    document.querySelectorAll(`.filter-checkbox-${type}`).forEach(checkbox => {
        checkbox.addEventListener('change', applyAllFilters);
    });
    minInput.addEventListener('input', applyAllFilters);
    maxInput.addEventListener('input', applyAllFilters);

    // Los botones de "Seleccionar Todo" también deben usar la función central
    document.getElementById(`seleccionar-todas-${type}`).onclick = () => {
        document.querySelectorAll(`.filter-checkbox-${type}`).forEach(cb => cb.checked = true);
        applyAllFilters();
    };
    document.getElementById(`deseleccionar-todas-${type}`).onclick = () => {
        document.querySelectorAll(`.filter-checkbox-${type}`).forEach(cb => cb.checked = false);
        applyAllFilters();
    };

    // --- NUEVOS EVENTOS PARA EL FORMATEO VISUAL ---
    // Si el filtro es de tipo 'saldo', aplicamos el formateo de moneda en tiempo real
    if (type === 'saldo') {
        minInput.addEventListener('keyup', () => formatCurrencyInput(minInput));
        maxInput.addEventListener('keyup', () => formatCurrencyInput(maxInput));
    }};

    ejecutarBusquedaBtn.addEventListener('click', () => {
        const texto = busquedaTextarea.value.trim();
        if (!texto) {
            showNotification('El campo de búsqueda está vacío.', 'error');
            return;
        }

        const ids = texto.split(/\r?\n/).filter(line => line.trim() !== '');
        
        const modalElement = document.getElementById('modal-busqueda-facturas');
        const modal = bootstrap.Modal.getInstance(modalElement);
        if (modal) {
            modal.hide();
        } else {
            console.warn("Modal de búsqueda no encontrado para cerrar.");
        }


        const searchData = {
            ids: ids,
            // Agrega esta información para el título de la página de resultados
            searchType: 'Factura Completa' 
        };
        sessionStorage.setItem('searchRequest', JSON.stringify(searchData));
        
        window.location.href = 'search-details.php';
    });

    charts.init();
    initializeDashboard();
});