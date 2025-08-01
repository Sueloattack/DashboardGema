/* ==========================================================================
   js/app.js - Lógica Principal del Dashboard de Análisis de Glosas
   --------------------------------------------------------------------------
   Este script gestiona la interactividad del dashboard principal.
   ========================================================================== */

import { fetchApi } from './api.js';
import { showNotification, formatarMoneda } from './utils.js';


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

    const statTotal = document.getElementById('stat-total');
    const statValorTotal = document.getElementById('stat-valor-total');
    const statRadicadas = document.getElementById('stat-radicadas');
    const statValorRadicado = document.getElementById('stat-valor-radicado');
    const statNoRadicadas = document.getElementById('stat-no-radicadas');
    const statValorNoRadicado = document.getElementById('stat-valor-no-radicado');
    const estatusTableContainer = document.getElementById('estatus-table-container');
    const ingresosChartTitle = document.getElementById('ingresos-chart-title');

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
        const fechaInicio = fechaInicioInput.value;
        const fechaFin = fechaFinInput.value;

        if (!fechaInicio || !fechaFin) {
            showNotification("Debes ejecutar un análisis primero para ver los detalles.", 'error');
            return;
        }

        const params = new URLSearchParams({
            fecha_inicio: fechaInicio,
            fecha_fin: fechaFin,
            categorias: Array.isArray(categorias) ? categorias.join(',') : categorias
        });

        if(entidad){
            params.append('entidad', entidad);
        }
        window.location.href = `details.php?${params.toString()}`;
    };

    // ==========================================================================
    // SECCIÓN 3: GESTIÓN DE GRÁFICOS (APEXCHARTS)
    // ==========================================================================

    const charts = {
        general: null,
        detalle: null,
        entidades: null,
        ingresos: null,

        init() {
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
                plotOptions: { pie: { donut: { labels: { show: true, total: { show: true, label: 'Total Facturas' } } } } },
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
                series: [{ name: 'N° Facturas', data: [] }],
                chart: { 
                    type: 'bar', 
                    height: 400, 
                    toolbar: { show: false }, 
                    events: {
                        dataPointSelection: (evt, chartCtx, config) => {
                            const entidadSeleccionada = config.w.config.series[0].data[config.dataPointIndex].x;
                            irADetalle(['T2', 'T3', 'T4', 'Mixtas'], entidadSeleccionada);
                        }
                    }
                },
                plotOptions: { bar: { borderRadius: 4, horizontal: true } },
                dataLabels: { enabled: true, formatter: val => val.toLocaleString('es') },
                xaxis: { title: { text: 'Cantidad de Facturas' } },
                noData: { text: 'Sin datos para mostrar.' },
                tooltip: { y: { formatter: val => val.toLocaleString('es') } },
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
                yaxis: { min:0.5, title: { text: 'N° de Facturas' }, labels: { formatter: (val) => val.toFixed(0) } },
                tooltip: { x: { format: 'dd MMMM yyyy' } },
                noData: { text: 'Sin datos para mostrar.' }
            };

            this.general = new ApexCharts(document.querySelector("#chart-general"), generalPieOptions);
            this.detalle = new ApexCharts(document.querySelector("#chart-puras"), detallePieOptions);
            this.entidades = new ApexCharts(document.querySelector("#chart-entidades"), barOptions);
            this.ingresos = new ApexCharts(document.querySelector("#chart-ingresos"), ingresosLineOptions);

            this.general.render();
            this.detalle.render();
            this.entidades.render();
            this.ingresos.render();
        },

        update(data) {
            const T1 = data.facturas_t1 || 0;
            const T2 = data.facturas_t2 || 0;
            const T3 = data.facturas_t3 || 0;
            const T4 = data.facturas_t4 || 0;
            const Mixtas = data.facturas_mixtas || 0;
            const TotalNoRadicadas = T2 + T3 + T4 + Mixtas;

            statTotal.textContent = (data.total_facturas_base || 0).toLocaleString('es');
            statRadicadas.textContent = T1.toLocaleString('es');
            statNoRadicadas.textContent = TotalNoRadicadas.toLocaleString('es');
            statValorTotal.textContent = formatarMoneda(data.valor_total_periodo);
            statValorRadicado.textContent = formatarMoneda(data.valor_total_radicado);
            statValorNoRadicado.textContent = formatarMoneda(data.valor_total_no_radicado);

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
        const btnLoader = runBtn.querySelector('.spinner-border');
        
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
            const response = await fetch(`http://127.0.0.1:5000/api/reportes/descargar-excel?${params.toString()}`);
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

    charts.init();
    initializeDashboard();
});