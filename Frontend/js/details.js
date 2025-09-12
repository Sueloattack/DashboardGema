/* ==========================================================================
   js/details.js - Lógica de la Vista de Detalle de Facturas
   --------------------------------------------------------------------------
   Este script gestiona la página de detalles, que muestra una tabla paginada
   de facturas para una o más categorías específicas.
   ========================================================================== */

import { fetchApi } from './api.js';
import { showNotification, formatarMoneda, formatearFecha } from './utils.js';


document.addEventListener('DOMContentLoaded', () => {

    // ==========================================================================
    // SECCIÓN 1: REFERENCIAS AL DOM Y ESTADO
    // ==========================================================================
    const titleElement = document.getElementById('details-title');
    const backLink = document.getElementById('back-link');
    const skeletonTableLoader = document.getElementById('skeleton-table-loader');
    const notificationArea = document.getElementById('notification-area');
    const detailsMainContent = document.getElementById('details-main-content');
    const tableContainer = document.getElementById('data-table');
    const paginationContainer = document.getElementById('pagination-container');
    const saldoContainer = document.getElementById('saldo-acumulado-container');
    const saldoValorElement = document.getElementById('saldo-acumulado-valor');
    
    
    let currentPage = 1;
    let totalPages = 1;
    const urlParams = new URLSearchParams(window.location.search);
    
    const categoriaTitulos = {
        'T1': 'Glosas radicadas',
        'T2': 'Glosas con Cuenta de Cobro y sin Fecha de Radicado',
        'T3': 'Glosas sin Cuenta de Cobro y sin Fecha de Radicado',
        'T4': 'Glosas sin Cuenta de Cobro y con Fecha de Radicado',
        'Mixtas': 'Glosas mixtas',
        'T2,T3,T4,Mixtas': 'Todas las glosas no radicadas'
    };

    // ==========================================================================
    // SECCIÓN 2: FUNCIONES DE UTILIDAD Y FORMATEO
    // ==========================================================================

    /**
     * Capitaliza la primera letra de una cadena y el resto en minúsculas.
     * @param {string} str - La cadena a formatear.
     * @returns {string} La cadena formateada.
     */
    function capitalizeFirstLetter(str) {
        if (!str) return '';
        return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
    }

    // ==========================================================================
    // SECCIÓN 3: LÓGICA DE CARGA Y RENDERIZADO
    // ==========================================================================

    async function loadResumenes(page = 1) {
        skeletonTableLoader.style.display = 'block';
        detailsMainContent.style.display = 'none';
        notificationArea.style.display = 'none';
        saldoContainer.style.visibility = 'hidden';
        
        const urlParams = new URLSearchParams(window.location.search);
        const fechaInicio = urlParams.get('fecha_inicio');
        const fechaFin = urlParams.get('fecha_fin');
        const categorias = urlParams.get('categorias');
        const entidad = urlParams.get('entidad');
        
    // ========= INICIO DE LA LÓGICA CORREGIDA PARA "VOLVER AL DASHBOARD" =========
        // 2. Construimos el enlace de vuelta usando las fechas de la URL.
        if (fechaInicio && fechaFin) {
            backLink.href = `./test.php?fecha_inicio=${fechaInicio}&fecha_fin=${fechaFin}`;
        } else {
            // Si por alguna razón no hay fechas, vuelve al dashboard sin filtros.
            backLink.href = './test.php';
        }
    // =========================================================================

        if (!fechaInicio || !fechaFin || !categorias) {
            showNotification('Faltan parámetros en la URL.', 'error');
            skeletonTableLoader.style.display = 'none';
            return;
        }
        
        const tituloBase = categoriaTitulos[categorias] || categorias;
        let tituloCargando = `Cargando detalle: ${capitalizeFirstLetter(tituloBase)}`;
        if (entidad) {
            tituloCargando += ` para ${capitalizeFirstLetter(entidad)}`;
        }
        titleElement.textContent = tituloCargando;

        try {
            const params = new URLSearchParams({ fecha_inicio: fechaInicio, fecha_fin: fechaFin, categorias, pagina: page });
            if (entidad) {
                params.append('entidad', entidad);
            }
            
            const result = await fetchApi(`/reportes/resumenes-paginados?${params.toString()}`);
            
            if (!result.success) throw new Error(result.message);
            
            const { data, pagina_actual, total_paginas, total_registros, saldo_total_acumulado } = result.data;
            currentPage = pagina_actual;
            totalPages = total_paginas;

            let tituloFinal = `Detalle: ${capitalizeFirstLetter(tituloBase)} (${total_registros.toLocaleString('es')} glosas)`;
            if (entidad) {
                tituloFinal += ` para ${capitalizeFirstLetter(entidad)}`;
            }
            titleElement.textContent = tituloFinal;

            if (saldo_total_acumulado !== undefined) {
                saldoValorElement.textContent = formatarMoneda(saldo_total_acumulado);
                saldoContainer.style.visibility = 'visible'; // Lo hacemos visible
            }

            if (total_registros === 0) {
                showNotification('No se encontraron glosas para los filtros seleccionados.', 'info');
                skeletonTableLoader.style.display = 'none';
            } else {
                renderTable(data);
                renderPagination();
                detailsMainContent.style.display = 'block';
            }
        } catch (error) {
            titleElement.textContent = `Error al cargar detalles`;
            showNotification(error.message, 'error');
        } finally {
            skeletonTableLoader.style.display = 'none';
        }
    }

    function renderTable(resumenes) {
        const resumenHeaderMap = {
            "FACTURA": "Factura", "gl_docn": "No. Paciente", "nom_entidad": "Entidad",
            "fechanotificacion": "Fecha de notificación", "fecha_gl": "Fecha de objeción", "freg": "Fecha de contestación",
            "vr_glosa": "Saldo de glosa en cartera", "tipo": "Tipo", "Total_Items_Factura": "Total Ítems",
            "Items_ConCC_ConFR": "CC y FR", "Items_ConCC_SinFR": "Solo CC",
            "Items_SinCC_ConFR": "Sin CC y con FR", "Items_SinCC_SinFR": "Sin CC ni FR"
        };
        const resumenHeaderKeys = Object.keys(resumenHeaderMap);

        let tableHTML = `<table class="table table-striped table-hover details-table">
            <thead>
                <tr>
                    <th></th>
                    ${resumenHeaderKeys.map(key => `<th>${resumenHeaderMap[key]}</th>`).join('')}
                </tr>
            </thead>
            <tbody>`;

        resumenes.forEach(resumen => {
            const docn = resumen['gl_docn'];
            tableHTML += `<tr class="resumen-row" data-docn="${docn}">
                <td><span class="toggle-icon">▸</span></td>`;

            resumenHeaderKeys.forEach(headerKey => {
                let value = resumen[headerKey];
                if (['fechanotificacion', 'fecha_gl', 'freg'].includes(headerKey)) {
                    value = formatearFecha(value);
                } else if (headerKey === 'vr_glosa') {
                    value = formatarMoneda(value);
                }
                const cellClass = headerKey === 'nom_entidad' ? 'class="cell-entidad"' : '';
                tableHTML += `<td ${cellClass}>${value ?? ''}</td>`;
            });
            tableHTML += `</tr>`;
        });

        tableHTML += `</tbody></table>`;
        tableContainer.innerHTML = tableHTML;
        
        document.querySelectorAll('.resumen-row').forEach(row => row.addEventListener('click', toggleDetalle));
    }
    
    async function toggleDetalle(event) {
        const filaResumen = event.currentTarget;
        const toggleIcon = filaResumen.querySelector('.toggle-icon');
        const docn = filaResumen.dataset.docn;
        const isAlreadyOpen = filaResumen.classList.contains('open');

        const itemAbierto = document.querySelector('.detalle-container-row');
        if (itemAbierto) {
            itemAbierto.previousElementSibling.classList.remove('open');
            itemAbierto.previousElementSibling.querySelector('.toggle-icon').textContent = '▸';
            itemAbierto.remove();
        }

        if (isAlreadyOpen) {
            filaResumen.classList.remove('open');
            toggleIcon.textContent = '▸';
            return;
        }
        
        filaResumen.classList.add('open');
        toggleIcon.textContent = '▾';

        const colspan = filaResumen.cells.length;
        const detalleRow = filaResumen.parentElement.insertRow(filaResumen.rowIndex);
        detalleRow.className = 'detalle-container-row';

        const detalleCell = detalleRow.insertCell(0);
        detalleCell.colSpan = colspan;
        detalleCell.innerHTML = `<div class="detalle-wrapper" style="padding: 2rem; text-align: center;">Cargando ítems...</div>`;

        if (!docn) {
            detalleCell.innerHTML = `<div class="detalle-wrapper error-message">Error: Falta ID para cargar el detalle.</div>`;
            return;
        }

        try {
            const params = new URLSearchParams({ docn });
            const result = await fetchApi(`/reportes/detalle-factura?${params.toString()}`);
            if (!result.success) throw new Error(result.message);
            
            let detailContentHTML = '';

            if (result.data && result.data.length > 0) {
                const itemHeaderMap = {
                    "FACTURA": "Factura", "gl_docn": "No. Paciente", "nom_entidad": "Entidad", 
                    "fechanotificacion": "Fecha de notificación", "fecha_gl": "Fecha de objeción", "freg": "Fecha de contestación", 
                    "gr_docn": "Cuenta de cobro", "fecha_rep": "Fecha de radicado", "estatus1": "Estatus", 
                    "vr_glosa": "Valor Glosa", "tipo": "Tipo"
                };
                
                let itemsHTML = '<div class="detalle-wrapper"><table class="table table-striped table-hover detail-inner-table"><thead><tr>';
                Object.values(itemHeaderMap).forEach(title => itemsHTML += `<th>${title}</th>`);
                itemsHTML += '</tr></thead><tbody>';
                
                result.data.forEach(item => {
                    let rowClass = '';
                    // Lógica para aplicar clases condicionales
                    const tieneFechaRadicado = item.fecha_rep !== null && item.fecha_rep !== undefined && item.fecha_rep !== '';
                    const tieneCuentaCobro = item.gr_docn !== null && item.gr_docn !== undefined && item.gr_docn !== 0;

                    if (tieneFechaRadicado) {
                        rowClass = 'status-radicado';
                    } else if (tieneCuentaCobro) {
                        rowClass = 'status-con-cc';
                    } else {
                        rowClass = 'status-error';
                    }

                    itemsHTML += `<tr class="${rowClass}">`;
                    Object.keys(itemHeaderMap).forEach(headerKey => {
                        let value = item[headerKey];
                        if (['fechanotificacion', 'fecha_gl', 'freg', 'fecha_rep'].includes(headerKey)) {
                            value = formatearFecha(value);
                        } else if (headerKey === 'vr_glosa') {
                            value = formatarMoneda(value);
                        }
                        itemsHTML += `<td>${value ?? ''}</td>`;
                    });
                    itemsHTML += '</tr>';
                });
                itemsHTML += '</tbody></table></div>';
                
                detailContentHTML = itemsHTML;
            } else {
                detailContentHTML = '<div class="detalle-wrapper no-detail-message">No se encontraron ítems de detalle para esta factura.</div>';
            }

            detalleCell.innerHTML = detailContentHTML;

        } catch (error) {
            detalleCell.innerHTML = `<div class="detalle-wrapper error-message">Error al cargar detalles: ${error.message}</div>`;
        }
    }
    
    function renderPagination() {
        if (totalPages <= 1) {
            paginationContainer.innerHTML = '';
            return;
        }
        
        let paginationHTML = `
            <button id="prev-page" class="btn btn-outline-secondary" ${currentPage === 1 ? 'disabled' : ''}>Anterior</button>
            <span class="pagination-info mx-2">Página ${currentPage} de ${totalPages}</span>
            <button id="next-page" class="btn btn-outline-secondary" ${currentPage === totalPages ? 'disabled' : ''}>Siguiente</button>
        `;
        paginationContainer.innerHTML = paginationHTML;

        document.getElementById('prev-page').addEventListener('click', () => loadResumenes(currentPage - 1));
        document.getElementById('next-page').addEventListener('click', () => loadResumenes(currentPage + 1));
    }
    
    // ==========================================================================
    // SECCIÓN 4: PUNTO DE ENTRADA
    // ==========================================================================

    loadResumenes(currentPage);
});