// js/search-details.js
// VERSIÓN FINAL: Unificada con la lógica de details.js

import { fetchApi } from './api.js';
import { showNotification, formatarMoneda, formatearFecha } from './utils.js';

document.addEventListener('DOMContentLoaded', () => {
    // --- Referencias al DOM ---
    const titleElement = document.getElementById('details-title');
    const backLink = document.getElementById('back-link');
    const saldoContainer = document.getElementById('saldo-acumulado-container');
    const saldoValorElement = document.getElementById('saldo-acumulado-valor');
    const tableContainer = document.getElementById('data-table');
    const mainContent = document.getElementById('details-main-content');
    const skeletonLoader = document.getElementById('skeleton-table-loader');

    // --- Configuración Botón "Volver" ---
    // --- LÓGICA FINAL Y ROBUSTA PARA EL BOTÓN "VOLVER" ---
    try {
        const lastDateRangeRaw = sessionStorage.getItem('lastDateRange');
        if (lastDateRangeRaw) {
            // Intentamos parsear. Si falla, el catch lo manejará.
            const { fecha_inicio, fecha_fin } = JSON.parse(lastDateRangeRaw);
            // Si las fechas existen, construimos el enlace.
            if (fecha_inicio && fecha_fin) {
                backLink.href = `./test.php?fecha_inicio=${fecha_inicio}&fecha_fin=${fecha_fin}`;
            } else {
                // Si el objeto está mal formado (sin fechas), volvemos sin parámetros.
                backLink.href = './test.php';
            }
        } else {
            // Si no hay nada en sessionStorage, volvemos sin parámetros.
            backLink.href = './test.php';
        }
    } catch (e) {
        console.error("Error al leer el rango de fechas del sessionStorage:", e);
        // Si hay cualquier error, volvemos sin parámetros como medida de seguridad.
        backLink.href = './test.php';
    }

    // --- ALMACENAR DATOS GLOBALES (para el acordeón) ---
    let fullDataFromAPI = [];

    // --- Funciones de Renderizado (AHORA IGUAL A DETAILS.JS) ---
    function renderTable(resumenes) {
        // ESTE MAPEO AHORA ES IDÉNTICO AL DE DETAILS.JS
        const resumenHeaderMap = {
            "FACTURA": "Factura", "gl_docn": "No. Paciente", "nom_entidad": "Entidad",
            "fechanotificacion": "Fecha de notificación", "fecha_gl": "Fecha de objeción", "freg": "Fecha de contestación",
            "vr_glosa": "Saldo de glosa en cartera", "tipo": "Tipo", "Total_Items_Factura": "Total Ítems",
            "Items_ConCC_ConFR": "CC y FR", "Items_ConCC_SinFR": "Solo CC",
            "Items_SinCC_ConFR": "Sin CC y con FR", "Items_SinCC_SinFR": "Sin CC ni FR"
        };
        const resumenHeaderKeys = Object.keys(resumenHeaderMap);

        let tableHTML = `<table class="table table-striped table-hover details-table"><thead><tr>
            <th></th>
            ${resumenHeaderKeys.map(key => `<th>${resumenHeaderMap[key]}</th>`).join('')}
        </tr></thead><tbody>`;

        resumenes.forEach(resumen => {
            const docn = resumen['gl_docn'];
            tableHTML += `<tr class="resumen-row" style="cursor:pointer;" data-docn="${docn}">
                <td><span class="toggle-icon">▸</span></td>`;
            resumenHeaderKeys.forEach(headerKey => {
                let value = resumen[headerKey];
                if (['fechanotificacion', 'fecha_gl', 'freg'].includes(headerKey)) {
                    value = formatearFecha(value);
                } else if (headerKey === 'vr_glosa') {
                    value = formatarMoneda(value);
                }
                tableHTML += `<td>${value ?? ''}</td>`;
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

        // --- LÓGICA ANTI-ERROR `IndexSizeError` ---
        // Buscamos si ya existe una fila de detalle y la eliminamos
        const itemAbierto = document.querySelector('.detalle-container-row');
        if (itemAbierto) {
            const filaPadreAbierta = itemAbierto.previousElementSibling;
            if (filaPadreAbierta) {
                filaPadreAbierta.classList.remove('open');
                filaPadreAbierta.querySelector('.toggle-icon').textContent = '▸';
            }
            itemAbierto.remove();
        }

        if (isAlreadyOpen) { // Si el clic fue en la que ya estaba abierta, solo la cerramos
            return;
        }
        
        filaResumen.classList.add('open');
        toggleIcon.textContent = '▾';

        const colspan = filaResumen.cells.length;
        // La inserción es después de la fila actual. Esto es clave.
        const detalleRow = filaResumen.parentElement.insertRow(filaResumen.rowIndex);
        detalleRow.className = 'detalle-container-row';
        const detalleCell = detalleRow.insertCell(0);
        detalleCell.colSpan = colspan;
        
        // Obtenemos los detalles de la variable global
        const detallesData = fullDataFromAPI.filter(item => item.TipoFila === 'Detalle Ítem' && item.gl_docn === parseInt(docn));
        
        if (detallesData.length > 0) {
            // Este mapeo ahora es IDÉNTICO al de details.js
            const itemHeaderMap = {
                "FACTURA": "Factura", 
                "gl_docn": "No. Paciente",
                "nom_entidad": "Entidad", 
                "fechanotificacion": "Fecha de notificación", 
                "fecha_gl": "Fecha de objeción", 
                "freg": "Fecha de contestación", 
                "gr_docn": "Cuenta de cobro", 
                "fecha_rep": "Fecha de radicado", 
                "estatus1": "Estatus", 
                "vr_glosa": "Valor Glosa", 
                "tipo": "Tipo"
            };
            
            let itemsHTML = '<div class="detalle-wrapper"><table class="table table-striped table-hover detail-inner-table"><thead><tr>';
            Object.values(itemHeaderMap).forEach(title => itemsHTML += `<th>${title}</th>`);
            itemsHTML += '</tr></thead><tbody>';
            
            detallesData.forEach(item => {
                const tieneFechaRadicado = !!item.fecha_rep;
                const tieneCuentaCobro = !!item.gr_docn;
                const rowClass = tieneFechaRadicado ? 'status-radicado' : (tieneCuentaCobro ? 'status-con-cc' : 'status-error');
                
                itemsHTML += `<tr class="${rowClass}">`;
                Object.keys(itemHeaderMap).forEach(headerKey => {
                    let value = item[headerKey];
                    const columnasDeFecha = ['fechanotificacion', 'fecha_gl', 'freg', 'fecha_rep'];
                    if (columnasDeFecha.includes(headerKey)) {
                        value = formatearFecha(value); 
                    } 
                    else if (headerKey === 'vr_glosa') { 
                        value = formatarMoneda(value); 
                    }
                    itemsHTML += `<td>${value ?? ''}</td>`;
                });
                itemsHTML += '</tr>';
            });
            itemsHTML += '</tbody></table></div>';
            detalleCell.innerHTML = itemsHTML;
        } else {
            detalleCell.innerHTML = '<div class="detalle-wrapper no-detail-message p-3 text-center">No se encontraron ítems de detalle.</div>';
        }
    }
    
    // --- FUNCIÓN PRINCIPAL DE CARGA ---
    const loadSearchResults = async () => {
        const searchRequestRaw = sessionStorage.getItem('searchRequest');
        if (!searchRequestRaw) { /* ... (manejo de no búsqueda) ... */ return; }
        
        const searchRequest = JSON.parse(searchRequestRaw);
        sessionStorage.removeItem('searchRequest');
        
        mainContent.style.display = 'none';
        skeletonLoader.style.display = 'block';

        try {
            const result = await fetchApi('/reportes/buscar-facturas', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ids: searchRequest.ids })
            });
            
            if (!result.success) throw new Error(result.message);
            const { encontrados, no_encontrados, saldo_total_acumulado } = result.data;
            fullDataFromAPI = encontrados;

            const resumenesEncontrados = encontrados.filter(r => r.TipoFila === 'Resumen Factura');
            const numFacturasEncontradas = resumenesEncontrados.length;
            
            titleElement.textContent = `Resultados de la Búsqueda (${numFacturasEncontradas} facturas)`;
            saldoValorElement.textContent = formatarMoneda(saldo_total_acumulado);
            saldoContainer.style.visibility = 'visible';
            
            if (no_encontrados.length > 0) {
                showNotification(`No se encontraron ${no_encontrados.length} facturas: ${no_encontrados.join(', ')}`, 'warning');
            } else if (numFacturasEncontradas > 0) {
                 showNotification(`Todas las facturas buscadas fueron encontradas.`, 'success');
            }

            if(numFacturasEncontradas > 0) {
                renderTable(resumenesEncontrados);
                mainContent.style.display = 'block';
            } else {
                 showNotification('No se encontraron facturas con los IDs proporcionados.', 'info');
            }

        } catch (error) {
            titleElement.textContent = 'Error en la Búsqueda';
            showNotification(error.message, 'error');
            mainContent.style.display = 'none';
        } finally {
            skeletonLoader.style.display = 'none';
        }
    };
    
    // Iniciar el proceso
    loadSearchResults();
});