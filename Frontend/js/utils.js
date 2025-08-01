/* ==========================================================================
   js/utils.js - Módulo de Funciones de Utilidad
   --------------------------------------------------------------------------
   Este módulo contiene funciones reutilizables en toda la aplicación.
   - Formateo de datos (moneda, fechas).
   - Notificaciones al usuario.
   ========================================================================== */

/**
 * Muestra un mensaje al usuario en el área de notificaciones.
 * @param {string} message - El mensaje a mostrar.
 * @param {'info' | 'error' | 'success'} [type='info'] - El tipo de notificación.
 */
export function showNotification(message, type = 'info') {
    const notificationArea = document.getElementById('notification-area');
    if (!notificationArea) return;

    notificationArea.textContent = message;
    notificationArea.className = `notification-container ${type}`;
    notificationArea.style.display = 'block';
}

/**
 * Formatea un número como moneda colombiana (COP) sin decimales.
 * @param {number | string | null | undefined} number - El número a formatear.
 * @returns {string} El número formateado como un string (ej. '$1.234.567' o '$0').
 */
export const formatarMoneda = (number) => {
    if (number === null || number === undefined || number === '') {
        return '$0';
    }
    return `$${Number(number).toLocaleString('es-CO', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`;
};

/**
 * Formatea un string de fecha (ISO 8601) a formato DD/MM/AAAA.
 * @param {string | null} dateString - El string de la fecha a formatear.
 * @returns {string} La fecha formateada o una cadena vacía si la entrada es nula.
 */
export function formatearFecha(dateString) {
    if (!dateString) return '';
    const date = new Date(dateString);
    
    if (isNaN(date.getTime())) {
        return dateString;
    }
    
    const dia = String(date.getUTCDate()).padStart(2, '0');
    const mes = String(date.getUTCMonth() + 1).padStart(2, '0');
    const anio = date.getUTCFullYear();
    
    return `${dia}/${mes}/${anio}`;
}
