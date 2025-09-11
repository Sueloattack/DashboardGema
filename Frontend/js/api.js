/* ==========================================================================
   js/api.js - Módulo de Comunicación con la API
   --------------------------------------------------------------------------
   Este módulo centraliza todas las interacciones con el backend.
   - Define la URL base de la API.
   - Proporciona una función `fetchApi` para realizar peticiones fetch
     con un manejo de errores estandarizado.
   ========================================================================== */

/** URL base de la API del backend para centralizar las llamadas. */
const API_BASE_URL = 'http://192.168.1.10:5000/api';

/**
 * Centraliza las llamadas a la API usando fetch, manejando errores de red y respuestas no exitosas.
 * @param {string} endpoint - La ruta de la API a la que llamar (ej. '/reportes/rango-fechas').
 * @param {object} [options={}] - Opciones para la petición fetch (método, cabeceras, etc.).
 * @returns {Promise<any>} Los datos JSON de la respuesta si fue exitosa.
 * @throws {Error} Lanza un error si la conexión falla o la respuesta no es OK.
 */
export async function fetchApi(endpoint, options = {}) {
    try {
        const response = await fetch(`${API_BASE_URL}${endpoint}`, options);
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            const errorMessage = errorData.message || `Error del servidor: HTTP ${response.status}`;
            throw new Error(errorMessage);
        }
        return response.json(); // Devuelve directamente el JSON
    } catch (error) {
        console.error("Error en fetchApi:", error);
        throw new Error(error.message || "No se pudo conectar con el servidor backend.");
    }
}
