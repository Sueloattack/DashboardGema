# extensions.py
# Este archivo centraliza las extensiones de Flask para evitar importaciones circulares.

from flask_caching import Cache

# Se crea la instancia de la caché aquí, pero se inicializa en app.py
cache = Cache()
