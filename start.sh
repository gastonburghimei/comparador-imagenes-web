#!/bin/bash
echo "🚀 Iniciando Comparador de Imágenes Web en puerto $PORT..."
exec gunicorn --bind 0.0.0.0:$PORT app_web:app --workers 1 --timeout 60 --preload 