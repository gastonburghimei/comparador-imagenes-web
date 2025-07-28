#!/bin/bash
echo "🚀 Iniciando Comparador de Imágenes Web..."
gunicorn --bind 0.0.0.0:$PORT app_web:app --workers 2 --timeout 30 