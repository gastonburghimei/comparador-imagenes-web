#!/bin/bash
set -e
echo "📦 Instalando dependencias para Comparador de Imágenes..."
python -m pip install --upgrade pip
pip install -r requirements_web.txt
echo "✅ Dependencias instaladas correctamente" 