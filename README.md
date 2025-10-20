# Comparador de Imágenes ML

Aplicación web para comparar imágenes con dos modos:
- **Evaluar Condición Médica**: Detecta diferencias entre ceguera real y cierre voluntario de ojos
- **Comparar Fondos**: Analiza similitud entre fondos de imágenes

## Tecnologías
- Flask + Python
- Machine Learning (RandomForest)
- Procesamiento de imágenes (PIL, OpenCV)
- Análisis médico especializado

## API
```
POST /api/compare-images
Content-Type: multipart/form-data

Parámetros:
- image1: archivo
- image2: archivo  
- mode: 'background' | 'disease'
```

## Deploy
Configurado para Render con Gunicorn
