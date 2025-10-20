# 🖼️ Comparador de Fondos de Imágenes

Una plataforma avanzada de visión artificial que permite comparar fondos de imágenes y determinar si son similares o idénticos.

## ✨ Características

- **Interfaz moderna y responsive**: Diseño intuitivo con drag & drop
- **Algoritmos avanzados**: Usa técnicas de segmentación y machine learning
- **Análisis detallado**: Compara color, textura, estructura y forma
- **Resultados visuales**: Muestra los fondos extraídos y métricas detalladas
- **Procesamiento rápido**: Optimizado para imágenes de hasta 10MB

## 🚀 Tecnologías Utilizadas

### Frontend
- **HTML5/CSS3**: Interfaz moderna con CSS Grid y Flexbox
- **JavaScript ES6+**: Funcionalidad interactiva y comunicación con API
- **Font Awesome**: Iconografía profesional

### Backend
- **Python 3.8+**: Lenguaje principal
- **Flask**: Framework web ligero
- **OpenCV**: Procesamiento de imágenes
- **scikit-image**: Algoritmos de visión artificial
- **scikit-learn**: Machine learning para comparación
- **NumPy/SciPy**: Computación numérica

## 📦 Instalación

### Prerequisitos
- Python 3.8 o superior
- pip (gestor de paquetes de Python)

### Pasos de instalación

1. **Clonar o descargar** los archivos del proyecto:
   ```bash
   # Asegúrate de tener todos estos archivos:
   # - main.html
   # - main.css  
   # - main.js
   # - app.py
   # - requirements.txt
   ```

2. **Instalar dependencias**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Ejecutar la aplicación**:
   ```bash
   python app.py
   ```

4. **Abrir en navegador**:
   ```
   http://localhost:5000
   ```

## 🎯 Cómo Usar

### Paso 1: Cargar Imágenes
- Arrastra y suelta dos imágenes en las áreas designadas
- O haz clic para navegar y seleccionar archivos
- Formatos soportados: JPG, PNG, BMP, TIFF
- Tamaño máximo: 10MB por imagen

### Paso 2: Comparar
- Haz clic en "Comparar Fondos" una vez que ambas imágenes estén cargadas
- El sistema procesará automáticamente las imágenes

### Paso 3: Resultados
La plataforma mostrará:
- **Puntuación general**: Porcentaje de similitud overall
- **Métricas detalladas**:
  - Similitud de colores
  - Similitud de texturas  
  - Similitud estructural
- **Fondos extraídos**: Visualización de los fondos procesados
- **Conclusión**: Interpretación inteligente de los resultados

## 🔬 Algoritmos de Comparación

### 1. Extracción de Fondos
- **Segmentación K-means**: Agrupa píxeles similares
- **Detección de bordes**: Identifica contornos de objetos
- **Filtros morfológicos**: Limpia y refina las máscaras

### 2. Análisis de Características
- **Histogramas de color**: Distribución de colores en RGB
- **Local Binary Patterns (LBP)**: Análisis de texturas
- **Gradientes Sobel**: Características estructurales
- **Momentos de Hu**: Descriptores de forma invariantes

### 3. Comparación
- **Similitud coseno**: Para vectores de características
- **Ponderación inteligente**: Combina diferentes métricas
- **Normalización**: Asegura comparaciones justas

## 📊 Interpretación de Resultados

### Puntuaciones de Similitud
- **70-100%**: Fondos muy similares o idénticos
- **50-69%**: Similitudes moderadas (mismo lugar, diferentes condiciones)
- **30-49%**: Algunas similitudes básicas
- **0-29%**: Fondos significativamente diferentes

### Métricas Individuales
- **Color**: Qué tan parecidos son los colores dominantes
- **Textura**: Similitud en patrones y texturas
- **Estructura**: Similitud en formas y gradientes

## 🛠️ Configuración Avanzada

### Modificar Parámetros (en app.py)
```python
# Tamaño máximo de imagen para procesamiento
max_dimension = 800

# Número de clusters para segmentación
k = 4

# Pesos para similitud general
weights = {
    'color': 0.3,
    'texture': 0.25, 
    'structural': 0.25,
    'shape': 0.2
}
```

### Variables de Entorno
```bash
# Puerto del servidor (opcional)
export FLASK_PORT=5000

# Modo debug (opcional) 
export FLASK_DEBUG=1
```

## 🔧 Solución de Problemas

### Error: "ModuleNotFoundError"
```bash
pip install -r requirements.txt
```

### Error: "OpenCV no funciona"
```bash
pip uninstall opencv-python
pip install opencv-python-headless
```

### Imágenes no se procesan
- Verifica que las imágenes sean válidas
- Asegúrate de que sean menores a 10MB
- Prueba con formatos JPG o PNG

### Rendimiento lento
- Usa imágenes más pequeñas (< 2MB)
- Cierra otras aplicaciones pesadas
- Considera usar un servidor más potente

## 🚀 Despliegue en Producción

### Docker (Recomendado)
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 5000

CMD ["python", "app.py"]
```

### Heroku
```bash
# Crear Procfile
echo "web: python app.py" > Procfile

# Desplegar
git init
heroku create tu-app-name
git add .
git commit -m "Initial commit"
git push heroku main
```

## 📈 Próximas Características

- [ ] Soporte para video comparisons
- [ ] API REST documentada
- [ ] Batch processing de múltiples imágenes
- [ ] Modelos de deep learning
- [ ] Integración con servicios en la nube
- [ ] Sistema de usuarios y historial

## 🤝 Contribuir

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

## 📝 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo `LICENSE` para detalles.

## 🙏 Agradecimientos

- OpenCV por las herramientas de visión artificial
- scikit-image por los algoritmos avanzados
- La comunidad open source por las librerías utilizadas

## 📞 Soporte

Para preguntas, sugerencias o reportar bugs:
- Crear un issue en el repositorio
- Contactar al equipo de desarrollo

---

⭐ ¡Si te gustó este proyecto, no olvides darle una estrella! ⭐ 