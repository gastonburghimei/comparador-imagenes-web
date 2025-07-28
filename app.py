"""
Comparador de Fondos de Imágenes - Backend Flask
Procesa y compara fondos de imágenes usando técnicas de visión artificial
"""

import os
import base64
import io
import numpy as np
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from PIL import Image, ImageFilter, ImageEnhance
import cv2
from skimage import feature, filters, segmentation, measure
from sklearn.metrics.pairwise import cosine_similarity
from scipy.spatial.distance import euclidean
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar Flask
app = Flask(__name__)
CORS(app)

# Configuración
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB máximo
UPLOAD_FOLDER = 'uploads'
PROCESSED_FOLDER = 'processed'

# Crear directorios si no existen
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

class BackgroundComparator:
    """Clase principal para comparar fondos de imágenes"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def extract_background(self, image_array):
        """
        Extrae el fondo de una imagen usando segmentación
        """
        try:
            # Convertir a RGB si es necesario
            if len(image_array.shape) == 3 and image_array.shape[2] == 3:
                # Convertir RGB a BGR para OpenCV
                image_bgr = cv2.cvtColor(image_array, cv2.COLOR_RGB2BGR)
            else:
                image_bgr = image_array
            
            # Redimensionar para procesamiento más rápido
            height, width = image_bgr.shape[:2]
            max_dimension = 800
            if max(height, width) > max_dimension:
                scale = max_dimension / max(height, width)
                new_width = int(width * scale)
                new_height = int(height * scale)
                image_bgr = cv2.resize(image_bgr, (new_width, new_height))
            
            # Método 1: Segmentación por clustering
            background_mask = self._segment_by_clustering(image_bgr)
            
            # Método 2: Detección de bordes para mejorar la máscara
            edges = cv2.Canny(image_bgr, 50, 150)
            edges_dilated = cv2.dilate(edges, np.ones((3,3), np.uint8), iterations=1)
            
            # Combinar máscaras
            combined_mask = cv2.bitwise_and(background_mask, cv2.bitwise_not(edges_dilated))
            
            # Aplicar filtros morfológicos para limpiar la máscara
            kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
            combined_mask = cv2.morphologyEx(combined_mask, cv2.MORPH_CLOSE, kernel)
            combined_mask = cv2.morphologyEx(combined_mask, cv2.MORPH_OPEN, kernel)
            
            # Extraer fondo
            background = cv2.bitwise_and(image_bgr, image_bgr, mask=combined_mask)
            
            return background, combined_mask
            
        except Exception as e:
            self.logger.error(f"Error extrayendo fondo: {e}")
            # Retornar imagen original como fallback
            return image_array, np.ones(image_array.shape[:2], dtype=np.uint8) * 255
    
    def _segment_by_clustering(self, image):
        """Segmentación por clustering K-means"""
        try:
            # Convertir a espacio de color Lab para mejor segmentación
            lab_image = cv2.cvtColor(image, cv2.COLOR_BGR2LAB)
            
            # Preparar datos para clustering
            data = lab_image.reshape((-1, 3))
            data = np.float32(data)
            
            # Aplicar K-means clustering
            criteria = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 20, 1.0)
            k = 4  # Número de clusters
            _, labels, centers = cv2.kmeans(data, k, None, criteria, 10, cv2.KMEANS_RANDOM_CENTERS)
            
            # Crear máscara para el cluster más grande (probablemente el fondo)
            labels = labels.reshape(image.shape[:2])
            unique, counts = np.unique(labels, return_counts=True)
            background_cluster = unique[np.argmax(counts)]
            
            # Crear máscara binaria
            mask = np.where(labels == background_cluster, 255, 0).astype(np.uint8)
            
            return mask
            
        except Exception as e:
            self.logger.error(f"Error en clustering: {e}")
            return np.ones(image.shape[:2], dtype=np.uint8) * 255
    
    def extract_features(self, image_array):
        """
        Extrae características visuales de una imagen
        """
        try:
            # Convertir a escala de grises
            if len(image_array.shape) == 3:
                gray = cv2.cvtColor(image_array, cv2.COLOR_BGR2GRAY)
            else:
                gray = image_array
            
            # Redimensionar para consistencia
            gray = cv2.resize(gray, (256, 256))
            
            features = {}
            
            # 1. Histograma de colores
            if len(image_array.shape) == 3:
                resized_color = cv2.resize(image_array, (256, 256))
                features['color_hist'] = self._calculate_color_histogram(resized_color)
            else:
                features['color_hist'] = np.zeros(256)
            
            # 2. Textura - Local Binary Patterns (LBP)
            features['texture_lbp'] = self._calculate_lbp(gray)
            
            # 3. Características estructurales - Gradientes
            features['gradients'] = self._calculate_gradients(gray)
            
            # 4. Características de forma
            features['shape'] = self._calculate_shape_features(gray)
            
            return features
            
        except Exception as e:
            self.logger.error(f"Error extrayendo características: {e}")
            return self._get_default_features()
    
    def _calculate_color_histogram(self, image):
        """Calcula histograma de colores"""
        try:
            # Histograma para cada canal
            hist_b = cv2.calcHist([image], [0], None, [64], [0, 256])
            hist_g = cv2.calcHist([image], [1], None, [64], [0, 256])
            hist_r = cv2.calcHist([image], [2], None, [64], [0, 256])
            
            # Concatenar y normalizar
            hist = np.concatenate([hist_r.flatten(), hist_g.flatten(), hist_b.flatten()])
            hist = hist / (hist.sum() + 1e-8)
            
            return hist
            
        except Exception as e:
            self.logger.error(f"Error calculando histograma: {e}")
            return np.zeros(192)
    
    def _calculate_lbp(self, gray_image):
        """Calcula Local Binary Patterns para textura"""
        try:
            # Parámetros LBP
            radius = 3
            n_points = 8 * radius
            
            # Calcular LBP
            lbp = feature.local_binary_pattern(gray_image, n_points, radius, method='uniform')
            
            # Histograma de LBP
            hist, _ = np.histogram(lbp.ravel(), bins=n_points + 2, 
                                 range=(0, n_points + 2), density=True)
            
            return hist
            
        except Exception as e:
            self.logger.error(f"Error calculando LBP: {e}")
            return np.zeros(26)
    
    def _calculate_gradients(self, gray_image):
        """Calcula características de gradientes"""
        try:
            # Gradientes Sobel
            grad_x = cv2.Sobel(gray_image, cv2.CV_64F, 1, 0, ksize=3)
            grad_y = cv2.Sobel(gray_image, cv2.CV_64F, 0, 1, ksize=3)
            
            # Magnitud y dirección
            magnitude = np.sqrt(grad_x**2 + grad_y**2)
            angle = np.arctan2(grad_y, grad_x)
            
            # Histograma de orientaciones
            hist, _ = np.histogram(angle.ravel(), bins=36, 
                                 range=(-np.pi, np.pi), density=True)
            
            # Estadísticas de magnitud
            mag_stats = [
                np.mean(magnitude),
                np.std(magnitude),
                np.percentile(magnitude, 25),
                np.percentile(magnitude, 75)
            ]
            
            return np.concatenate([hist, mag_stats])
            
        except Exception as e:
            self.logger.error(f"Error calculando gradientes: {e}")
            return np.zeros(40)
    
    def _calculate_shape_features(self, gray_image):
        """Calcula características de forma"""
        try:
            # Detectar contornos
            edges = cv2.Canny(gray_image, 50, 150)
            contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            if not contours:
                return np.zeros(10)
            
            # Características del contorno más grande
            largest_contour = max(contours, key=cv2.contourArea)
            
            features = []
            
            # Área y perímetro
            area = cv2.contourArea(largest_contour)
            perimeter = cv2.arcLength(largest_contour, True)
            features.extend([area, perimeter])
            
            # Relación aspecto de bounding box
            x, y, w, h = cv2.boundingRect(largest_contour)
            aspect_ratio = float(w) / h if h != 0 else 0
            features.append(aspect_ratio)
            
            # Solidez (área del contorno / área del hull convexo)
            hull = cv2.convexHull(largest_contour)
            hull_area = cv2.contourArea(hull)
            solidity = area / hull_area if hull_area != 0 else 0
            features.append(solidity)
            
            # Compacidad
            compactness = (perimeter**2) / area if area != 0 else 0
            features.append(compactness)
            
            # Momentos de Hu (invariantes)
            moments = cv2.moments(largest_contour)
            hu_moments = cv2.HuMoments(moments)
            features.extend(hu_moments.flatten()[:5])  # Primeros 5 momentos
            
            return np.array(features)
            
        except Exception as e:
            self.logger.error(f"Error calculando características de forma: {e}")
            return np.zeros(10)
    
    def _get_default_features(self):
        """Características por defecto en caso de error"""
        return {
            'color_hist': np.zeros(192),
            'texture_lbp': np.zeros(26),
            'gradients': np.zeros(40),
            'shape': np.zeros(10)
        }
    
    def compare_features(self, features1, features2):
        """
        Compara las características extraídas de dos imágenes
        """
        try:
            similarities = {}
            
            # Similitud de color
            color_sim = cosine_similarity(
                features1['color_hist'].reshape(1, -1),
                features2['color_hist'].reshape(1, -1)
            )[0, 0]
            similarities['color'] = max(0, color_sim)
            
            # Similitud de textura
            texture_sim = cosine_similarity(
                features1['texture_lbp'].reshape(1, -1),
                features2['texture_lbp'].reshape(1, -1)
            )[0, 0]
            similarities['texture'] = max(0, texture_sim)
            
            # Similitud estructural
            struct_sim = cosine_similarity(
                features1['gradients'].reshape(1, -1),
                features2['gradients'].reshape(1, -1)
            )[0, 0]
            similarities['structural'] = max(0, struct_sim)
            
            # Similitud de forma
            shape_sim = cosine_similarity(
                features1['shape'].reshape(1, -1),
                features2['shape'].reshape(1, -1)
            )[0, 0]
            similarities['shape'] = max(0, shape_sim)
            
            # Similitud general (promedio ponderado)
            overall = (
                similarities['color'] * 0.3 +
                similarities['texture'] * 0.25 +
                similarities['structural'] * 0.25 +
                similarities['shape'] * 0.2
            )
            
            similarities['overall'] = overall
            
            return similarities
            
        except Exception as e:
            self.logger.error(f"Error comparando características: {e}")
            return {
                'color': 0.0,
                'texture': 0.0,
                'structural': 0.0,
                'shape': 0.0,
                'overall': 0.0
            }

# Instancia global del comparador
comparator = BackgroundComparator()

def load_image_from_upload(file):
    """Carga una imagen desde un archivo subido"""
    try:
        # Leer imagen
        image = Image.open(file.stream)
        
        # Convertir a RGB si es necesario
        if image.mode != 'RGB':
            image = image.convert('RGB')
        
        # Convertir a array numpy
        image_array = np.array(image)
        
        return image_array
        
    except Exception as e:
        logger.error(f"Error cargando imagen: {e}")
        raise

def array_to_base64(image_array):
    """Convierte un array numpy a string base64"""
    try:
        # Asegurarse de que esté en formato uint8
        if image_array.dtype != np.uint8:
            image_array = np.clip(image_array, 0, 255).astype(np.uint8)
        
        # Convertir a PIL Image
        image = Image.fromarray(image_array)
        
        # Convertir a base64
        buffer = io.BytesIO()
        image.save(buffer, format='JPEG', quality=85)
        buffer.seek(0)
        
        return base64.b64encode(buffer.getvalue()).decode()
        
    except Exception as e:
        logger.error(f"Error convirtiendo a base64: {e}")
        return None

@app.route('/')
def index():
    """Servir la página principal"""
    return send_from_directory('.', 'main.html')

@app.route('/<path:filename>')
def serve_static(filename):
    """Servir archivos estáticos"""
    return send_from_directory('.', filename)

@app.route('/api/compare-backgrounds', methods=['POST'])
def compare_backgrounds():
    """Endpoint principal para comparar fondos de imágenes"""
    try:
        # Verificar que se hayan subido dos imágenes
        if 'image1' not in request.files or 'image2' not in request.files:
            return jsonify({'error': 'Se requieren dos imágenes'}), 400
        
        file1 = request.files['image1']
        file2 = request.files['image2']
        
        if file1.filename == '' or file2.filename == '':
            return jsonify({'error': 'Archivos vacíos'}), 400
        
        logger.info("Iniciando comparación de fondos...")
        
        # Cargar imágenes
        image1 = load_image_from_upload(file1)
        image2 = load_image_from_upload(file2)
        
        logger.info(f"Imágenes cargadas: {image1.shape}, {image2.shape}")
        
        # Extraer fondos
        background1, mask1 = comparator.extract_background(image1)
        background2, mask2 = comparator.extract_background(image2)
        
        logger.info("Fondos extraídos")
        
        # Extraer características de los fondos
        features1 = comparator.extract_features(background1)
        features2 = comparator.extract_features(background2)
        
        logger.info("Características extraídas")
        
        # Comparar características
        similarities = comparator.compare_features(features1, features2)
        
        logger.info(f"Similitudes calculadas: {similarities}")
        
        # Convertir fondos procesados a base64
        processed_img1_b64 = array_to_base64(background1)
        processed_img2_b64 = array_to_base64(background2)
        
        # Preparar respuesta
        response = {
            'overall_similarity': similarities['overall'],
            'color_similarity': similarities['color'],
            'texture_similarity': similarities['texture'],
            'structural_similarity': similarities['structural'],
            'shape_similarity': similarities['shape'],
            'processed_image1': processed_img1_b64,
            'processed_image2': processed_img2_b64,
            'message': 'Comparación completada exitosamente'
        }
        
        logger.info("Comparación completada")
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error en comparación: {e}")
        return jsonify({'error': f'Error procesando imágenes: {str(e)}'}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Endpoint de verificación de salud"""
    return jsonify({'status': 'ok', 'message': 'Backend funcionando correctamente'})

if __name__ == '__main__':
    print("🚀 Iniciando Comparador de Fondos de Imágenes...")
    print("📁 Archivos necesarios:")
    print("   - main.html (interfaz)")
    print("   - main.css (estilos)")
    print("   - main.js (funcionalidad)")
    print("💡 Instala las dependencias con: pip install -r requirements.txt")
    print("🌐 Accede a: http://localhost:5000")
    print("-" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=5000) 