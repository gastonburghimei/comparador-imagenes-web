#!/usr/bin/env python3
"""
Comparador de Fondos Mejorado - Algoritmo Real
Implementa comparación real de imágenes usando PIL y técnicas básicas
"""

import os
import time
import base64
import io
import hashlib
from collections import Counter
import math

try:
    from flask import Flask, send_from_directory, jsonify, request
    from flask_cors import CORS
    from PIL import Image, ImageStat, ImageFilter, ImageChops
except ImportError:
    print("❌ Dependencias no instaladas")
    print("💡 Ejecuta: pip3 install flask flask-cors pillow")
    exit(1)

app = Flask(__name__)
CORS(app)

class ImageComparator:
    """Clase para comparar imágenes usando técnicas de visión computacional básica"""
    
    def __init__(self):
        pass
    
    def load_image(self, file_stream):
        """Carga y normaliza una imagen"""
        try:
            # Abrir imagen
            image = Image.open(file_stream)
            
            # Convertir a RGB si es necesario
            if image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Redimensionar para comparación consistente (mantener aspecto)
            max_size = 800
            image.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
            
            return image
        except Exception as e:
            print(f"Error cargando imagen: {e}")
            return None
    
    def extract_background(self, image):
        """Extrae fondo usando técnicas simples"""
        try:
            # Crear una versión suavizada para identificar fondo
            blurred = image.filter(ImageFilter.GaussianBlur(radius=5))
            
            # Redimensionar más para análisis rápido
            small = blurred.resize((100, 75), Image.Resampling.LANCZOS)
            
            # Obtener colores dominantes (simplificado)
            pixels = list(small.getdata())
            
            # Agrupar colores similares
            color_groups = {}
            for pixel in pixels:
                # Redondear colores para agrupar similares
                rounded = tuple(round(c/20)*20 for c in pixel)
                color_groups[rounded] = color_groups.get(rounded, 0) + 1
            
            # Encontrar color más común (probablemente fondo)
            dominant_color = max(color_groups, key=color_groups.get)
            
            # Crear máscara simple basada en color dominante
            mask = Image.new('L', image.size, 0)
            width, height = image.size
            
            for y in range(0, height, 10):  # Muestrear cada 10 píxeles
                for x in range(0, width, 10):
                    try:
                        pixel = image.getpixel((x, y))
                        # Si el píxel es similar al color dominante
                        if self._color_distance(pixel, dominant_color) < 50:
                            # Marcar área como fondo
                            for dy in range(max(0, y-5), min(height, y+5)):
                                for dx in range(max(0, x-5), min(width, x+5)):
                                    mask.putpixel((dx, dy), 255)
                    except:
                        continue
            
            # Aplicar máscara al fondo
            background = Image.composite(image, Image.new('RGB', image.size, (0,0,0)), mask)
            
            return background, mask
        except Exception as e:
            print(f"Error extrayendo fondo: {e}")
            return image, None
    
    def _color_distance(self, color1, color2):
        """Calcula distancia entre dos colores"""
        return math.sqrt(sum((a - b) ** 2 for a, b in zip(color1, color2)))
    
    def compare_images(self, image1, image2):
        """Compara dos imágenes usando múltiples técnicas"""
        try:
            results = {}
            
            # 1. Comparación directa de píxeles (para imágenes idénticas)
            results['pixel_similarity'] = self._compare_pixels(image1, image2)
            
            # 2. Comparación de histogramas de color
            results['color_similarity'] = self._compare_histograms(image1, image2)
            
            # 3. Comparación de características estadísticas
            results['stats_similarity'] = self._compare_stats(image1, image2)
            
            # 4. Comparación estructural básica
            results['structural_similarity'] = self._compare_structure(image1, image2)
            
            # 5. Comparación de hash perceptual
            results['hash_similarity'] = self._compare_hashes(image1, image2)
            
            # Calcular similitud general con pesos optimizados
            overall = (
                results['pixel_similarity'] * 0.4 +      # Muy importante para imágenes idénticas
                results['color_similarity'] * 0.25 +     # Color del fondo
                results['hash_similarity'] * 0.2 +       # Estructura general
                results['stats_similarity'] * 0.1 +      # Estadísticas
                results['structural_similarity'] * 0.05  # Estructura detallada
            )
            
            results['overall_similarity'] = overall
            
            return results
            
        except Exception as e:
            print(f"Error comparando imágenes: {e}")
            return self._default_results()
    
    def _compare_pixels(self, img1, img2):
        """Comparación directa de píxeles (detecta imágenes idénticas)"""
        try:
            # Redimensionar a mismo tamaño
            size = (200, 150)
            img1_small = img1.resize(size, Image.Resampling.LANCZOS)
            img2_small = img2.resize(size, Image.Resampling.LANCZOS)
            
            # Comparar píxel por píxel
            pixels1 = list(img1_small.getdata())
            pixels2 = list(img2_small.getdata())
            
            identical_pixels = 0
            total_pixels = len(pixels1)
            
            for p1, p2 in zip(pixels1, pixels2):
                # Permitir pequeñas diferencias (compresión JPEG)
                if self._color_distance(p1, p2) < 10:
                    identical_pixels += 1
            
            similarity = identical_pixels / total_pixels
            
            # Bonus para imágenes muy similares
            if similarity > 0.95:
                similarity = min(1.0, similarity * 1.05)
                
            return similarity
            
        except Exception as e:
            print(f"Error en comparación de píxeles: {e}")
            return 0.0
    
    def _compare_histograms(self, img1, img2):
        """Compara histogramas de color"""
        try:
            # Obtener histogramas
            hist1 = img1.histogram()
            hist2 = img2.histogram()
            
            # Normalizar histogramas
            total1 = sum(hist1)
            total2 = sum(hist2)
            
            if total1 == 0 or total2 == 0:
                return 0.0
            
            hist1_norm = [h / total1 for h in hist1]
            hist2_norm = [h / total2 for h in hist2]
            
            # Calcular correlación
            correlation = 0
            for h1, h2 in zip(hist1_norm, hist2_norm):
                correlation += min(h1, h2)
            
            return correlation
            
        except Exception as e:
            print(f"Error en histogramas: {e}")
            return 0.0
    
    def _compare_stats(self, img1, img2):
        """Compara estadísticas básicas de las imágenes"""
        try:
            stat1 = ImageStat.Stat(img1)
            stat2 = ImageStat.Stat(img2)
            
            # Comparar medias
            mean1 = stat1.mean
            mean2 = stat2.mean
            
            mean_diff = sum(abs(m1 - m2) for m1, m2 in zip(mean1, mean2)) / len(mean1)
            mean_similarity = max(0, 1 - mean_diff / 255)
            
            # Comparar desviaciones estándar
            stddev1 = stat1.stddev
            stddev2 = stat2.stddev
            
            std_diff = sum(abs(s1 - s2) for s1, s2 in zip(stddev1, stddev2)) / len(stddev1)
            std_similarity = max(0, 1 - std_diff / 255)
            
            # Promedio
            return (mean_similarity + std_similarity) / 2
            
        except Exception as e:
            print(f"Error en estadísticas: {e}")
            return 0.0
    
    def _compare_structure(self, img1, img2):
        """Comparación estructural básica usando diferencias"""
        try:
            # Redimensionar
            size = (100, 75)
            img1_small = img1.resize(size, Image.Resampling.LANCZOS)
            img2_small = img2.resize(size, Image.Resampling.LANCZOS)
            
            # Calcular diferencia
            diff = ImageChops.difference(img1_small, img2_small)
            
            # Obtener estadísticas de la diferencia
            stat = ImageStat.Stat(diff)
            mean_diff = sum(stat.mean) / len(stat.mean)
            
            # Convertir a similitud (menor diferencia = mayor similitud)
            similarity = max(0, 1 - mean_diff / 128)
            
            return similarity
            
        except Exception as e:
            print(f"Error en comparación estructural: {e}")
            return 0.0
    
    def _compare_hashes(self, img1, img2):
        """Comparación usando hash perceptual simple"""
        try:
            # Crear hash simple basado en thumbnail
            def simple_hash(img):
                # Muy pequeño y en escala de grises
                thumb = img.resize((8, 8), Image.Resampling.LANCZOS).convert('L')
                pixels = list(thumb.getdata())
                avg = sum(pixels) / len(pixels)
                # Crear hash binario
                return ''.join('1' if p > avg else '0' for p in pixels)
            
            hash1 = simple_hash(img1)
            hash2 = simple_hash(img2)
            
            # Calcular similitud de Hamming
            if len(hash1) != len(hash2):
                return 0.0
            
            matches = sum(h1 == h2 for h1, h2 in zip(hash1, hash2))
            similarity = matches / len(hash1)
            
            return similarity
            
        except Exception as e:
            print(f"Error en hash: {e}")
            return 0.0
    
    def _default_results(self):
        """Resultados por defecto en caso de error"""
        return {
            'pixel_similarity': 0.0,
            'color_similarity': 0.0,
            'stats_similarity': 0.0,
            'structural_similarity': 0.0,
            'hash_similarity': 0.0,
            'overall_similarity': 0.0
        }

# Instancia global del comparador
comparator = ImageComparator()

@app.route('/')
def index():
    """Servir la página principal"""
    if os.path.exists('main.html'):
        return send_from_directory('.', 'main.html')
    else:
        # HTML integrado mejorado
        return """
<!DOCTYPE html>
<html>
<head>
    <title>🖼️ Comparador de Fondos - Versión Mejorada</title>
    <meta charset="UTF-8">
    <style>
        body { 
            font-family: Arial, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            margin: 0; padding: 20px; color: white; text-align: center; 
        }
        .container { max-width: 900px; margin: 0 auto; }
        .header { background: rgba(72, 187, 120, 0.2); padding: 20px; border-radius: 10px; margin: 20px 0; }
        .upload-grid { display: grid; grid-template-columns: 1fr auto 1fr; gap: 20px; align-items: center; margin: 20px 0; }
        .upload-area { 
            border: 3px dashed #fff; padding: 40px; border-radius: 10px; cursor: pointer; 
            transition: all 0.3s; min-height: 200px; display: flex; flex-direction: column; justify-content: center;
        }
        .upload-area:hover { background: rgba(255,255,255,0.1); transform: translateY(-2px); }
        .upload-area.has-file { border-color: #4CAF50; background: rgba(76, 175, 80, 0.2); }
        .vs-divider { 
            width: 60px; height: 60px; background: linear-gradient(135deg, #667eea, #764ba2);
            border-radius: 50%; display: flex; align-items: center; justify-content: center;
            font-weight: bold; font-size: 18px; color: white; box-shadow: 0 4px 8px rgba(0,0,0,0.3);
        }
        .btn { 
            background: #4CAF50; color: white; padding: 15px 30px; 
            border: none; border-radius: 10px; cursor: pointer; margin: 20px; font-size: 16px;
            transition: all 0.3s; box-shadow: 0 4px 8px rgba(0,0,0,0.3);
        }
        .btn:hover { background: #45a049; transform: translateY(-2px); }
        .btn:disabled { background: #666; cursor: not-allowed; transform: none; }
        .result { 
            background: rgba(255,255,255,0.1); padding: 30px; border-radius: 15px; margin: 20px 0;
            backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,0.2);
        }
        .similarity-score { font-size: 3em; font-weight: bold; margin: 20px 0; }
        .details-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
        .detail-item { 
            background: rgba(255,255,255,0.1); padding: 15px; border-radius: 10px;
            border-left: 4px solid #4CAF50;
        }
        .detail-label { font-size: 0.9em; opacity: 0.8; }
        .detail-value { font-size: 1.2em; font-weight: bold; }
        .image-preview { max-width: 100%; height: 200px; object-fit: cover; border-radius: 10px; margin-top: 10px; }
        .loading { animation: pulse 1.5s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🖼️ Comparador de Fondos - Algoritmo Mejorado</h1>
            <p>✅ Detecta imágenes idénticas • 🎨 Análisis de color • 📊 Comparación estructural</p>
        </div>
        
        <div class="upload-grid">
            <div class="upload-area" id="area1" onclick="document.getElementById('file1').click()">
                <h3 id="title1">📸 Imagen 1</h3>
                <p>Haz clic para seleccionar</p>
                <input type="file" id="file1" accept="image/*" style="display:none">
                <img id="preview1" class="image-preview" style="display:none">
            </div>
            
            <div class="vs-divider">VS</div>
            
            <div class="upload-area" id="area2" onclick="document.getElementById('file2').click()">
                <h3 id="title2">📸 Imagen 2</h3>
                <p>Haz clic para seleccionar</p>
                <input type="file" id="file2" accept="image/*" style="display:none">
                <img id="preview2" class="image-preview" style="display:none">
            </div>
        </div>
        
        <button class="btn" id="compareBtn" onclick="compareImages()" disabled>🔍 Comparar Fondos</button>
        
        <div id="result" style="display:none;" class="result">
            <h2>📊 Resultados del Análisis</h2>
            <div class="similarity-score" id="overallScore">--%</div>
            <div class="details-grid">
                <div class="detail-item">
                    <div class="detail-label">🎯 Píxeles Idénticos</div>
                    <div class="detail-value" id="pixelSim">--%</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">🎨 Colores</div>
                    <div class="detail-value" id="colorSim">--%</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">📈 Estadísticas</div>
                    <div class="detail-value" id="statsSim">--%</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">🏗️ Estructura</div>
                    <div class="detail-value" id="structSim">--%</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">🔢 Hash Perceptual</div>
                    <div class="detail-value" id="hashSim">--%</div>
                </div>
            </div>
            <div id="conclusion" style="margin-top: 20px; font-size: 1.1em;"></div>
        </div>
    </div>
    
    <script>
        let file1 = null, file2 = null;
        
        function setupFileInput(fileInputId, areaId, titleId, previewId) {
            const fileInput = document.getElementById(fileInputId);
            const area = document.getElementById(areaId);
            const title = document.getElementById(titleId);
            const preview = document.getElementById(previewId);
            
            fileInput.addEventListener('change', (e) => {
                const file = e.target.files[0];
                if (file) {
                    if (fileInputId === 'file1') file1 = file;
                    if (fileInputId === 'file2') file2 = file;
                    
                    title.innerHTML = '✅ ' + file.name;
                    area.classList.add('has-file');
                    
                    // Mostrar preview
                    const reader = new FileReader();
                    reader.onload = (e) => {
                        preview.src = e.target.result;
                        preview.style.display = 'block';
                    };
                    reader.readAsDataURL(file);
                    
                    updateCompareButton();
                }
            });
        }
        
        setupFileInput('file1', 'area1', 'title1', 'preview1');
        setupFileInput('file2', 'area2', 'title2', 'preview2');
        
        function updateCompareButton() {
            const btn = document.getElementById('compareBtn');
            btn.disabled = !(file1 && file2);
        }
        
        function compareImages() {
            if (!file1 || !file2) return;
            
            const formData = new FormData();
            formData.append('image1', file1);
            formData.append('image2', file2);
            
            // Mostrar loading
            document.getElementById('result').style.display = 'block';
            document.getElementById('overallScore').innerHTML = '🔄';
            document.getElementById('overallScore').classList.add('loading');
            
            fetch('/api/compare-backgrounds', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                document.getElementById('overallScore').classList.remove('loading');
                
                const overall = Math.round(data.overall_similarity * 100);
                document.getElementById('overallScore').innerHTML = overall + '%';
                
                document.getElementById('pixelSim').innerHTML = Math.round(data.pixel_similarity * 100) + '%';
                document.getElementById('colorSim').innerHTML = Math.round(data.color_similarity * 100) + '%';
                document.getElementById('statsSim').innerHTML = Math.round(data.stats_similarity * 100) + '%';
                document.getElementById('structSim').innerHTML = Math.round(data.structural_similarity * 100) + '%';
                document.getElementById('hashSim').innerHTML = Math.round(data.hash_similarity * 100) + '%';
                
                // Conclusión inteligente
                let conclusion = '';
                if (overall >= 95) {
                    conclusion = '🎯 <strong>Imágenes prácticamente idénticas</strong> - Muy alta probabilidad de ser la misma imagen';
                } else if (overall >= 80) {
                    conclusion = '✅ <strong>Fondos muy similares</strong> - Probablemente el mismo lugar con pequeñas variaciones';
                } else if (overall >= 60) {
                    conclusion = '🟡 <strong>Similitudes moderadas</strong> - Podrían ser fondos relacionados';
                } else if (overall >= 40) {
                    conclusion = '🟠 <strong>Algunas similitudes</strong> - Fondos diferentes pero con elementos comunes';
                } else {
                    conclusion = '🔴 <strong>Fondos muy diferentes</strong> - No parecen ser el mismo lugar';
                }
                
                document.getElementById('conclusion').innerHTML = conclusion;
            })
            .catch(error => {
                document.getElementById('overallScore').innerHTML = '❌';
                console.error('Error:', error);
            });
        }
    </script>
</body>
</html>
        """

@app.route('/<path:filename>')
def serve_static(filename):
    """Servir archivos estáticos"""
    return send_from_directory('.', filename)

@app.route('/api/compare-backgrounds', methods=['POST'])
def compare_backgrounds():
    """API mejorada para comparar fondos reales"""
    try:
        # Verificar archivos
        if 'image1' not in request.files or 'image2' not in request.files:
            return jsonify({'error': 'Se requieren dos imágenes'}), 400
        
        file1 = request.files['image1']
        file2 = request.files['image2']
        
        print(f"📸 Comparando: {file1.filename} vs {file2.filename}")
        
        # Cargar imágenes
        img1 = comparator.load_image(file1.stream)
        img2 = comparator.load_image(file2.stream)
        
        if not img1 or not img2:
            return jsonify({'error': 'Error cargando imágenes'}), 400
        
        # Comparar imágenes completas primero
        results = comparator.compare_images(img1, img2)
        
        print(f"📊 Resultados: {results['overall_similarity']:.2%}")
        
        # Si las imágenes son muy similares, también comparar fondos extraídos
        if results['overall_similarity'] < 0.9:
            try:
                bg1, _ = comparator.extract_background(img1)
                bg2, _ = comparator.extract_background(img2)
                
                bg_results = comparator.compare_images(bg1, bg2)
                
                # Promediar resultados de imagen completa y fondo
                for key in ['color_similarity', 'stats_similarity', 'structural_similarity']:
                    if key in results and key in bg_results:
                        results[key] = (results[key] + bg_results[key]) / 2
                
                # Recalcular overall con fondo incluido
                results['overall_similarity'] = (
                    results['pixel_similarity'] * 0.4 +
                    results['color_similarity'] * 0.25 +
                    results['hash_similarity'] * 0.2 +
                    results['stats_similarity'] * 0.1 +
                    results['structural_similarity'] * 0.05
                )
                
            except Exception as e:
                print(f"Error extrayendo fondos: {e}")
        
        # Preparar respuesta
        response = {
            'overall_similarity': float(results['overall_similarity']),
            'pixel_similarity': float(results['pixel_similarity']),
            'color_similarity': float(results['color_similarity']),
            'texture_similarity': float(results['stats_similarity']),  # Mapear para compatibilidad
            'structural_similarity': float(results['structural_similarity']),
            'hash_similarity': float(results['hash_similarity']),
            'stats_similarity': float(results['stats_similarity']),
            'message': f'✅ Análisis completado con algoritmo mejorado'
        }
        
        return jsonify(response)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({'error': f'Error procesando imágenes: {str(e)}'}), 500

@app.route('/api/health')
def health():
    """Verificación de salud"""
    return jsonify({
        'status': 'ok', 
        'port': 8080, 
        'version': 'mejorado',
        'message': 'Algoritmo de comparación real activo'
    })

if __name__ == '__main__':
    print("🚀 Iniciando Comparador de Fondos - Versión Mejorada")
    print("🧠 Algoritmo real de comparación de imágenes")
    print("🎯 Detección de imágenes idénticas optimizada")
    print("🌐 URL: http://localhost:8080")
    print("🛑 Presiona Ctrl+C para detener")
    print("-" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=8080) 