#!/usr/bin/env python3
"""
Comparador de Fondos Optimizado - Súper Rápido
Sin extracción de fondos, solo comparación directa de imágenes
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

class FastImageComparator:
    """Comparador rápido de imágenes sin extracción de fondos"""
    
    def __init__(self):
        pass
    
    def load_image(self, file_stream):
        """Carga y normaliza una imagen rápidamente"""
        try:
            image = Image.open(file_stream)
            
            if image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Redimensionar para comparación rápida
            max_size = 600  # Reducido para mayor velocidad
            image.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
            
            return image
        except Exception as e:
            print(f"Error cargando imagen: {e}")
            return None
    
    def compare_images_fast(self, image1, image2):
        """Comparación rápida sin extracción de fondos"""
        try:
            results = {}
            
            # 1. Comparación directa de píxeles (MUY importante para idénticas)
            results['pixel_similarity'] = self._compare_pixels_fast(image1, image2)
            
            # 2. Hash perceptual rápido
            results['hash_similarity'] = self._compare_hashes_fast(image1, image2)
            
            # 3. Comparación de histogramas (solo si no son idénticas)
            if results['pixel_similarity'] < 0.95:
                results['color_similarity'] = self._compare_histograms_fast(image1, image2)
            else:
                results['color_similarity'] = results['pixel_similarity']
            
            # 4. Estadísticas básicas (solo si necesario)
            if results['pixel_similarity'] < 0.90:
                results['stats_similarity'] = self._compare_stats_fast(image1, image2)
            else:
                results['stats_similarity'] = results['pixel_similarity']
            
            # 5. Estructura (solo para casos no obvios)
            if results['pixel_similarity'] < 0.85:
                results['structural_similarity'] = self._compare_structure_fast(image1, image2)
            else:
                results['structural_similarity'] = results['pixel_similarity']
            
            # Cálculo optimizado de similitud general
            if results['pixel_similarity'] > 0.95:
                # Para imágenes muy similares, dar peso máximo a píxeles
                overall = results['pixel_similarity'] * 0.9 + results['hash_similarity'] * 0.1
            else:
                # Para imágenes diferentes, usar todos los criterios
                overall = (
                    results['pixel_similarity'] * 0.5 +
                    results['color_similarity'] * 0.25 +
                    results['hash_similarity'] * 0.15 +
                    results['stats_similarity'] * 0.06 +
                    results['structural_similarity'] * 0.04
                )
            
            results['overall_similarity'] = overall
            
            return results
            
        except Exception as e:
            print(f"Error comparando imágenes: {e}")
            return self._default_results()
    
    def _compare_pixels_fast(self, img1, img2):
        """Comparación ultra-rápida de píxeles"""
        try:
            # Tamaño más pequeño para velocidad
            size = (150, 100)
            img1_small = img1.resize(size, Image.Resampling.LANCZOS)
            img2_small = img2.resize(size, Image.Resampling.LANCZOS)
            
            pixels1 = list(img1_small.getdata())
            pixels2 = list(img2_small.getdata())
            
            identical_pixels = 0
            total_pixels = len(pixels1)
            
            for p1, p2 in zip(pixels1, pixels2):
                # Tolerancia para compresión JPEG
                if self._color_distance(p1, p2) < 15:
                    identical_pixels += 1
            
            similarity = identical_pixels / total_pixels
            
            # Bonus para imágenes muy similares
            if similarity > 0.98:
                similarity = min(1.0, similarity * 1.02)
            elif similarity > 0.95:
                similarity = min(1.0, similarity * 1.01)
                
            return similarity
            
        except Exception as e:
            print(f"Error en píxeles: {e}")
            return 0.0
    
    def _compare_hashes_fast(self, img1, img2):
        """Hash perceptual ultra-rápido"""
        try:
            def quick_hash(img):
                # Muy pequeño para velocidad máxima
                thumb = img.resize((8, 8), Image.Resampling.LANCZOS).convert('L')
                pixels = list(thumb.getdata())
                avg = sum(pixels) / len(pixels)
                return ''.join('1' if p > avg else '0' for p in pixels)
            
            hash1 = quick_hash(img1)
            hash2 = quick_hash(img2)
            
            if hash1 == hash2:
                return 1.0
            
            matches = sum(h1 == h2 for h1, h2 in zip(hash1, hash2))
            similarity = matches / len(hash1)
            
            return similarity
            
        except Exception as e:
            print(f"Error en hash: {e}")
            return 0.0
    
    def _compare_histograms_fast(self, img1, img2):
        """Comparación rápida de histogramas"""
        try:
            # Reducir resolución para velocidad
            small1 = img1.resize((100, 75), Image.Resampling.LANCZOS)
            small2 = img2.resize((100, 75), Image.Resampling.LANCZOS)
            
            hist1 = small1.histogram()
            hist2 = small2.histogram()
            
            # Correlación simplificada
            correlation = 0
            total1 = sum(hist1) or 1
            total2 = sum(hist2) or 1
            
            for h1, h2 in zip(hist1, hist2):
                correlation += min(h1/total1, h2/total2)
            
            return correlation
            
        except Exception as e:
            print(f"Error en histograma: {e}")
            return 0.0
    
    def _compare_stats_fast(self, img1, img2):
        """Estadísticas rápidas"""
        try:
            stat1 = ImageStat.Stat(img1)
            stat2 = ImageStat.Stat(img2)
            
            # Solo comparar medias (más rápido)
            mean1 = stat1.mean
            mean2 = stat2.mean
            
            mean_diff = sum(abs(m1 - m2) for m1, m2 in zip(mean1, mean2)) / len(mean1)
            similarity = max(0, 1 - mean_diff / 255)
            
            return similarity
            
        except Exception as e:
            print(f"Error en stats: {e}")
            return 0.0
    
    def _compare_structure_fast(self, img1, img2):
        """Comparación estructural ultra-rápida"""
        try:
            # Muy pequeño para velocidad
            size = (50, 40)
            img1_tiny = img1.resize(size, Image.Resampling.LANCZOS)
            img2_tiny = img2.resize(size, Image.Resampling.LANCZOS)
            
            diff = ImageChops.difference(img1_tiny, img2_tiny)
            stat = ImageStat.Stat(diff)
            mean_diff = sum(stat.mean) / len(stat.mean)
            
            similarity = max(0, 1 - mean_diff / 128)
            return similarity
            
        except Exception as e:
            print(f"Error en estructura: {e}")
            return 0.0
    
    def _color_distance(self, color1, color2):
        """Distancia rápida entre colores"""
        return abs(color1[0] - color2[0]) + abs(color1[1] - color2[1]) + abs(color1[2] - color2[2])
    
    def _default_results(self):
        """Resultados por defecto"""
        return {
            'pixel_similarity': 0.0,
            'color_similarity': 0.0,
            'stats_similarity': 0.0,
            'structural_similarity': 0.0,
            'hash_similarity': 0.0,
            'overall_similarity': 0.0
        }

# Instancia global del comparador
comparator = FastImageComparator()

@app.route('/')
def index():
    """Servir la página principal"""
    if os.path.exists('main.html'):
        return send_from_directory('.', 'main.html')
    else:
        # HTML optimizado sin fondos extraídos
        return """
<!DOCTYPE html>
<html>
<head>
    <title>🖼️ Comparador Rápido de Fondos</title>
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
        .similarity-score { font-size: 4em; font-weight: bold; margin: 20px 0; }
        .details-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 15px; margin: 20px 0; }
        .detail-item { 
            background: rgba(255,255,255,0.1); padding: 15px; border-radius: 10px;
            border-left: 4px solid #4CAF50;
        }
        .detail-label { font-size: 0.9em; opacity: 0.8; }
        .detail-value { font-size: 1.3em; font-weight: bold; }
        .image-preview { max-width: 100%; height: 200px; object-fit: cover; border-radius: 10px; margin-top: 10px; }
        .loading { animation: pulse 1.5s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .speed-badge { background: #ff6b6b; color: white; padding: 5px 10px; border-radius: 20px; font-size: 0.8em; margin-left: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🖼️ Comparador Rápido de Fondos <span class="speed-badge">⚡ ULTRA RÁPIDO</span></h1>
            <p>✅ Detección instantánea • 🎯 Sin extracción de fondos • 🚀 Optimizado para velocidad</p>
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
        
        <button class="btn" id="compareBtn" onclick="compareImages()" disabled>⚡ Comparar Rápido</button>
        
        <div id="result" style="display:none;" class="result">
            <h2>📊 Resultado de Comparación</h2>
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
                    <div class="detail-label">🔢 Hash Estructural</div>
                    <div class="detail-value" id="hashSim">--%</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">⏱️ Tiempo</div>
                    <div class="detail-value" id="timeValue">--s</div>
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
            
            const startTime = performance.now();
            
            const formData = new FormData();
            formData.append('image1', file1);
            formData.append('image2', file2);
            
            document.getElementById('result').style.display = 'block';
            document.getElementById('overallScore').innerHTML = '⚡';
            document.getElementById('overallScore').classList.add('loading');
            
            fetch('/api/compare-backgrounds', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                const endTime = performance.now();
                const processingTime = ((endTime - startTime) / 1000).toFixed(2);
                
                document.getElementById('overallScore').classList.remove('loading');
                
                const overall = Math.round(data.overall_similarity * 100);
                document.getElementById('overallScore').innerHTML = overall + '%';
                
                document.getElementById('pixelSim').innerHTML = Math.round(data.pixel_similarity * 100) + '%';
                document.getElementById('colorSim').innerHTML = Math.round(data.color_similarity * 100) + '%';
                document.getElementById('hashSim').innerHTML = Math.round(data.hash_similarity * 100) + '%';
                document.getElementById('timeValue').innerHTML = processingTime + 's';
                
                let conclusion = '';
                if (overall >= 98) {
                    conclusion = '🎯 <strong>Imágenes idénticas</strong> - 100% seguro que es la misma imagen';
                } else if (overall >= 90) {
                    conclusion = '✅ <strong>Prácticamente idénticas</strong> - Muy alta probabilidad';
                } else if (overall >= 75) {
                    conclusion = '🟢 <strong>Fondos muy similares</strong> - Mismo lugar o fondo';
                } else if (overall >= 50) {
                    conclusion = '🟡 <strong>Similitudes moderadas</strong> - Algunos elementos comunes';
                } else {
                    conclusion = '🔴 <strong>Fondos diferentes</strong> - No parecen relacionados';
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
    """API optimizada - sin extracción de fondos"""
    try:
        start_time = time.time()
        
        if 'image1' not in request.files or 'image2' not in request.files:
            return jsonify({'error': 'Se requieren dos imágenes'}), 400
        
        file1 = request.files['image1']
        file2 = request.files['image2']
        
        print(f"⚡ Comparación rápida: {file1.filename} vs {file2.filename}")
        
        # Cargar imágenes
        img1 = comparator.load_image(file1.stream)
        img2 = comparator.load_image(file2.stream)
        
        if not img1 or not img2:
            return jsonify({'error': 'Error cargando imágenes'}), 400
        
        # Comparación súper rápida (sin extracción de fondos)
        results = comparator.compare_images_fast(img1, img2)
        
        processing_time = round(time.time() - start_time, 3)
        
        print(f"📊 Resultado: {results['overall_similarity']:.2%} en {processing_time}s")
        
        # Respuesta optimizada
        response = {
            'overall_similarity': float(results['overall_similarity']),
            'pixel_similarity': float(results['pixel_similarity']),
            'color_similarity': float(results['color_similarity']),
            'texture_similarity': float(results['stats_similarity']),  # Para compatibilidad
            'structural_similarity': float(results['structural_similarity']),
            'hash_similarity': float(results['hash_similarity']),
            'stats_similarity': float(results['stats_similarity']),
            'processing_time': processing_time,
            'message': f'⚡ Análisis completado en {processing_time}s (sin extracción de fondos)'
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
        'version': 'rapido',
        'message': 'Comparación ultra-rápida sin extracción de fondos'
    })

if __name__ == '__main__':
    print("⚡ Iniciando Comparador de Fondos - Versión Ultra Rápida")
    print("🚀 Sin extracción de fondos - Solo comparación directa")
    print("⏱️ Optimizado para máxima velocidad")
    print("🌐 URL: http://localhost:8080")
    print("🛑 Presiona Ctrl+C para detener")
    print("-" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=8080) 