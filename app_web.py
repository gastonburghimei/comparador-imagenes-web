#!/usr/bin/env python3
"""
Comparador de Imágenes Web
Versión optimizada para despliegue en producción
"""

import os
import time
import logging
from flask import Flask, jsonify, request
from flask_cors import CORS
from PIL import Image, ImageStat, ImageChops

# Configurar logging para producción
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

class FastImageComparator:
    """Comparador rápido de imágenes optimizado para web"""
    
    def load_image(self, file_stream):
        """Carga y normaliza una imagen rápidamente"""
        try:
            image = Image.open(file_stream)
            
            if image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Redimensionar para comparación rápida
            max_size = 600
            image.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
            
            return image
        except Exception as e:
            logger.error(f"Error cargando imagen: {e}")
            return None
    
    def compare_images_fast(self, image1, image2):
        """Comparación rápida y directa"""
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
            if results['pixel_similarity'] >= 0.99:
                overall = 1.0
            elif results['pixel_similarity'] > 0.95:
                overall = results['pixel_similarity'] * 0.95 + results['hash_similarity'] * 0.05
            elif results['pixel_similarity'] > 0.85:
                overall = (
                    results['pixel_similarity'] * 0.7 +
                    results['color_similarity'] * 0.15 +
                    results['hash_similarity'] * 0.15
                )
            else:
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
            logger.error(f"Error comparando imágenes: {e}")
            return self._default_results()
    
    def _compare_pixels_fast(self, img1, img2):
        """Comparación ultra-rápida de píxeles optimizada para imágenes idénticas"""
        try:
            if img1.size == img2.size:
                diff = ImageChops.difference(img1, img2)
                stat = ImageStat.Stat(diff)
                mean_diff = sum(stat.mean) / len(stat.mean)
                
                if mean_diff < 2.0:
                    return 1.0
            
            size = (200, 150)
            img1_small = img1.resize(size, Image.Resampling.LANCZOS)
            img2_small = img2.resize(size, Image.Resampling.LANCZOS)
            
            pixels1 = list(img1_small.getdata())
            pixels2 = list(img2_small.getdata())
            
            identical_pixels = 0
            total_pixels = len(pixels1)
            
            for p1, p2 in zip(pixels1, pixels2):
                if self._color_distance(p1, p2) < 8:
                    identical_pixels += 1
            
            similarity = identical_pixels / total_pixels
            
            if similarity > 0.99:
                similarity = 1.0
            elif similarity > 0.97:
                similarity = min(1.0, similarity * 1.03)
            elif similarity > 0.94:
                similarity = min(1.0, similarity * 1.02)
                
            return similarity
            
        except Exception as e:
            logger.error(f"Error en píxeles: {e}")
            return 0.0
    
    def _compare_hashes_fast(self, img1, img2):
        """Hash perceptual optimizado"""
        try:
            def enhanced_hash(img):
                thumb = img.resize((16, 16), Image.Resampling.LANCZOS).convert('L')
                pixels = list(thumb.getdata())
                avg = sum(pixels) / len(pixels)
                return ''.join('1' if p > avg else '0' for p in pixels)
            
            hash1 = enhanced_hash(img1)
            hash2 = enhanced_hash(img2)
            
            if hash1 == hash2:
                return 1.0
            
            matches = sum(h1 == h2 for h1, h2 in zip(hash1, hash2))
            similarity = matches / len(hash1)
            
            if similarity > 0.98:
                similarity = 1.0
            elif similarity > 0.95:
                similarity = min(1.0, similarity * 1.02)
            
            return similarity
            
        except Exception as e:
            logger.error(f"Error en hash: {e}")
            return 0.0
    
    def _compare_histograms_fast(self, img1, img2):
        """Comparación rápida de histogramas"""
        try:
            small1 = img1.resize((100, 75), Image.Resampling.LANCZOS)
            small2 = img2.resize((100, 75), Image.Resampling.LANCZOS)
            
            hist1 = small1.histogram()
            hist2 = small2.histogram()
            
            correlation = 0
            total1 = sum(hist1) or 1
            total2 = sum(hist2) or 1
            
            for h1, h2 in zip(hist1, hist2):
                correlation += min(h1/total1, h2/total2)
            
            return correlation
            
        except Exception as e:
            logger.error(f"Error en histograma: {e}")
            return 0.0
    
    def _compare_stats_fast(self, img1, img2):
        """Estadísticas rápidas"""
        try:
            stat1 = ImageStat.Stat(img1)
            stat2 = ImageStat.Stat(img2)
            
            mean1 = stat1.mean
            mean2 = stat2.mean
            
            mean_diff = sum(abs(m1 - m2) for m1, m2 in zip(mean1, mean2)) / len(mean1)
            similarity = max(0, 1 - mean_diff / 255)
            
            return similarity
            
        except Exception as e:
            logger.error(f"Error en stats: {e}")
            return 0.0
    
    def _compare_structure_fast(self, img1, img2):
        """Comparación estructural ultra-rápida"""
        try:
            size = (50, 40)
            img1_tiny = img1.resize(size, Image.Resampling.LANCZOS)
            img2_tiny = img2.resize(size, Image.Resampling.LANCZOS)
            
            diff = ImageChops.difference(img1_tiny, img2_tiny)
            stat = ImageStat.Stat(diff)
            mean_diff = sum(stat.mean) / len(stat.mean)
            
            similarity = max(0, 1 - mean_diff / 128)
            return similarity
            
        except Exception as e:
            logger.error(f"Error en estructura: {e}")
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
    """Página principal con interfaz integrada"""
    return """
<!DOCTYPE html>
<html>
<head>
    <title>🖼️ Comparador de Imágenes MercadoPago</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #00A6D6 0%, #009EE3 100%);
            margin: 0; padding: 20px; color: white; text-align: center; min-height: 100vh;
        }
        .container { max-width: 1000px; margin: 0 auto; }
        .header { 
            background: rgba(0, 166, 214, 0.2); padding: 30px; border-radius: 15px; 
            margin: 20px 0; border: 1px solid rgba(255,255,255,0.2);
            backdrop-filter: blur(10px);
        }
        .title-container { 
            display: flex; align-items: center; justify-content: center; 
            gap: 20px; flex-wrap: wrap; margin-bottom: 15px;
        }
        .mp-logo { 
            width: 60px; height: 60px; transition: transform 0.3s ease;
            filter: drop-shadow(0 4px 8px rgba(0,0,0,0.3));
        }
        .mp-logo:hover { transform: scale(1.1) rotate(5deg); }
        .upload-grid { 
            display: grid; grid-template-columns: 1fr auto 1fr; 
            gap: 30px; align-items: center; margin: 30px 0; 
        }
        .upload-area { 
            border: 3px dashed rgba(255,255,255,0.8); padding: 50px 30px; 
            border-radius: 15px; cursor: pointer; transition: all 0.3s; 
            min-height: 250px; display: flex; flex-direction: column; 
            justify-content: center; background: rgba(255,255,255,0.05);
            backdrop-filter: blur(5px);
        }
        .upload-area:hover { 
            background: rgba(255,255,255,0.15); transform: translateY(-5px); 
            box-shadow: 0 10px 25px rgba(0,0,0,0.3);
        }
        .upload-area.has-file { 
            border-color: #4CAF50; background: rgba(76, 175, 80, 0.2); 
        }
        .vs-divider { 
            width: 80px; height: 80px; 
            background: linear-gradient(135deg, #00A6D6, #009EE3);
            border-radius: 50%; display: flex; align-items: center; 
            justify-content: center; font-weight: bold; font-size: 24px; 
            color: white; box-shadow: 0 8px 16px rgba(0,0,0,0.4);
            border: 3px solid rgba(255,255,255,0.3);
        }
        .btn { 
            background: linear-gradient(45deg, #4CAF50, #45a049); 
            color: white; padding: 18px 40px; border: none; 
            border-radius: 50px; cursor: pointer; margin: 30px; 
            font-size: 18px; font-weight: bold; transition: all 0.3s; 
            box-shadow: 0 6px 12px rgba(0,0,0,0.3);
            text-transform: uppercase; letter-spacing: 1px;
        }
        .btn:hover { 
            background: linear-gradient(45deg, #45a049, #4CAF50); 
            transform: translateY(-3px); 
            box-shadow: 0 8px 16px rgba(0,0,0,0.4);
        }
        .btn:disabled { 
            background: #666; cursor: not-allowed; transform: none; 
            box-shadow: none;
        }
        .result { 
            background: rgba(255,255,255,0.15); padding: 40px; 
            border-radius: 20px; margin: 30px 0; backdrop-filter: blur(15px); 
            border: 1px solid rgba(255,255,255,0.3);
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }
        .similarity-score { 
            font-size: 5em; font-weight: bold; margin: 30px 0; 
            text-shadow: 0 4px 8px rgba(0,0,0,0.3);
            background: linear-gradient(45deg, #FFD700, #FFA500);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        .details-grid { 
            display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
            gap: 20px; margin: 30px 0; 
        }
        .detail-item { 
            background: rgba(255,255,255,0.15); padding: 20px; 
            border-radius: 15px; border-left: 5px solid #4CAF50;
            backdrop-filter: blur(5px); transition: transform 0.3s;
        }
        .detail-item:hover { transform: translateY(-2px); }
        .detail-label { font-size: 0.95em; opacity: 0.9; margin-bottom: 8px; }
        .detail-value { font-size: 1.4em; font-weight: bold; }
        .image-preview { 
            max-width: 100%; height: 220px; object-fit: cover; 
            border-radius: 12px; margin-top: 15px; 
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        }
        .loading { animation: pulse 1.5s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .speed-badge { 
            background: linear-gradient(45deg, #ff6b6b, #ff5252); 
            color: white; padding: 8px 15px; border-radius: 25px; 
            font-size: 0.85em; margin-left: 15px; font-weight: bold;
            box-shadow: 0 3px 6px rgba(0,0,0,0.3);
        }
        .conclusion { 
            margin-top: 25px; font-size: 1.2em; padding: 20px; 
            background: rgba(255,255,255,0.1); border-radius: 12px;
            border: 1px solid rgba(255,255,255,0.2);
        }
        @media (max-width: 768px) { 
            .upload-grid { grid-template-columns: 1fr; gap: 20px; }
            .vs-divider { width: 60px; height: 60px; font-size: 18px; }
            .similarity-score { font-size: 3.5em; }
            .title-container { flex-direction: column; gap: 15px; }
            .mp-logo { width: 50px; height: 50px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="title-container">
                <svg class="mp-logo" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
                    <defs>
                        <linearGradient id="mpGradient" x1="0%" y1="0%" x2="100%" y2="100%">
                            <stop offset="0%" style="stop-color:#00B9FF;stop-opacity:1" />
                            <stop offset="100%" style="stop-color:#009EE3;stop-opacity:1" />
                        </linearGradient>
                    </defs>
                    <circle cx="50" cy="50" r="45" fill="url(#mpGradient)" stroke="rgba(255,255,255,0.4)" stroke-width="2"/>
                    <text x="50" y="58" text-anchor="middle" fill="white" font-family="Arial, sans-serif" font-weight="bold" font-size="20">MP</text>
                </svg>
                <h1>🖼️ Comparador de Imágenes <span class="speed-badge">⚡ ULTRA RÁPIDO</span></h1>
            </div>
            <p style="font-size: 1.1em; margin: 0;">
                ✅ Detección instantánea • 🎯 100% preciso para imágenes idénticas • 🚀 Optimizado para velocidad
            </p>
        </div>
        
        <div class="upload-grid">
            <div class="upload-area" id="area1" onclick="document.getElementById('file1').click()">
                <h3 id="title1">📸 Primera Imagen</h3>
                <p>Haz clic aquí para seleccionar</p>
                <input type="file" id="file1" accept="image/*" style="display:none">
                <img id="preview1" class="image-preview" style="display:none">
            </div>
            
            <div class="vs-divider">VS</div>
            
            <div class="upload-area" id="area2" onclick="document.getElementById('file2').click()">
                <h3 id="title2">📸 Segunda Imagen</h3>
                <p>Haz clic aquí para seleccionar</p>
                <input type="file" id="file2" accept="image/*" style="display:none">
                <img id="preview2" class="image-preview" style="display:none">
            </div>
        </div>
        
        <button class="btn" id="compareBtn" onclick="compareImages()" disabled>⚡ Comparar Imágenes</button>
        
        <div id="result" style="display:none;" class="result">
            <h2>📊 Resultado de Comparación</h2>
            <div class="similarity-score" id="overallScore">--%</div>
            <div class="details-grid">
                <div class="detail-item">
                    <div class="detail-label">🎯 Similitud de Píxeles</div>
                    <div class="detail-value" id="pixelSim">--%</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">🎨 Similitud de Colores</div>
                    <div class="detail-value" id="colorSim">--%</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">🔢 Hash Estructural</div>
                    <div class="detail-value" id="hashSim">--%</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">⏱️ Tiempo de Proceso</div>
                    <div class="detail-value" id="timeValue">--s</div>
                </div>
            </div>
            <div id="conclusion" class="conclusion"></div>
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
            
            fetch('/api/compare-images', {
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
                    conclusion = '🎯 <strong>¡Imágenes idénticas!</strong><br>100% seguro que es la misma imagen';
                } else if (overall >= 90) {
                    conclusion = '✅ <strong>Prácticamente idénticas</strong><br>Muy alta probabilidad de ser la misma imagen';
                } else if (overall >= 75) {
                    conclusion = '🟢 <strong>Imágenes muy similares</strong><br>Mismo contenido o lugar, posibles variaciones menores';
                } else if (overall >= 50) {
                    conclusion = '🟡 <strong>Similitudes moderadas</strong><br>Algunos elementos comunes detectados';
                } else {
                    conclusion = '🔴 <strong>Imágenes diferentes</strong><br>No parecen estar relacionadas';
                }
                
                document.getElementById('conclusion').innerHTML = conclusion;
            })
            .catch(error => {
                document.getElementById('overallScore').innerHTML = '❌';
                document.getElementById('conclusion').innerHTML = '❌ Error procesando imágenes';
                console.error('Error:', error);
            });
        }
    </script>
</body>
</html>
    """

@app.route('/api/compare-images', methods=['POST'])
@app.route('/api/compare-backgrounds', methods=['POST'])
def compare_images():
    """API de comparación de imágenes"""
    try:
        start_time = time.time()
        
        if 'image1' not in request.files or 'image2' not in request.files:
            return jsonify({'error': 'Se requieren dos imágenes'}), 400
        
        file1 = request.files['image1']
        file2 = request.files['image2']
        
        logger.info(f"Comparando: {file1.filename} vs {file2.filename}")
        
        # Cargar imágenes
        img1 = comparator.load_image(file1.stream)
        img2 = comparator.load_image(file2.stream)
        
        if not img1 or not img2:
            return jsonify({'error': 'Error cargando imágenes'}), 400
        
        # Comparación rápida
        results = comparator.compare_images_fast(img1, img2)
        
        processing_time = round(time.time() - start_time, 3)
        
        logger.info(f"Resultado: {results['overall_similarity']:.2%} en {processing_time}s")
        
        # Respuesta optimizada
        response = {
            'overall_similarity': float(results['overall_similarity']),
            'pixel_similarity': float(results['pixel_similarity']),
            'color_similarity': float(results['color_similarity']),
            'texture_similarity': float(results['stats_similarity']),
            'structural_similarity': float(results['structural_similarity']),
            'hash_similarity': float(results['hash_similarity']),
            'stats_similarity': float(results['stats_similarity']),
            'processing_time': processing_time,
            'message': f'Análisis completado en {processing_time}s'
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return jsonify({'error': f'Error procesando imágenes: {str(e)}'}), 500

@app.route('/api/health')
def health():
    """Verificación de salud del servicio"""
    return jsonify({
        'status': 'ok', 
        'version': 'web_1.0',
        'message': 'Comparador de Imágenes Web - Funcionando correctamente'
    })

# Para producción con Gunicorn
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    debug_mode = os.environ.get('FLASK_ENV') != 'production'
    
    logger.info("🌐 Iniciando Comparador de Imágenes Web")
    logger.info(f"🚀 Puerto: {port}")
    
    app.run(debug=debug_mode, host='0.0.0.0', port=port) 