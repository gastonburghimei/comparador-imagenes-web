#!/usr/bin/env python3
"""
Comparador de Fondos - Versión Final
Solo resultados de comparación, sin fondos extraídos
"""

import os
import time
import math

try:
    from flask import Flask, jsonify, request
    from flask_cors import CORS
    from PIL import Image, ImageStat, ImageChops
except ImportError:
    print("❌ Dependencias no instaladas")
    print("💡 Ejecuta: pip3 install flask flask-cors pillow")
    exit(1)

app = Flask(__name__)
CORS(app)

class SimpleImageComparator:
    """Comparador simple y rápido de imágenes"""
    
    def load_image(self, file_stream):
        """Carga imagen optimizada"""
        try:
            image = Image.open(file_stream)
            if image.mode != 'RGB':
                image = image.convert('RGB')
            # Tamaño optimizado para velocidad
            image.thumbnail((400, 400), Image.Resampling.LANCZOS)
            return image
        except Exception as e:
            print(f"Error cargando imagen: {e}")
            return None
    
    def compare_images(self, image1, image2):
        """Comparación directa sin extracción de fondos"""
        try:
            # 1. Comparación de píxeles (más importante)
            pixel_sim = self._compare_pixels(image1, image2)
            
            # 2. Hash perceptual
            hash_sim = self._compare_hash(image1, image2)
            
            # 3. Colores (solo si no son idénticas)
            if pixel_sim > 0.95:
                color_sim = pixel_sim
            else:
                color_sim = self._compare_colors(image1, image2)
            
            # Similitud general optimizada
            if pixel_sim > 0.98:
                overall = pixel_sim
            else:
                overall = pixel_sim * 0.6 + hash_sim * 0.25 + color_sim * 0.15
            
            return {
                'overall_similarity': overall,
                'pixel_similarity': pixel_sim,
                'color_similarity': color_sim,
                'hash_similarity': hash_sim
            }
            
        except Exception as e:
            print(f"Error comparando: {e}")
            return {'overall_similarity': 0.0, 'pixel_similarity': 0.0, 'color_similarity': 0.0, 'hash_similarity': 0.0}
    
    def _compare_pixels(self, img1, img2):
        """Comparación directa de píxeles"""
        try:
            size = (100, 75)
            img1_small = img1.resize(size, Image.Resampling.LANCZOS)
            img2_small = img2.resize(size, Image.Resampling.LANCZOS)
            
            pixels1 = list(img1_small.getdata())
            pixels2 = list(img2_small.getdata())
            
            identical = 0
            for p1, p2 in zip(pixels1, pixels2):
                if abs(p1[0] - p2[0]) + abs(p1[1] - p2[1]) + abs(p1[2] - p2[2]) < 20:
                    identical += 1
            
            similarity = identical / len(pixels1)
            return min(1.0, similarity * 1.02 if similarity > 0.95 else similarity)
        except:
            return 0.0
    
    def _compare_hash(self, img1, img2):
        """Hash perceptual simple"""
        try:
            def get_hash(img):
                thumb = img.resize((8, 8), Image.Resampling.LANCZOS).convert('L')
                pixels = list(thumb.getdata())
                avg = sum(pixels) / len(pixels)
                return ''.join('1' if p > avg else '0' for p in pixels)
            
            hash1 = get_hash(img1)
            hash2 = get_hash(img2)
            
            matches = sum(h1 == h2 for h1, h2 in zip(hash1, hash2))
            return matches / len(hash1)
        except:
            return 0.0
    
    def _compare_colors(self, img1, img2):
        """Comparación de histogramas"""
        try:
            hist1 = img1.resize((50, 50), Image.Resampling.LANCZOS).histogram()
            hist2 = img2.resize((50, 50), Image.Resampling.LANCZOS).histogram()
            
            correlation = 0
            total1 = sum(hist1) or 1
            total2 = sum(hist2) or 1
            
            for h1, h2 in zip(hist1, hist2):
                correlation += min(h1/total1, h2/total2)
            
            return correlation
        except:
            return 0.0

# Instancia del comparador
comparator = SimpleImageComparator()

@app.route('/')
def index():
    """Página principal integrada - SIN archivos externos"""
    return """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🖼️ Comparador de Fondos</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh; color: white; padding: 20px;
        }
        
        .container { max-width: 800px; margin: 0 auto; }
        
        .header { 
            text-align: center; background: rgba(255,255,255,0.1); 
            padding: 30px; border-radius: 15px; margin-bottom: 30px;
            backdrop-filter: blur(10px);
        }
        
        .header h1 { font-size: 2.5em; margin-bottom: 10px; }
        .header p { font-size: 1.1em; opacity: 0.9; }
        
        .upload-section { 
            display: grid; grid-template-columns: 1fr auto 1fr; 
            gap: 30px; align-items: center; margin-bottom: 30px; 
        }
        
        .upload-area { 
            border: 3px dashed rgba(255,255,255,0.6); 
            border-radius: 15px; padding: 50px 20px; text-align: center;
            cursor: pointer; transition: all 0.3s; min-height: 250px;
            display: flex; flex-direction: column; justify-content: center;
        }
        
        .upload-area:hover { 
            border-color: #4CAF50; background: rgba(76,175,80,0.1); 
            transform: translateY(-3px);
        }
        
        .upload-area.has-image { 
            border-color: #4CAF50; background: rgba(76,175,80,0.2); 
        }
        
        .upload-area h3 { font-size: 1.5em; margin-bottom: 10px; }
        .upload-area p { opacity: 0.8; }
        
        .vs-badge { 
            width: 80px; height: 80px; background: linear-gradient(45deg, #4CAF50, #45a049);
            border-radius: 50%; display: flex; align-items: center; justify-content: center;
            font-size: 1.5em; font-weight: bold; box-shadow: 0 8px 16px rgba(0,0,0,0.3);
        }
        
        .preview-img { 
            max-width: 100%; height: 200px; object-fit: cover; 
            border-radius: 10px; margin-top: 15px; display: none;
        }
        
        .compare-btn { 
            display: block; margin: 30px auto; padding: 18px 40px;
            background: linear-gradient(45deg, #4CAF50, #45a049);
            color: white; border: none; border-radius: 25px;
            font-size: 1.2em; font-weight: bold; cursor: pointer;
            transition: all 0.3s; box-shadow: 0 6px 12px rgba(0,0,0,0.3);
        }
        
        .compare-btn:hover:not(:disabled) { 
            transform: translateY(-3px); box-shadow: 0 8px 16px rgba(0,0,0,0.4); 
        }
        
        .compare-btn:disabled { 
            background: #666; cursor: not-allowed; transform: none; 
        }
        
        .results { 
            background: rgba(255,255,255,0.1); border-radius: 15px; 
            padding: 40px; margin-top: 30px; text-align: center;
            backdrop-filter: blur(10px); display: none;
        }
        
        .score { 
            font-size: 4em; font-weight: bold; margin: 20px 0; 
            text-shadow: 0 2px 4px rgba(0,0,0,0.3);
        }
        
        .details { 
            display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
            gap: 20px; margin: 30px 0; 
        }
        
        .detail { 
            background: rgba(255,255,255,0.1); padding: 20px; 
            border-radius: 10px; border-left: 4px solid #4CAF50;
        }
        
        .detail-label { font-size: 0.9em; opacity: 0.8; margin-bottom: 5px; }
        .detail-value { font-size: 1.4em; font-weight: bold; }
        
        .conclusion { 
            background: rgba(255,255,255,0.1); padding: 25px; 
            border-radius: 10px; margin-top: 20px; font-size: 1.1em;
        }
        
        .loading { animation: pulse 1.5s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        
        @media (max-width: 768px) {
            .upload-section { grid-template-columns: 1fr; gap: 20px; }
            .vs-badge { margin: 20px auto; }
            .details { grid-template-columns: 1fr; }
            .score { font-size: 3em; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🖼️ Comparador de Fondos</h1>
            <p>Análisis rápido y preciso de similitud entre imágenes</p>
        </div>
        
        <div class="upload-section">
            <div class="upload-area" id="area1" onclick="document.getElementById('file1').click()">
                <h3 id="title1">📸 Primera Imagen</h3>
                <p>Haz clic para seleccionar</p>
                <input type="file" id="file1" accept="image/*" style="display:none">
                <img id="preview1" class="preview-img">
            </div>
            
            <div class="vs-badge">VS</div>
            
            <div class="upload-area" id="area2" onclick="document.getElementById('file2').click()">
                <h3 id="title2">📸 Segunda Imagen</h3>
                <p>Haz clic para seleccionar</p>
                <input type="file" id="file2" accept="image/*" style="display:none">
                <img id="preview2" class="preview-img">
            </div>
        </div>
        
        <button class="compare-btn" id="compareBtn" onclick="compareImages()" disabled>
            🔍 Comparar Imágenes
        </button>
        
        <div class="results" id="results">
            <h2>📊 Resultados</h2>
            <div class="score" id="score">--%</div>
            
            <div class="details">
                <div class="detail">
                    <div class="detail-label">🎯 Píxeles Similares</div>
                    <div class="detail-value" id="pixelScore">--%</div>
                </div>
                <div class="detail">
                    <div class="detail-label">🎨 Colores</div>
                    <div class="detail-value" id="colorScore">--%</div>
                </div>
                <div class="detail">
                    <div class="detail-label">🔢 Estructura</div>
                    <div class="detail-value" id="hashScore">--%</div>
                </div>
                <div class="detail">
                    <div class="detail-label">⏱️ Tiempo</div>
                    <div class="detail-value" id="timeScore">--s</div>
                </div>
            </div>
            
            <div class="conclusion" id="conclusion">
                Esperando análisis...
            </div>
        </div>
    </div>
    
    <script>
        let file1 = null, file2 = null;
        
        // Configurar inputs de archivos
        document.getElementById('file1').addEventListener('change', (e) => handleFile(e, 1));
        document.getElementById('file2').addEventListener('change', (e) => handleFile(e, 2));
        
        function handleFile(event, num) {
            const file = event.target.files[0];
            if (!file) return;
            
            if (num === 1) file1 = file;
            else file2 = file;
            
            // Actualizar UI
            document.getElementById('title' + num).textContent = '✅ ' + file.name;
            document.getElementById('area' + num).classList.add('has-image');
            
            // Mostrar preview
            const reader = new FileReader();
            reader.onload = (e) => {
                const preview = document.getElementById('preview' + num);
                preview.src = e.target.result;
                preview.style.display = 'block';
            };
            reader.readAsDataURL(file);
            
            // Habilitar botón si ambas imágenes están cargadas
            document.getElementById('compareBtn').disabled = !(file1 && file2);
        }
        
        function compareImages() {
            if (!file1 || !file2) return;
            
            const startTime = performance.now();
            const formData = new FormData();
            formData.append('image1', file1);
            formData.append('image2', file2);
            
            // Mostrar loading
            document.getElementById('results').style.display = 'block';
            document.getElementById('score').textContent = '🔄';
            document.getElementById('score').classList.add('loading');
            
            fetch('/api/compare', { method: 'POST', body: formData })
                .then(response => response.json())
                .then(data => {
                    const time = ((performance.now() - startTime) / 1000).toFixed(2);
                    
                    document.getElementById('score').classList.remove('loading');
                    
                    const overall = Math.round(data.overall_similarity * 100);
                    document.getElementById('score').textContent = overall + '%';
                    document.getElementById('pixelScore').textContent = Math.round(data.pixel_similarity * 100) + '%';
                    document.getElementById('colorScore').textContent = Math.round(data.color_similarity * 100) + '%';
                    document.getElementById('hashScore').textContent = Math.round(data.hash_similarity * 100) + '%';
                    document.getElementById('timeScore').textContent = time + 's';
                    
                    // Conclusión
                    let conclusion;
                    if (overall >= 98) conclusion = '🎯 <strong>Imágenes idénticas</strong> - Son la misma imagen';
                    else if (overall >= 85) conclusion = '✅ <strong>Muy similares</strong> - Muy alta probabilidad de ser iguales';
                    else if (overall >= 70) conclusion = '🟢 <strong>Similares</strong> - Probablemente relacionadas';
                    else if (overall >= 40) conclusion = '🟡 <strong>Algo similares</strong> - Algunas características comunes';
                    else conclusion = '🔴 <strong>Diferentes</strong> - No parecen relacionadas';
                    
                    document.getElementById('conclusion').innerHTML = conclusion;
                })
                .catch(error => {
                    document.getElementById('score').textContent = '❌';
                    console.error('Error:', error);
                });
        }
    </script>
</body>
</html>
    """

@app.route('/api/compare', methods=['POST'])
def compare_images():
    """API simplificada - solo comparación directa"""
    try:
        start_time = time.time()
        
        if 'image1' not in request.files or 'image2' not in request.files:
            return jsonify({'error': 'Se requieren dos imágenes'}), 400
        
        file1 = request.files['image1']
        file2 = request.files['image2']
        
        print(f"🔍 Comparando: {file1.filename} vs {file2.filename}")
        
        # Cargar y comparar
        img1 = comparator.load_image(file1.stream)
        img2 = comparator.load_image(file2.stream)
        
        if not img1 or img2:
            return jsonify({'error': 'Error cargando imágenes'}), 400
        
        results = comparator.compare_images(img1, img2)
        processing_time = round(time.time() - start_time, 3)
        
        print(f"📊 Resultado: {results['overall_similarity']:.2%} en {processing_time}s")
        
        return jsonify({
            'overall_similarity': results['overall_similarity'],
            'pixel_similarity': results['pixel_similarity'],
            'color_similarity': results['color_similarity'],
            'hash_similarity': results['hash_similarity'],
            'processing_time': processing_time
        })
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/health')
def health():
    return jsonify({'status': 'ok', 'version': 'final', 'message': 'Solo comparación directa'})

# Bloquear archivos externos que podrían tener código de fondos extraídos
@app.route('/main.css')
@app.route('/main.js') 
@app.route('/<path:filename>')
def block_external_files(filename=None):
    """Bloquear archivos externos para evitar código de fondos extraídos"""
    return "Archivo no disponible en versión final", 404

if __name__ == '__main__':
    print("🚀 Comparador de Fondos - Versión Final")
    print("🎯 Solo comparación directa - Sin fondos extraídos")
    print("⚡ Interfaz integrada - Sin archivos externos")
    print("🌐 URL: http://localhost:8080")
    print("🛑 Presiona Ctrl+C para detener")
    print("-" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=8080) 