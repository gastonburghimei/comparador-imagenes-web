#!/usr/bin/env python3
"""
Versión rápida del Comparador de Fondos - Puerto 8080
Evita el conflicto con AirPlay en macOS
"""

import os
import random
import time
import base64
import io

try:
    from flask import Flask, send_from_directory, jsonify, request
    from flask_cors import CORS
except ImportError:
    print("❌ Flask no está instalado")
    print("💡 Ejecuta: pip3 install flask flask-cors")
    exit(1)

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

app = Flask(__name__)
CORS(app)

@app.route('/')
def index():
    """Servir la página principal"""
    if os.path.exists('main.html'):
        return send_from_directory('.', 'main.html')
    else:
        return """
<!DOCTYPE html>
<html>
<head>
    <title>🖼️ Comparador de Fondos</title>
    <meta charset="UTF-8">
    <style>
        body { 
            font-family: Arial, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            margin: 0; padding: 20px; color: white; text-align: center; 
        }
        .container { max-width: 800px; margin: 0 auto; }
        .success { background: rgba(72, 187, 120, 0.2); padding: 20px; border-radius: 10px; margin: 20px 0; }
        .upload-area { 
            border: 3px dashed #fff; padding: 40px; margin: 20px 0; 
            border-radius: 10px; cursor: pointer; 
        }
        .btn { 
            background: #4CAF50; color: white; padding: 15px 30px; 
            border: none; border-radius: 5px; cursor: pointer; margin: 10px; font-size: 16px;
        }
        .btn:hover { background: #45a049; }
        .result { background: rgba(255,255,255,0.1); padding: 20px; border-radius: 10px; margin: 20px 0; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🖼️ Comparador de Fondos de Imágenes</h1>
        <div class="success">
            <h2>✅ ¡Servidor Funcionando!</h2>
            <p>Puerto: 8080 | Modo: Demo Simplificado</p>
        </div>
        
        <div class="upload-area" onclick="document.getElementById('file1').click()">
            <h3>📸 Imagen 1</h3>
            <p>Haz clic para seleccionar imagen</p>
            <input type="file" id="file1" accept="image/*" style="display:none">
        </div>
        
        <div class="upload-area" onclick="document.getElementById('file2').click()">
            <h3>📸 Imagen 2</h3>
            <p>Haz clic para seleccionar imagen</p>
            <input type="file" id="file2" accept="image/*" style="display:none">
        </div>
        
        <button class="btn" onclick="compareImages()">🔍 Comparar Fondos</button>
        
        <div id="result" style="display:none;" class="result">
            <h3>📊 Resultados</h3>
            <div id="similarity"></div>
            <div id="details"></div>
        </div>
    </div>
    
    <script>
        let file1 = null, file2 = null;
        
        document.getElementById('file1').addEventListener('change', (e) => {
            file1 = e.target.files[0];
            if(file1) document.querySelector('.upload-area:first-of-type h3').innerHTML = '✅ ' + file1.name;
        });
        
        document.getElementById('file2').addEventListener('change', (e) => {
            file2 = e.target.files[0];
            if(file2) document.querySelector('.upload-area:last-of-type h3').innerHTML = '✅ ' + file2.name;
        });
        
        function compareImages() {
            if(!file1 || !file2) {
                alert('Por favor selecciona ambas imágenes');
                return;
            }
            
            const formData = new FormData();
            formData.append('image1', file1);
            formData.append('image2', file2);
            
            document.getElementById('similarity').innerHTML = '🔄 Procesando...';
            document.getElementById('result').style.display = 'block';
            
            fetch('/api/compare-backgrounds', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                const similarity = Math.round(data.overall_similarity * 100);
                document.getElementById('similarity').innerHTML = 
                    `<h2>Similitud: ${similarity}%</h2>`;
                document.getElementById('details').innerHTML = 
                    `<p>🎨 Color: ${Math.round(data.color_similarity * 100)}%</p>
                     <p>🖼️ Textura: ${Math.round(data.texture_similarity * 100)}%</p>
                     <p>🏗️ Estructura: ${Math.round(data.structural_similarity * 100)}%</p>
                     <p><em>${data.message}</em></p>`;
            })
            .catch(error => {
                document.getElementById('similarity').innerHTML = '❌ Error: ' + error;
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
    """API demo para comparar fondos"""
    time.sleep(1)  # Simular procesamiento
    
    # Generar resultados demo realistas
    base_similarity = random.uniform(0.3, 0.8)
    
    return jsonify({
        'overall_similarity': base_similarity,
        'color_similarity': base_similarity + random.uniform(-0.1, 0.1),
        'texture_similarity': base_similarity + random.uniform(-0.1, 0.1), 
        'structural_similarity': base_similarity + random.uniform(-0.1, 0.1),
        'message': '✅ Comparación demo completada (puerto 8080)'
    })

@app.route('/api/health')
def health():
    """Verificación de salud"""
    return jsonify({'status': 'ok', 'port': 8080, 'message': 'Funcionando correctamente'})

if __name__ == '__main__':
    print("🚀 Iniciando Comparador de Fondos")
    print("🔧 Usando puerto 8080 (evitando conflicto con AirPlay)")
    print("🌐 URL: http://localhost:8080")
    print("🛑 Presiona Ctrl+C para detener")
    print("-" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=8080) 