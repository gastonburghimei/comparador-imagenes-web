#!/usr/bin/env python3
"""
Versión simplificada del Comparador de Fondos de Imágenes
Funciona con dependencias mínimas y modo demo siempre activo
"""

import os
import sys
import base64
import io
import random
import time
from urllib.parse import quote
import json

# Intentar importar Flask, si no está disponible, usar servidor HTTP básico
try:
    from flask import Flask, request, jsonify, send_from_directory
    from flask_cors import CORS
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False
    print("⚠️ Flask no disponible, usando servidor HTTP básico")

# Intentar importar PIL para procesamiento básico de imágenes
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("⚠️ PIL no disponible, usando modo demo completo")

def create_demo_app():
    """Crea la aplicación demo con Flask si está disponible"""
    if not FLASK_AVAILABLE:
        return None
    
    app = Flask(__name__)
    
    try:
        CORS(app)
    except:
        pass  # Si CORS no está disponible, continuar sin él
    
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB
    
    @app.route('/')
    def index():
        """Servir la página principal"""
        if os.path.exists('main.html'):
            return send_from_directory('.', 'main.html')
        else:
            return create_basic_html()
    
    @app.route('/<path:filename>')
    def serve_static(filename):
        """Servir archivos estáticos"""
        if os.path.exists(filename):
            return send_from_directory('.', filename)
        else:
            return "Archivo no encontrado", 404
    
    @app.route('/api/compare-backgrounds', methods=['POST'])
    def compare_backgrounds():
        """Endpoint demo para comparar fondos"""
        try:
            # Generar resultados demo realistas
            time.sleep(1)  # Simular procesamiento
            
            # Resultados aleatorios pero realistas
            base_similarity = random.uniform(0.2, 0.8)
            noise = random.uniform(-0.1, 0.1)
            
            results = {
                'overall_similarity': max(0, min(1, base_similarity)),
                'color_similarity': max(0, min(1, base_similarity + noise)),
                'texture_similarity': max(0, min(1, base_similarity + noise)),
                'structural_similarity': max(0, min(1, base_similarity + noise)),
                'shape_similarity': max(0, min(1, base_similarity + noise)),
                'processed_image1': create_demo_image_b64(),
                'processed_image2': create_demo_image_b64(),
                'message': '✅ Comparación demo completada (versión simplificada)'
            }
            
            return jsonify(results)
            
        except Exception as e:
            return jsonify({'error': f'Error en modo demo: {str(e)}'}), 500
    
    @app.route('/api/health', methods=['GET'])
    def health_check():
        """Endpoint de verificación"""
        return jsonify({
            'status': 'ok', 
            'message': 'Servidor demo funcionando',
            'mode': 'simplified'
        })
    
    return app

def create_demo_image_b64():
    """Crea una imagen demo en base64"""
    if PIL_AVAILABLE:
        try:
            # Crear imagen simple con PIL
            img = Image.new('RGB', (200, 150), color=(
                random.randint(100, 255),
                random.randint(100, 255), 
                random.randint(100, 255)
            ))
            
            buffer = io.BytesIO()
            img.save(buffer, format='JPEG')
            buffer.seek(0)
            return base64.b64encode(buffer.getvalue()).decode()
        except:
            pass
    
    # Placeholder base64 de una imagen 1x1 transparente
    return "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=="

def create_basic_html():
    """Crea HTML básico si main.html no existe"""
    return '''
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Comparador de Fondos - Modo Simplificado</title>
    <style>
        body { 
            font-family: Arial, sans-serif; 
            margin: 0; 
            padding: 20px; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: white;
        }
        .container { 
            max-width: 800px; 
            margin: 0 auto; 
            text-align: center; 
        }
        .warning {
            background: rgba(255, 255, 255, 0.1);
            padding: 20px;
            border-radius: 10px;
            margin: 20px 0;
        }
        .btn {
            background: #4CAF50;
            color: white;
            padding: 10px 20px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            margin: 10px;
            text-decoration: none;
            display: inline-block;
        }
        .btn:hover { background: #45a049; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🖼️ Comparador de Fondos</h1>
        <h2>Modo Simplificado</h2>
        
        <div class="warning">
            <h3>⚠️ Ejecutando en modo simplificado</h3>
            <p>Algunos archivos o dependencias no están disponibles.</p>
            <p>Esta es una versión básica para verificar que el servidor funciona.</p>
        </div>
        
        <div>
            <h3>📋 Para la versión completa:</h3>
            <p>1. Asegúrate de tener todos los archivos (main.html, main.css, main.js)</p>
            <p>2. Instala las dependencias: <code>pip install -r requirements.txt</code></p>
            <p>3. Ejecuta: <code>python app.py</code></p>
        </div>
        
        <div>
            <a href="/api/health" class="btn">Probar API</a>
            <button onclick="testDemo()" class="btn">Probar Demo</button>
        </div>
        
        <div id="result" style="margin-top: 20px;"></div>
    </div>
    
    <script>
        function testDemo() {
            document.getElementById('result').innerHTML = '🔄 Probando...';
            
            // Simular llamada a la API
            fetch('/api/compare-backgrounds', {
                method: 'POST',
                body: new FormData()
            })
            .then(response => response.json())
            .then(data => {
                document.getElementById('result').innerHTML = 
                    `✅ Demo funcionando<br>
                     Similitud: ${Math.round(data.overall_similarity * 100)}%<br>
                     Mensaje: ${data.message}`;
            })
            .catch(error => {
                document.getElementById('result').innerHTML = 
                    `❌ Error: ${error}`;
            });
        }
    </script>
</body>
</html>
    '''

def start_basic_server():
    """Inicia servidor HTTP básico si Flask no está disponible"""
    import http.server
    import socketserver
    from functools import partial
    
    class CustomHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/':
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(create_basic_html().encode())
            else:
                super().do_GET()
        
        def do_POST(self):
            if self.path == '/api/compare-backgrounds':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                # Respuesta demo
                demo_response = {
                    'overall_similarity': random.uniform(0.3, 0.7),
                    'color_similarity': random.uniform(0.2, 0.8),
                    'texture_similarity': random.uniform(0.2, 0.8),
                    'structural_similarity': random.uniform(0.2, 0.8),
                    'message': 'Demo básico sin Flask'
                }
                
                self.wfile.write(json.dumps(demo_response).encode())
            else:
                self.send_response(404)
                self.end_headers()
    
    PORT = 5000
    
    # Intentar diferentes puertos si 5000 está ocupado
    for port in range(5000, 5010):
        try:
            with socketserver.TCPServer(("", port), CustomHandler) as httpd:
                print(f"🌐 Servidor básico corriendo en http://localhost:{port}")
                print("🛑 Presiona Ctrl+C para detener")
                httpd.serve_forever()
                break
        except OSError:
            print(f"⚠️ Puerto {port} ocupado, probando {port + 1}...")
            continue

def main():
    """Función principal"""
    print("🚀 Iniciando Comparador de Fondos - Modo Simplificado")
    print("=" * 50)
    
    # Verificar archivos básicos
    files_status = []
    required_files = ['main.html', 'main.css', 'main.js', 'app.py']
    
    for file in required_files:
        if os.path.exists(file):
            files_status.append(f"✅ {file}")
        else:
            files_status.append(f"❌ {file}")
    
    print("📁 Estado de archivos:")
    for status in files_status:
        print(f"   {status}")
    
    # Verificar dependencias
    print(f"\n📦 Dependencias:")
    print(f"   Flask: {'✅' if FLASK_AVAILABLE else '❌'}")
    print(f"   PIL: {'✅' if PIL_AVAILABLE else '❌'}")
    
    if FLASK_AVAILABLE:
        print(f"\n🔄 Iniciando con Flask...")
        try:
            app = create_demo_app()
            if app:
                print(f"🌐 Servidor corriendo en: http://localhost:5000")
                print(f"🛑 Presiona Ctrl+C para detener")
                app.run(debug=True, host='0.0.0.0', port=5000)
            else:
                raise Exception("No se pudo crear la app")
        except Exception as e:
            print(f"❌ Error con Flask: {e}")
            print(f"🔄 Cambiando a servidor básico...")
            start_basic_server()
    else:
        print(f"\n🔄 Iniciando servidor HTTP básico...")
        start_basic_server()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n👋 Servidor detenido")
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        print(f"\n💡 Prueba ejecutar: python diagnostico.py")
        sys.exit(1) 