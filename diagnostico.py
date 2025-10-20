#!/usr/bin/env python3
"""
Script de diagnóstico para el Comparador de Fondos de Imágenes
Identifica y soluciona problemas comunes de instalación y ejecución
"""

import sys
import os
import subprocess
import importlib
import socket
import urllib.request
import urllib.error

def print_section(title):
    """Imprime una sección con formato"""
    print(f"\n{'='*60}")
    print(f"🔍 {title}")
    print('='*60)

def check_python_version():
    """Verifica la versión de Python"""
    print_section("VERIFICACIÓN DE PYTHON")
    
    version = sys.version_info
    print(f"📍 Versión de Python: {version.major}.{version.minor}.{version.micro}")
    
    if version < (3, 8):
        print("❌ ERROR: Se requiere Python 3.8 o superior")
        print("💡 Solución: Actualiza Python desde https://python.org")
        return False
    else:
        print("✅ Versión de Python compatible")
        return True

def check_files():
    """Verifica que todos los archivos necesarios existan"""
    print_section("VERIFICACIÓN DE ARCHIVOS")
    
    required_files = [
        'main.html',
        'main.css', 
        'main.js',
        'app.py',
        'requirements.txt'
    ]
    
    missing_files = []
    for file in required_files:
        if os.path.exists(file):
            size = os.path.getsize(file)
            print(f"✅ {file} - {size} bytes")
        else:
            missing_files.append(file)
            print(f"❌ {file} - NO ENCONTRADO")
    
    if missing_files:
        print(f"\n❌ FALTAN ARCHIVOS: {', '.join(missing_files)}")
        print("💡 Solución: Asegúrate de tener todos los archivos del proyecto")
        return False
    
    print("✅ Todos los archivos necesarios están presentes")
    return True

def check_dependencies():
    """Verifica las dependencias de Python"""
    print_section("VERIFICACIÓN DE DEPENDENCIAS")
    
    dependencies = {
        'flask': 'Flask',
        'flask_cors': 'Flask-CORS', 
        'PIL': 'Pillow',
        'cv2': 'opencv-python',
        'numpy': 'numpy',
        'sklearn': 'scikit-learn',
        'scipy': 'scipy',
        'skimage': 'scikit-image'
    }
    
    missing = []
    installed = []
    
    for module, package in dependencies.items():
        try:
            importlib.import_module(module)
            installed.append(package)
            print(f"✅ {package} - Instalado")
        except ImportError:
            missing.append(package)
            print(f"❌ {package} - NO INSTALADO")
    
    if missing:
        print(f"\n❌ DEPENDENCIAS FALTANTES: {', '.join(missing)}")
        print("💡 Solución: Ejecuta uno de estos comandos:")
        print("   pip install -r requirements.txt")
        print("   o manualmente: pip install " + " ".join(missing))
        return False
    
    print("✅ Todas las dependencias están instaladas")
    return True

def check_port():
    """Verifica si el puerto 5000 está disponible"""
    print_section("VERIFICACIÓN DE PUERTO")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('localhost', 5000))
        sock.close()
        
        if result == 0:
            print("⚠️ El puerto 5000 está ocupado")
            print("💡 Soluciones:")
            print("   1. Detén otras aplicaciones que usen el puerto 5000")
            print("   2. O modifica el puerto en app.py (línea final)")
            return False
        else:
            print("✅ Puerto 5000 disponible")
            return True
            
    except Exception as e:
        print(f"⚠️ No se pudo verificar el puerto: {e}")
        return True

def test_app_import():
    """Prueba importar la aplicación"""
    print_section("PRUEBA DE IMPORTACIÓN")
    
    try:
        # Cambiar al directorio del script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
        
        print("🔄 Intentando importar app.py...")
        from app import app, BackgroundComparator
        print("✅ app.py se importó correctamente")
        
        print("🔄 Probando BackgroundComparator...")
        comparator = BackgroundComparator()
        print("✅ BackgroundComparator funciona")
        
        return True
        
    except ImportError as e:
        print(f"❌ Error de importación: {e}")
        print("💡 Posibles soluciones:")
        print("   1. Verifica que todas las dependencias estén instaladas")
        print("   2. Ejecuta: pip install -r requirements.txt")
        return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False

def test_server_start():
    """Prueba iniciar el servidor"""
    print_section("PRUEBA DEL SERVIDOR")
    
    try:
        print("🔄 Intentando iniciar servidor en segundo plano...")
        
        # Ejecutar el servidor en un proceso separado
        process = subprocess.Popen(
            [sys.executable, 'app.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Esperar un poco para que el servidor inicie
        import time
        time.sleep(3)
        
        # Verificar si el proceso sigue corriendo
        if process.poll() is None:
            print("✅ Servidor iniciado correctamente")
            
            # Probar conexión HTTP
            try:
                response = urllib.request.urlopen('http://localhost:5000', timeout=5)
                print("✅ Respuesta HTTP recibida")
                print(f"📊 Código de estado: {response.getcode()}")
                
                # Terminar el proceso
                process.terminate()
                process.wait(timeout=5)
                
                return True
                
            except urllib.error.URLError as e:
                print(f"❌ Error de conexión HTTP: {e}")
                process.terminate()
                return False
            except Exception as e:
                print(f"❌ Error probando HTTP: {e}")
                process.terminate()
                return False
        else:
            # El proceso terminó, leer el error
            stdout, stderr = process.communicate()
            print("❌ El servidor no pudo iniciarse")
            if stderr:
                print(f"Error: {stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error iniciando servidor: {e}")
        return False

def provide_solutions():
    """Proporciona soluciones comunes"""
    print_section("SOLUCIONES COMUNES")
    
    print("🛠️ Si tienes problemas, prueba estos pasos en orden:")
    print()
    print("1️⃣ INSTALAR DEPENDENCIAS:")
    print("   pip install -r requirements.txt")
    print()
    print("2️⃣ USAR EL SCRIPT DE INICIO:")
    print("   python run.py")
    print()
    print("3️⃣ PROBAR MANUALMENTE:")
    print("   python app.py")
    print()
    print("4️⃣ PROBAR MODO DEMO:")
    print("   python demo.py")
    print()
    print("5️⃣ VERIFICAR NAVEGADOR:")
    print("   Abre: http://localhost:5000")
    print("   Prueba otro navegador si es necesario")
    print()
    print("6️⃣ PROBLEMAS DE PUERTO:")
    print("   Cambia el puerto en app.py (última línea):")
    print("   app.run(debug=True, host='0.0.0.0', port=8080)")
    print()
    print("7️⃣ REINSTALAR DESDE CERO:")
    print("   pip uninstall -r requirements.txt -y")
    print("   pip install -r requirements.txt")

def main():
    """Función principal de diagnóstico"""
    print("🏥 DIAGNÓSTICO DEL COMPARADOR DE FONDOS")
    print("Este script identificará problemas comunes")
    
    # Lista de verificaciones
    checks = [
        ("Python", check_python_version),
        ("Archivos", check_files),
        ("Dependencias", check_dependencies),
        ("Puerto", check_port),
        ("Importación", test_app_import),
        ("Servidor", test_server_start)
    ]
    
    results = {}
    
    # Ejecutar verificaciones
    for name, check_func in checks:
        try:
            results[name] = check_func()
        except Exception as e:
            print(f"❌ Error en verificación {name}: {e}")
            results[name] = False
    
    # Resumen final
    print_section("RESUMEN DEL DIAGNÓSTICO")
    
    all_good = True
    for name, result in results.items():
        if result:
            print(f"✅ {name}: OK")
        else:
            print(f"❌ {name}: PROBLEMA")
            all_good = False
    
    print()
    if all_good:
        print("🎉 ¡TODO ESTÁ BIEN! La aplicación debería funcionar.")
        print("🌐 Accede a: http://localhost:5000")
    else:
        print("⚠️ Se encontraron problemas. Revisa las soluciones abajo.")
        provide_solutions()
    
    print(f"\n💡 Si sigues teniendo problemas:")
    print(f"   1. Ejecuta: python demo.py (para probar sin servidor)")
    print(f"   2. Revisa el README.md para más detalles")
    print(f"   3. Verifica la documentación de instalación")

if __name__ == "__main__":
    main() 