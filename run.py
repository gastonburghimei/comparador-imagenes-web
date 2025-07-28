#!/usr/bin/env python3
"""
Script de inicio para el Comparador de Fondos de Imágenes
Verifica dependencias y ejecuta la aplicación
"""

import sys
import subprocess
import importlib
import os

def check_python_version():
    """Verifica que la versión de Python sea compatible"""
    if sys.version_info < (3, 8):
        print("❌ Error: Se requiere Python 3.8 o superior")
        print(f"   Tu versión: {sys.version}")
        sys.exit(1)
    else:
        print(f"✅ Python {sys.version.split()[0]} - Compatible")

def check_dependencies():
    """Verifica que las dependencias estén instaladas"""
    required_packages = [
        'flask',
        'PIL',
        'cv2',
        'numpy',
        'sklearn',
        'scipy',
        'skimage'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            if package == 'PIL':
                importlib.import_module('PIL')
            elif package == 'cv2':
                importlib.import_module('cv2')
            elif package == 'skimage':
                importlib.import_module('skimage')
            else:
                importlib.import_module(package)
            print(f"✅ {package} - Instalado")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} - No encontrado")
    
    return missing_packages

def install_dependencies():
    """Instala las dependencias faltantes"""
    print("\n📦 Instalando dependencias...")
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
        print("✅ Dependencias instaladas correctamente")
        return True
    except subprocess.CalledProcessError:
        print("❌ Error instalando dependencias")
        return False

def check_files():
    """Verifica que todos los archivos necesarios estén presentes"""
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
            print(f"✅ {file} - Encontrado")
        else:
            missing_files.append(file)
            print(f"❌ {file} - No encontrado")
    
    return missing_files

def main():
    """Función principal"""
    print("🚀 Iniciando Comparador de Fondos de Imágenes...")
    print("=" * 50)
    
    # Verificar versión de Python
    print("\n🐍 Verificando Python...")
    check_python_version()
    
    # Verificar archivos
    print("\n📁 Verificando archivos...")
    missing_files = check_files()
    if missing_files:
        print(f"\n❌ Faltan archivos: {', '.join(missing_files)}")
        print("   Asegúrate de tener todos los archivos del proyecto")
        sys.exit(1)
    
    # Verificar dependencias
    print("\n📦 Verificando dependencias...")
    missing_packages = check_dependencies()
    
    if missing_packages:
        print(f"\n⚠️  Faltan paquetes: {', '.join(missing_packages)}")
        response = input("¿Quieres instalarlos automáticamente? (s/n): ")
        
        if response.lower() in ['s', 'si', 'y', 'yes']:
            if not install_dependencies():
                sys.exit(1)
        else:
            print("💡 Instala las dependencias manualmente con:")
            print("   pip install -r requirements.txt")
            sys.exit(1)
    
    print("\n" + "=" * 50)
    print("✅ Todas las verificaciones pasaron")
    print("🌐 Iniciando servidor web...")
    print("📍 URL: http://localhost:5000")
    print("🛑 Presiona Ctrl+C para detener el servidor")
    print("=" * 50)
    
    # Importar e iniciar la aplicación
    try:
        from app import app
        app.run(debug=True, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        print("\n\n👋 Servidor detenido por el usuario")
    except Exception as e:
        print(f"\n❌ Error iniciando la aplicación: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 