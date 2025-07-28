#!/usr/bin/env python3
"""
Script de configuración automatizada para BigQuery en Cursor
Basado en las mejores prácticas del repositorio fury_mcp-pf-bigquery-analizer
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from typing import Dict, Any


class BigQuerySetup:
    """Configurador automático para BigQuery"""
    
    def __init__(self):
        self.project_root = Path.cwd()
        self.env_file = self.project_root / "env.template"
        self.credentials_dir = self.project_root / "credentials"
        
    def check_prerequisites(self) -> Dict[str, bool]:
        """Verifica los prerequisitos del sistema"""
        print("🔍 Verificando prerequisitos...")
        
        results = {}
        
        # Verificar Python
        try:
            python_version = sys.version_info
            if python_version.major >= 3 and python_version.minor >= 8:
                results['python'] = True
                print(f"✅ Python {python_version.major}.{python_version.minor} está disponible")
            else:
                results['python'] = False
                print(f"❌ Python 3.8+ requerido, encontrado {python_version.major}.{python_version.minor}")
        except Exception as e:
            results['python'] = False
            print(f"❌ Error verificando Python: {e}")
        
        # Verificar pip
        try:
            # Intentar pip3 primero, luego pip
            try:
                subprocess.run(['pip3', '--version'], capture_output=True, check=True)
                results['pip'] = True
                print("✅ pip3 está disponible")
            except (subprocess.CalledProcessError, FileNotFoundError):
                subprocess.run(['pip', '--version'], capture_output=True, check=True)
                results['pip'] = True
                print("✅ pip está disponible")
        except (subprocess.CalledProcessError, FileNotFoundError):
            results['pip'] = False
            print("❌ pip no está disponible")
        
        # Verificar gcloud CLI (opcional)
        try:
            result = subprocess.run(['gcloud', '--version'], capture_output=True, check=True, text=True)
            results['gcloud'] = True
            print("✅ Google Cloud CLI está disponible")
        except (subprocess.CalledProcessError, FileNotFoundError):
            results['gcloud'] = False
            print("⚠️  Google Cloud CLI no está disponible (opcional)")
        
        return results
    
    def install_dependencies(self) -> bool:
        """Instala las dependencias de BigQuery"""
        print("\n📦 Instalando dependencias de BigQuery...")
        
        try:
            # Verificar si requirements.txt existe
            req_file = self.project_root / "requirements.txt"
            if not req_file.exists():
                print("❌ requirements.txt no encontrado")
                return False
            
            # Instalar dependencias usando el módulo pip de Python
            cmd = [sys.executable, '-m', 'pip', 'install', '-r', str(req_file)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ Dependencias instaladas exitosamente")
                return True
            else:
                print(f"❌ Error instalando dependencias: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Error durante la instalación: {e}")
            return False
    
    def setup_credentials_directory(self):
        """Crea el directorio para credenciales"""
        print("\n🔐 Configurando directorio de credenciales...")
        
        self.credentials_dir.mkdir(exist_ok=True)
        
        # Crear .gitignore para credentials
        gitignore_path = self.credentials_dir / ".gitignore"
        gitignore_content = """# Ignorar todos los archivos de credenciales
*.json
*.key
*.pem
service-account-*.json
"""
        
        with open(gitignore_path, 'w') as f:
            f.write(gitignore_content)
        
        print(f"✅ Directorio de credenciales creado: {self.credentials_dir}")
        print("📝 Archivo .gitignore creado para proteger credenciales")
    
    def create_environment_config(self):
        """Configura las variables de entorno"""
        print("\n🌍 Configurando variables de entorno...")
        
        # Verificar si ya existe configuración
        if self.env_file.exists():
            print(f"✅ Archivo de configuración encontrado: {self.env_file}")
        else:
            print(f"❌ Archivo env.template no encontrado en {self.env_file}")
            return
        
        # Crear archivo .env personalizado
        env_local = self.project_root / ".env"
        if not env_local.exists():
            # Copiar template
            with open(self.env_file, 'r') as template:
                content = template.read()
            
            with open(env_local, 'w') as env_file:
                env_file.write(content)
            
            print(f"✅ Archivo .env creado desde template")
            print("📝 Edita el archivo .env con tus credenciales específicas")
        else:
            print("⚠️  El archivo .env ya existe")
    
    def test_bigquery_connection(self) -> bool:
        """Prueba la conexión a BigQuery"""
        print("\n🧪 Probando conexión a BigQuery...")
        
        try:
            # Importar la configuración de BigQuery
            from bigquery_config import create_default_connection
            
            # Crear conexión
            connection = create_default_connection()
            
            # Probar conexión
            result = connection.test_connection()
            
            if result['status'] == 'success':
                print("✅ Conexión a BigQuery exitosa")
                print(f"📊 Proyecto: {result['project_id']}")
                print(f"⏰ Tiempo de respuesta: {result['test_time']}")
                return True
            else:
                print(f"❌ Error en la conexión: {result['message']}")
                return False
                
        except ImportError as e:
            print(f"❌ Error importando módulos: {e}")
            print("💡 Asegúrate de que las dependencias estén instaladas")
            return False
        except Exception as e:
            print(f"❌ Error probando conexión: {e}")
            return False
    
    def show_next_steps(self):
        """Muestra los próximos pasos"""
        print("\n🎯 Próximos pasos para usar BigQuery:")
        print("-" * 50)
        
        print("1. 🔑 Configurar credenciales:")
        print("   a) Crear service account en Google Cloud Console")
        print("   b) Descargar archivo JSON de credenciales")
        print(f"   c) Guardar en: {self.credentials_dir}/service-account-key.json")
        print("   d) Actualizar GOOGLE_APPLICATION_CREDENTIALS en .env")
        
        print("\n2. ⚙️ Configurar proyecto:")
        print("   a) Editar .env con tu GOOGLE_CLOUD_PROJECT")
        print("   b) Configurar BIGQUERY_LOCATION si es necesario")
        
        print("\n3. 🧪 Probar conexión:")
        print("   python bigquery_config.py")
        
        print("\n4. 📚 Ejecutar ejemplos:")
        print("   python bigquery_examples.py")
        
        print("\n5. 🚀 Integrar en tu código:")
        print("   from bigquery_config import BigQueryConnection, quick_query")
        
        print("\n📖 Recursos adicionales:")
        print("   - Documentación: https://cloud.google.com/bigquery/docs")
        print("   - Datos públicos: https://cloud.google.com/bigquery/public-data")
        print("   - Precios: https://cloud.google.com/bigquery/pricing")
    
    def create_vscode_settings(self):
        """Crea configuración para VS Code/Cursor"""
        print("\n⚙️ Creando configuración para Cursor/VS Code...")
        
        vscode_dir = self.project_root / ".vscode"
        vscode_dir.mkdir(exist_ok=True)
        
        # Configuración de Python para BigQuery
        settings = {
            "python.defaultInterpreterPath": "./venv/bin/python",
            "python.terminal.activateEnvironment": True,
            "python.linting.enabled": True,
            "python.linting.pylintEnabled": True,
            "files.associations": {
                "*.sql": "sql"
            },
            "sql.format.keywordCase": "upper",
            "workbench.editorAssociations": {
                "*.sql": "default"
            }
        }
        
        settings_file = vscode_dir / "settings.json"
        with open(settings_file, 'w') as f:
            json.dump(settings, f, indent=2)
        
        # Extensiones recomendadas
        extensions = {
            "recommendations": [
                "ms-python.python",
                "ms-python.pylint",
                "ms-toolsai.jupyter",
                "redhat.vscode-yaml",
                "bradlc.vscode-tailwindcss",
                "googlecloudtools.cloudcode",
                "mtxr.sqltools",
                "mtxr.sqltools-driver-pg"
            ]
        }
        
        extensions_file = vscode_dir / "extensions.json"
        with open(extensions_file, 'w') as f:
            json.dump(extensions, f, indent=2)
        
        print("✅ Configuración de Cursor/VS Code creada")
        print("💡 Instala las extensiones recomendadas para mejor experiencia")
    
    def run_setup(self):
        """Ejecuta la configuración completa"""
        print("🚀 Configuración de BigQuery para Cursor")
        print("=" * 50)
        
        # Verificar prerequisitos
        prereqs = self.check_prerequisites()
        if not all([prereqs.get('python', False), prereqs.get('pip', False)]):
            print("❌ Faltan prerequisitos básicos. Configuración cancelada.")
            return False
        
        # Instalar dependencias
        if not self.install_dependencies():
            print("❌ Error instalando dependencias. Configuración cancelada.")
            return False
        
        # Configurar credenciales
        self.setup_credentials_directory()
        
        # Configurar entorno
        self.create_environment_config()
        
        # Configurar VS Code/Cursor
        self.create_vscode_settings()
        
        # Probar conexión (puede fallar si no hay credenciales)
        print("\n🧪 Intentando probar conexión...")
        connection_ok = self.test_bigquery_connection()
        
        if not connection_ok:
            print("⚠️  No se pudo probar la conexión (normal si no hay credenciales configuradas)")
        
        # Mostrar próximos pasos
        self.show_next_steps()
        
        print("\n✅ Configuración completada!")
        return True


def main():
    """Función principal"""
    setup = BigQuerySetup()
    setup.run_setup()


if __name__ == "__main__":
    main() 