#!/usr/bin/env python3
"""
Test de integración para el MCP BigQuery
Verifica que todo esté funcionando correctamente
"""

import asyncio
import json
import subprocess
import sys
import os
from pathlib import Path

async def test_mcp_server():
    """Prueba el servidor MCP directamente"""
    print("🧪 PRUEBA 1: Servidor MCP BigQuery")
    print("-" * 50)
    
    try:
        # Importar el servidor MCP
        from mcp_bigquery_setup import MCPBigQueryServer, MCPConfig
        
        # Crear instancia del servidor
        config = MCPConfig()
        server = MCPBigQueryServer(config)
        server.setup_operations("meli-bi-data", "US")
        
        # Probar inicialización
        result = await server.initialize()
        if result["status"] == "success":
            print("✅ Servidor MCP inicializado correctamente")
            
            # Probar capacidades
            capabilities = server.get_capabilities()
            print(f"🔧 Herramientas disponibles: {len(capabilities['tools'])}")
            for tool in capabilities['tools']:
                print(f"   - {tool['name']}")
            
            return True
        else:
            print(f"❌ Error en inicialización: {result['message']}")
            return False
            
    except Exception as e:
        print(f"❌ Error en servidor MCP: {e}")
        return False

def test_cursor_config():
    """Verifica la configuración de Cursor"""
    print("\n🧪 PRUEBA 2: Configuración de Cursor")
    print("-" * 50)
    
    config_file = Path.home() / ".cursor" / "mcp_settings.json"
    
    if config_file.exists():
        print(f"✅ Archivo de configuración encontrado: {config_file}")
        
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            if "mcpServers" in config:
                servers = config["mcpServers"]
                print(f"🔧 Servidores MCP configurados: {len(servers)}")
                
                for server_name, server_config in servers.items():
                    print(f"   - {server_name}")
                    print(f"     Comando: {server_config.get('command', 'N/A')}")
                    print(f"     Proyecto: {server_config.get('env', {}).get('GOOGLE_CLOUD_PROJECT', 'N/A')}")
                
                return True
            else:
                print("❌ No se encontraron servidores MCP en la configuración")
                return False
                
        except Exception as e:
            print(f"❌ Error leyendo configuración: {e}")
            return False
    else:
        print(f"❌ Archivo de configuración no encontrado: {config_file}")
        return False

def test_bigquery_credentials():
    """Verifica las credenciales de BigQuery"""
    print("\n🧪 PRUEBA 3: Credenciales BigQuery")
    print("-" * 50)
    
    try:
        from bigquery_config import create_default_connection
        
        # Probar conexión básica
        connection = create_default_connection()
        result = connection.test_connection()
        
        if result["status"] == "success":
            print("✅ Credenciales BigQuery funcionando")
            print(f"📊 Proyecto: {result['project_id']}")
            return True
        else:
            print(f"❌ Error en credenciales: {result['message']}")
            return False
            
    except Exception as e:
        print(f"❌ Error verificando credenciales: {e}")
        return False

async def test_mcp_with_bigquery():
    """Prueba el MCP con BigQuery real"""
    print("\n🧪 PRUEBA 4: MCP + BigQuery (Consulta Real)")
    print("-" * 50)
    
    try:
        # Probar consulta específica del usuario
        from mcp_bigquery_setup import MCPBigQueryBasicOperations
        
        operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
        
        # Inicializar
        init_result = await operations.initialize()
        if init_result["status"] != "success":
            print(f"❌ Error inicializando operaciones: {init_result['message']}")
            return False
        
        # Probar consulta simple con una tabla real
        query = """
        SELECT 
            'meli-bi-data' as project_name,
            CURRENT_TIMESTAMP() as query_time,
            1 as test_value
        """
        
        result = await operations.execute_query(query, 5)
        
        if result["status"] == "success":
            print("✅ Consulta MCP ejecutada exitosamente")
            rows = result["result"]["rows"]
            if rows:
                print(f"📊 Resultado: {rows[0]}")
            return True
        else:
            print(f"❌ Error en consulta MCP: {result.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ Error en prueba MCP+BigQuery: {e}")
        return False

async def run_all_tests():
    """Ejecuta todas las pruebas"""
    print("🚀 EJECUTANDO PRUEBAS DE INTEGRACIÓN MCP BIGQUERY")
    print("=" * 60)
    print("📋 Basado en fury_mcp-pf-bigquery-analizer de MercadoLibre")
    print("=" * 60)
    
    results = []
    
    # Ejecutar todas las pruebas
    results.append(await test_mcp_server())
    results.append(test_cursor_config())
    results.append(test_bigquery_credentials())
    results.append(await test_mcp_with_bigquery())
    
    # Resumen final
    print("\n" + "=" * 60)
    print("📋 RESUMEN DE PRUEBAS:")
    print(f"✅ Pruebas exitosas: {sum(results)}/{len(results)}")
    print(f"❌ Pruebas fallidas: {len(results) - sum(results)}/{len(results)}")
    
    if all(results):
        print("\n🎉 ¡TODAS LAS PRUEBAS EXITOSAS!")
        print("✅ MCP BigQuery está funcionando PERFECTAMENTE")
        print("🚀 ¡Cursor está listo para usar el MCP!")
        
        print("\n📖 CÓMO USAR:")
        print("1. Reinicia Cursor")
        print("2. El MCP se conectará automáticamente")
        print("3. Usa comandos como '@bigquery-analyzer list datasets'")
        print("4. Ejecuta consultas SQL directamente desde Cursor")
        
    else:
        print("\n⚠️  Algunas pruebas fallaron")
        print("💡 Revisa los errores arriba para solucionarlos")
    
    return all(results)

if __name__ == "__main__":
    try:
        success = asyncio.run(run_all_tests())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n🛑 Pruebas interrumpidas por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        sys.exit(1) 