#!/usr/bin/env python3
"""
Prueba de conexión específica para warehouse-cross-pf
"""

import os
from bigquery_config import BigQueryConfig, BigQueryConnection

def test_warehouse_connection():
    """Prueba la conexión al proyecto warehouse-cross-pf"""
    print("🔗 Probando conexión a warehouse-cross-pf...")
    
    # Configurar el proyecto específico
    config = BigQueryConfig(
        project_id="warehouse-cross-pf",
        location="US"
    )
    
    try:
        connection = BigQueryConnection(config)
        result = connection.test_connection()
        
        if result['status'] == 'success':
            print("✅ ¡CONEXIÓN EXITOSA!")
            print(f"📊 Proyecto: {result['project_id']}")
            print(f"⏰ Timestamp: {result['test_time']}")
            
            # Listar datasets disponibles
            print("\n📁 Explorando datasets disponibles...")
            datasets = connection.list_datasets()
            print(f"📂 Datasets encontrados: {len(datasets)}")
            
            for i, dataset in enumerate(datasets[:5], 1):
                print(f"   {i}. {dataset}")
                # Listar algunas tablas del dataset
                tables = connection.list_tables(dataset)
                print(f"      └── Tablas: {len(tables)}")
                if tables:
                    for table in tables[:3]:
                        print(f"          • {table}")
            
            print("\n🎉 ¡INTEGRACIÓN COMPLETADA!")
            print("BigQuery + Cursor está funcionando perfectamente")
            return True
            
        else:
            print(f"❌ Error de conexión: {result['message']}")
            print("\n🔑 NECESITAS CONFIGURAR CREDENCIALES:")
            print("Ejecuta uno de estos comandos:")
            print("1. gcloud auth application-default login")
            print("2. O crea un Service Account en Google Cloud Console")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    test_warehouse_connection() 