#!/usr/bin/env python3
"""
Prueba rápida de BigQuery con datos públicos (SIN credenciales)
Esto verifica que todo funciona antes de configurar credenciales
"""

import pandas as pd
from google.cloud import bigquery

def test_public_bigquery():
    """Prueba BigQuery con datos públicos que no requieren autenticación"""
    print("🧪 Probando BigQuery con datos públicos...")
    
    try:
        # Crear cliente sin credenciales específicas para datos públicos
        client = bigquery.Client()
        
        # Consulta a datos públicos (no requiere credenciales especiales)
        query = """
        SELECT
            name,
            gender,
            SUM(number) as total_births
        FROM `bigquery-public-data.usa_names.usa_1910_current`
        WHERE 
            year = 2020
            AND gender = 'F'
        GROUP BY name, gender
        ORDER BY total_births DESC
        LIMIT 10
        """
        
        print("📊 Ejecutando consulta a datos públicos...")
        
        # Ejecutar consulta
        result = client.query(query)
        df = result.to_dataframe()
        
        if not df.empty:
            print("✅ ¡ÉXITO! BigQuery funciona correctamente")
            print("\n📈 Top 10 nombres femeninos en USA (2020):")
            print("-" * 50)
            for idx, row in df.iterrows():
                print(f"{idx+1:2d}. {row['name']:15s} - {row['total_births']:,} bebés")
            
            print(f"\n🎉 Total registros: {len(df)}")
            print("✅ La integración BigQuery + Cursor está FUNCIONANDO")
            return True
        else:
            print("⚠️  Consulta ejecutada pero sin resultados")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Posibles soluciones:")
        print("1. Verificar conexión a internet")
        print("2. Instalar dependencias: pip3 install -r requirements.txt")
        print("3. Configurar credenciales de Google Cloud")
        return False

if __name__ == "__main__":
    test_public_bigquery() 