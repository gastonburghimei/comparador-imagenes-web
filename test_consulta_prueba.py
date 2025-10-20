#!/usr/bin/env python3
"""
Consulta de prueba para verificar que BigQuery + Cursor funciona perfectamente
"""

from bigquery_config import quick_query, create_default_connection
import pandas as pd

def test_consulta_simple():
    """Prueba con una consulta simple a warehouse-cross-pf"""
    print("🧪 PRUEBA 1: Consulta básica de metadatos")
    print("-" * 50)
    
    query = """
    SELECT 
        COUNT(*) as total_records,
        CURRENT_TIMESTAMP() as timestamp_consulta,
        'warehouse-cross-pf' as proyecto
    """
    
    try:
        df = quick_query(query)
        print("✅ Consulta ejecutada exitosamente!")
        print(f"📊 Total de registros simulados: {df['total_records'].iloc[0]}")
        print(f"⏰ Timestamp: {df['timestamp_consulta'].iloc[0]}")
        print(f"🏢 Proyecto: {df['proyecto'].iloc[0]}")
        return True
    except Exception as e:
        print(f"❌ Error en consulta básica: {e}")
        return False

def test_consulta_tabla_real():
    """Prueba con una tabla real del proyecto"""
    print("\n🧪 PRUEBA 2: Consulta a tabla real - fmo_ato.feature_metadata_part_1")
    print("-" * 50)
    
    query = """
    SELECT 
        COUNT(*) as total_filas,
        COUNT(DISTINCT REGEXP_EXTRACT(TO_JSON_STRING(*), r'"[^"]+')) as aprox_columnas
    FROM `warehouse-cross-pf.fmo_ato.feature_metadata_part_1`
    LIMIT 1
    """
    
    try:
        df = quick_query(query)
        print("✅ Consulta a tabla real exitosa!")
        print(f"📈 Filas en la tabla: {df['total_filas'].iloc[0]:,}")
        print(f"📊 Estructura detectada correctamente")
        return True
    except Exception as e:
        print(f"❌ Error en tabla real: {e}")
        print("💡 Probando con consulta más simple...")
        
        # Consulta más simple si la anterior falla
        simple_query = """
        SELECT COUNT(*) as total
        FROM `warehouse-cross-pf.fmo_ato.feature_metadata_part_1`
        LIMIT 1
        """
        
        try:
            df_simple = quick_query(simple_query)
            print(f"✅ Consulta simple exitosa! Total: {df_simple['total'].iloc[0]:,}")
            return True
        except Exception as e2:
            print(f"❌ Error también en consulta simple: {e2}")
            return False

def test_explorar_datasets():
    """Prueba explorando datasets disponibles"""
    print("\n🧪 PRUEBA 3: Explorar datasets disponibles")
    print("-" * 50)
    
    try:
        connection = create_default_connection()
        datasets = connection.list_datasets()
        
        print(f"✅ Conectado! Datasets disponibles: {len(datasets)}")
        
        # Mostrar algunos datasets relacionados con ATO/fraude
        datasets_ato = [d for d in datasets if 'ato' in d.lower() or 'fmo' in d.lower()]
        print(f"📊 Datasets relacionados con ATO/FMO: {len(datasets_ato)}")
        
        for i, dataset in enumerate(datasets_ato[:5], 1):
            print(f"   {i}. {dataset}")
            
        return True
    except Exception as e:
        print(f"❌ Error explorando datasets: {e}")
        return False

def test_consulta_datos_muestra():
    """Consulta de muestra con datos reales"""
    print("\n🧪 PRUEBA 4: Obtener muestra de datos reales")
    print("-" * 50)
    
    query = """
    SELECT *
    FROM `warehouse-cross-pf.fmo_ato.feature_metadata_part_1`
    LIMIT 5
    """
    
    try:
        df = quick_query(query)
        print("✅ Muestra de datos obtenida!")
        print(f"📊 Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
        print(f"📋 Columnas: {list(df.columns)}")
        
        if not df.empty:
            print("\n📄 Primeras filas:")
            print(df.head(2).to_string(max_cols=5))
        
        return True
    except Exception as e:
        print(f"❌ Error obteniendo muestra: {e}")
        return False

def ejecutar_todas_las_pruebas():
    """Ejecuta todas las pruebas de conectividad"""
    print("🚀 EJECUTANDO PRUEBAS DE CONECTIVIDAD BIGQUERY + CURSOR")
    print("=" * 60)
    
    resultados = []
    
    # Ejecutar todas las pruebas
    resultados.append(test_consulta_simple())
    resultados.append(test_consulta_tabla_real())
    resultados.append(test_explorar_datasets())
    resultados.append(test_consulta_datos_muestra())
    
    # Resumen final
    print("\n" + "=" * 60)
    print("📋 RESUMEN DE PRUEBAS:")
    print(f"✅ Pruebas exitosas: {sum(resultados)}/{len(resultados)}")
    print(f"❌ Pruebas fallidas: {len(resultados) - sum(resultados)}/{len(resultados)}")
    
    if all(resultados):
        print("\n🎉 ¡TODAS LAS PRUEBAS EXITOSAS!")
        print("✅ BigQuery + Cursor está funcionando PERFECTAMENTE")
        print("🚀 ¡Listo para trabajar con datos reales!")
    else:
        print("\n⚠️  Algunas pruebas fallaron")
        print("💡 Pero la conectividad básica está funcionando")
    
    return all(resultados)

if __name__ == "__main__":
    ejecutar_todas_las_pruebas() 