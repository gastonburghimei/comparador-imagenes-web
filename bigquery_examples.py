"""
Ejemplos prácticos para usar BigQuery desde Cursor
Incluye casos de uso comunes y scripts de demostración
"""

import pandas as pd
from bigquery_config import BigQueryConnection, BigQueryConfig, quick_query
import os
from datetime import datetime, timedelta


def example_1_basic_connection():
    """Ejemplo 1: Conexión básica y prueba"""
    print("🔗 Ejemplo 1: Probando conexión básica a BigQuery")
    print("-" * 50)
    
    # Crear configuración
    config = BigQueryConfig(
        project_id="tu-proyecto-bigquery",  # Cambiar por tu proyecto
        location="US"
    )
    
    # Crear conexión
    connection = BigQueryConnection(config)
    
    # Probar conexión
    result = connection.test_connection()
    print(f"Estado: {result['status']}")
    print(f"Mensaje: {result['message']}")
    if result['status'] == 'success':
        print(f"Proyecto: {result['project_id']}")
        print(f"Tiempo de prueba: {result['test_time']}")
    
    return connection


def example_2_explore_datasets(connection):
    """Ejemplo 2: Explorar datasets y tablas"""
    print("\n📊 Ejemplo 2: Explorando datasets y tablas")
    print("-" * 50)
    
    # Listar datasets
    datasets = connection.list_datasets()
    print(f"Datasets encontrados: {len(datasets)}")
    
    for dataset in datasets[:3]:  # Mostrar solo los primeros 3
        print(f"\n📁 Dataset: {dataset}")
        tables = connection.list_tables(dataset)
        print(f"   Tablas: {len(tables)}")
        for table in tables[:5]:  # Mostrar solo las primeras 5 tablas
            print(f"   - {table}")


def example_3_public_data_queries():
    """Ejemplo 3: Consultas con datos públicos de BigQuery"""
    print("\n🌍 Ejemplo 3: Consultando datos públicos")
    print("-" * 50)
    
    # Ejemplo con datos públicos de GitHub
    query_github = """
    SELECT
        repository_name,
        COUNT(*) as commits_count
    FROM `bigquery-public-data.github_repos.commits`
    WHERE 
        committer.date >= '2024-01-01'
        AND repository_name LIKE '%python%'
    GROUP BY repository_name
    ORDER BY commits_count DESC
    LIMIT 10
    """
    
    try:
        print("Ejecutando consulta de datos públicos de GitHub...")
        df_github = quick_query(query_github)
        print("\n🐙 Top 10 repositorios Python con más commits en 2024:")
        print(df_github.to_string(index=False))
    except Exception as e:
        print(f"❌ Error en consulta GitHub: {e}")
    
    # Ejemplo con datos de nombres de bebés
    query_names = """
    SELECT
        name,
        SUM(number) as total_births
    FROM `bigquery-public-data.usa_names.usa_1910_current`
    WHERE 
        year >= 2020
        AND gender = 'F'
    GROUP BY name
    ORDER BY total_births DESC
    LIMIT 10
    """
    
    try:
        print("\n👶 Ejecutando consulta de nombres de bebés...")
        df_names = quick_query(query_names)
        print("\nTop 10 nombres femeninos más populares desde 2020:")
        print(df_names.to_string(index=False))
    except Exception as e:
        print(f"❌ Error en consulta nombres: {e}")


def example_4_data_analysis():
    """Ejemplo 4: Análisis de datos y visualización básica"""
    print("\n📈 Ejemplo 4: Análisis de datos")
    print("-" * 50)
    
    # Consulta de análisis temporal
    query_weather = """
    SELECT
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        AVG(temperature) as avg_temperature,
        COUNT(*) as records_count
    FROM `bigquery-public-data.noaa_gsod.gsod2023`
    WHERE
        station_number = '725030'  -- Estación JFK Airport
        AND temperature IS NOT NULL
        AND temperature != 9999.9  -- Filtrar valores faltantes
    GROUP BY year, month
    ORDER BY year, month
    """
    
    try:
        print("Analizando datos meteorológicos de JFK Airport 2023...")
        df_weather = quick_query(query_weather)
        
        if not df_weather.empty:
            print(f"\nRegistros encontrados: {len(df_weather)}")
            print("\nEstadísticas de temperatura:")
            print(f"Temperatura promedio anual: {df_weather['avg_temperature'].mean():.2f}°F")
            print(f"Temperatura máxima mensual: {df_weather['avg_temperature'].max():.2f}°F")
            print(f"Temperatura mínima mensual: {df_weather['avg_temperature'].min():.2f}°F")
            
            print("\nDatos por mes:")
            print(df_weather.to_string(index=False))
    except Exception as e:
        print(f"❌ Error en análisis meteorológico: {e}")


def example_5_create_sample_data(connection):
    """Ejemplo 5: Crear y subir datos de muestra"""
    print("\n🆕 Ejemplo 5: Creando datos de muestra")
    print("-" * 50)
    
    # Crear DataFrame de muestra
    sample_data = pd.DataFrame({
        'fecha': pd.date_range('2024-01-01', periods=100, freq='D'),
        'ventas': pd.np.random.randint(100, 1000, 100),
        'producto': pd.np.random.choice(['A', 'B', 'C'], 100),
        'region': pd.np.random.choice(['Norte', 'Sur', 'Este', 'Oeste'], 100)
    })
    
    print("Datos de muestra creados:")
    print(sample_data.head().to_string(index=False))
    print(f"\nTotal de registros: {len(sample_data)}")
    
    # Intentar subir a BigQuery (requiere permisos de escritura)
    try:
        dataset_id = "test_dataset"  # Cambiar por tu dataset
        table_id = "ventas_muestra"
        
        success = connection.upload_dataframe(
            sample_data, 
            dataset_id, 
            table_id, 
            if_exists="replace"
        )
        
        if success:
            print(f"✅ Datos subidos exitosamente a {dataset_id}.{table_id}")
        else:
            print("❌ Error al subir datos")
    except Exception as e:
        print(f"⚠️  No se pudieron subir los datos (normal si no tienes permisos de escritura): {e}")


def example_6_sql_from_files():
    """Ejemplo 6: Ejecutar consultas SQL desde archivos"""
    print("\n📄 Ejemplo 6: Ejecutando SQL desde archivos")
    print("-" * 50)
    
    # Buscar archivos SQL en el directorio actual
    sql_files = [f for f in os.listdir('.') if f.endswith('.sql')]
    print(f"Archivos SQL encontrados: {len(sql_files)}")
    
    for sql_file in sql_files[:3]:  # Mostrar solo los primeros 3
        print(f"\n📄 Archivo: {sql_file}")
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\n')
                print(f"   Líneas: {len(lines)}")
                print(f"   Tamaño: {len(content)} caracteres")
                
                # Mostrar las primeras líneas (sin ejecutar)
                print("   Primeras líneas:")
                for i, line in enumerate(lines[:3]):
                    if line.strip():
                        print(f"   {i+1}: {line[:60]}{'...' if len(line) > 60 else ''}")
        except Exception as e:
            print(f"   ❌ Error leyendo archivo: {e}")


def run_all_examples():
    """Ejecuta todos los ejemplos"""
    print("🚀 Ejecutando todos los ejemplos de BigQuery")
    print("=" * 60)
    
    # Ejemplo 1: Conexión básica
    try:
        connection = example_1_basic_connection()
    except Exception as e:
        print(f"❌ Error en ejemplo 1: {e}")
        print("⚠️  Asegúrate de configurar las credenciales correctamente")
        return
    
    # Ejemplo 2: Explorar datasets (solo si la conexión es exitosa)
    try:
        example_2_explore_datasets(connection)
    except Exception as e:
        print(f"❌ Error en ejemplo 2: {e}")
    
    # Ejemplo 3: Datos públicos (funciona sin configuración específica)
    example_3_public_data_queries()
    
    # Ejemplo 4: Análisis de datos
    example_4_data_analysis()
    
    # Ejemplo 5: Crear datos de muestra
    try:
        example_5_create_sample_data(connection)
    except Exception as e:
        print(f"❌ Error en ejemplo 5: {e}")
    
    # Ejemplo 6: SQL desde archivos
    example_6_sql_from_files()
    
    print("\n🎉 Ejemplos completados")
    print("💡 Para usar BigQuery en tus proyectos:")
    print("   1. Configura las credenciales en env.template")
    print("   2. Instala las dependencias: pip install -r requirements.txt")
    print("   3. Usa las clases en bigquery_config.py")


if __name__ == "__main__":
    run_all_examples() 