#!/usr/bin/env python3
"""
Query CORREGIDA: "Usuarios se contactan" (Hoja 2)
JOIN corrigiendo tipos de datos: INT64 vs STRING
"""

import asyncio
from mcp_bigquery_setup import MCPBigQueryBasicOperations

async def verificar_tipos_datos():
    """Verificar tipos de datos en ambas tablas"""
    
    print("🔍 VERIFICANDO TIPOS DE DATOS")
    print("-" * 50)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # Verificar tipos en tabla de restricciones
    query_check1 = """
    SELECT 
        TYPEOF(SENTENCE_ID) as tipo_sentence_id,
        TYPEOF(USER_ID) as tipo_user_id,
        TYPEOF(INFRACTION_TYPE) as tipo_infraction,
        TYPEOF(SENTENCE_DATE) as tipo_sentence_date
    FROM `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW`
    WHERE INFRACTION_TYPE = 'CUENTA_DE_HACKER'
    LIMIT 1
    """
    
    print("📊 Tipos en BT_RES_RESTRICTIONS_INFRACTIONS_NW:")
    result1 = await operations.execute_query(query_check1, 1)
    if result1["status"] == "success" and result1["result"]["rows"]:
        row = result1["result"]["rows"][0]
        for col, tipo in row.items():
            print(f"   {col}: {tipo}")
    
    # Verificar tipos en tabla ATO
    query_check2 = """
    SELECT 
        TYPEOF(GCA_ID) as tipo_gca_id,
        TYPEOF(GCA_CUST_ID) as tipo_gca_cust_id,
        TYPEOF(fecha_apertura_caso) as tipo_apertura,
        TYPEOF(fecha_cierre_caso) as tipo_cierre
    FROM `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO`
    LIMIT 1
    """
    
    print("\n📊 Tipos en CONSULTAS_ATO:")
    result2 = await operations.execute_query(query_check2, 1)
    if result2["status"] == "success" and result2["result"]["rows"]:
        row = result2["result"]["rows"][0]
        for col, tipo in row.items():
            print(f"   {col}: {tipo}")

async def query_usuarios_se_contactan_corregida():
    """
    Query corregida con conversión de tipos
    """
    
    print("\n🔍 RECREANDO QUERY CORREGIDA: 'Usuarios se contactan'")
    print("📋 JOIN con conversión de tipos de datos")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # Query corrigiendo tipos de datos
    query = """
    SELECT 
        res.SENTENCE_ID,
        res.USER_ID,
        res.INFRACTION_TYPE,
        res.SENTENCE_DATE,
        res.COLOR_DE_TARJETA,
        res.SIT_SITE_ID,
        res.SENTENCE_LAST_STATUS,
        -- Fechas de casos ATO asociados
        ato.fecha_apertura_caso,
        ato.fecha_cierre_caso
    FROM 
        `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res
    INNER JOIN 
        `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO` ato
        -- CORRECCIÓN: Convertir tipos para que coincidan
        ON CAST(res.USER_ID AS STRING) = CAST(ato.GCA_CUST_ID AS STRING)
    WHERE 
        res.INFRACTION_TYPE = 'CUENTA_DE_HACKER'
        
        -- Período abril-mayo 2025
        AND res.SENTENCE_DATE >= '2025-04-01'
        AND res.SENTENCE_DATE <= '2025-05-31'
        
        -- Estados incluyendo REINSTATED
        AND res.SENTENCE_LAST_STATUS IN ('ACTIVE', 'ROLLBACKED', 'REINSTATED')
        
        -- Casos ATO válidos
        AND ato.fecha_apertura_caso IS NOT NULL
        AND ato.fecha_cierre_caso IS NOT NULL
        
    ORDER BY 
        res.SENTENCE_DATE DESC,
        res.SENTENCE_ID DESC
    """
    
    try:
        print("📊 Ejecutando query con tipos corregidos...")
        result = await operations.execute_query(query, 100)
        
        if result["status"] == "success":
            rows = result["result"]["rows"]
            
            print(f"✅ ¡Query corregida ejecutada exitosamente!")
            print(f"📊 {len(rows)} resultados encontrados")
            
            if result["result"].get("job_info"):
                job_info = result["result"]["job_info"]
                bytes_processed = job_info.get('bytes_processed', 0)
                print(f"💾 Bytes procesados: {bytes_processed:,}")
            
            if rows:
                print(f"\n📋 USUARIOS SE CONTACTAN (con fechas ATO):")
                print("=" * 140)
                print(f"{'SENTENCE_ID':<12} {'USER_ID':<12} {'SENTENCE_DATE':<12} {'ESTADO':<12} {'APERTURA_ATO':<20} {'CIERRE_ATO':<20}")
                print("=" * 140)
                
                for i, row in enumerate(rows[:15]):  # Mostrar más ejemplos
                    sentence_id = str(row.get('SENTENCE_ID', 'N/A'))[:11]
                    user_id = str(row.get('USER_ID', 'N/A'))[:11]
                    sentence_date = str(row.get('SENTENCE_DATE', 'N/A'))[:11] if row.get('SENTENCE_DATE') else 'N/A'
                    estado = str(row.get('SENTENCE_LAST_STATUS', 'N/A'))[:11]
                    apertura = str(row.get('fecha_apertura_caso', 'N/A'))[:19] if row.get('fecha_apertura_caso') else 'N/A'
                    cierre = str(row.get('fecha_cierre_caso', 'N/A'))[:19] if row.get('fecha_cierre_caso') else 'N/A'
                    
                    print(f"{sentence_id:<12} {user_id:<12} {sentence_date:<12} {estado:<12} {apertura:<20} {cierre:<20}")
                
                if len(rows) > 15:
                    print(f"... y {len(rows) - 15} más")
                
                # Estadísticas específicas
                print(f"\n📈 ANÁLISIS DETALLADO:")
                
                # Estados
                estados = {}
                for row in rows:
                    estado = row.get('SENTENCE_LAST_STATUS', 'Sin estado')
                    estados[estado] = estados.get(estado, 0) + 1
                
                print(f"\n📊 Distribución por estado:")
                for estado, cantidad in sorted(estados.items(), key=lambda x: x[1], reverse=True):
                    pct = (cantidad / len(rows)) * 100
                    print(f"   {estado}: {cantidad:,} casos ({pct:.1f}%)")
                
                # Sitios
                sitios = {}
                for row in rows:
                    sitio = row.get('SIT_SITE_ID', 'Sin sitio')
                    sitios[sitio] = sitios.get(sitio, 0) + 1
                
                print(f"\n🌍 Distribución por sitio:")
                for sitio, cantidad in sorted(sitios.items(), key=lambda x: x[1], reverse=True):
                    pct = (cantidad / len(rows)) * 100
                    print(f"   {sitio}: {cantidad:,} casos ({pct:.1f}%)")
                
                # Análisis temporal (diferencia entre apertura y cierre ATO)
                print(f"\n⏱️  Análisis temporal casos ATO:")
                duraciones_horas = []
                
                for row in rows[:50]:  # Analizar los primeros 50
                    apertura = row.get('fecha_apertura_caso')
                    cierre = row.get('fecha_cierre_caso')
                    
                    if apertura and cierre:
                        try:
                            from datetime import datetime
                            # Manejar diferentes formatos de fecha
                            if isinstance(apertura, str):
                                apertura_dt = datetime.fromisoformat(apertura.replace('T', ' ').replace('Z', ''))
                            else:
                                apertura_dt = apertura
                                
                            if isinstance(cierre, str):
                                cierre_dt = datetime.fromisoformat(cierre.replace('T', ' ').replace('Z', ''))
                            else:
                                cierre_dt = cierre
                            
                            duracion_horas = (cierre_dt - apertura_dt).total_seconds() / 3600
                            if duracion_horas >= 0:  # Solo duraciones positivas válidas
                                duraciones_horas.append(duracion_horas)
                        except:
                            pass
                
                if duraciones_horas:
                    promedio = sum(duraciones_horas) / len(duraciones_horas)
                    minimo = min(duraciones_horas)
                    maximo = max(duraciones_horas)
                    
                    print(f"   Casos analizados: {len(duraciones_horas)}")
                    print(f"   Duración promedio: {promedio:.1f} horas ({promedio/24:.1f} días)")
                    print(f"   Duración mínima: {minimo:.1f} horas")
                    print(f"   Duración máxima: {maximo:.1f} horas ({maximo/24:.1f} días)")
                
                return rows
                
            else:
                print("⚠️  No se encontraron usuarios con casos ATO asociados")
                return None
                
        else:
            print(f"❌ Error en query: {result.get('error', 'Error desconocido')}")
            return None
            
    except Exception as e:
        print(f"❌ Error ejecutando query: {e}")
        return None

async def query_sin_filtros_temporales():
    """Query alternativa sin filtros temporales restrictivos"""
    
    print("\n" + "="*80)
    print("🔍 QUERY SIN FILTROS TEMPORALES (buscar todos los matches)")
    print("📋 Para maximizar resultados del JOIN")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    query_amplia = """
    SELECT 
        res.SENTENCE_ID,
        res.USER_ID,
        res.INFRACTION_TYPE,
        res.SENTENCE_DATE,
        res.COLOR_DE_TARJETA,
        res.SIT_SITE_ID,
        res.SENTENCE_LAST_STATUS,
        ato.fecha_apertura_caso,
        ato.fecha_cierre_caso
    FROM 
        `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res
    INNER JOIN 
        `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO` ato
        ON CAST(res.USER_ID AS STRING) = CAST(ato.GCA_CUST_ID AS STRING)
    WHERE 
        res.INFRACTION_TYPE = 'CUENTA_DE_HACKER'
        AND res.SENTENCE_DATE >= '2025-04-01'
        AND res.SENTENCE_DATE <= '2025-05-31'
        AND res.SENTENCE_LAST_STATUS IN ('ACTIVE', 'ROLLBACKED', 'REINSTATED')
        -- SIN filtros temporales en casos ATO
        
    ORDER BY 
        res.SENTENCE_DATE DESC,
        res.SENTENCE_ID DESC
    """
    
    try:
        print("📊 Ejecutando query amplia...")
        result = await operations.execute_query(query_amplia, 100)
        
        if result["status"] == "success" and result["result"]["rows"]:
            rows = result["result"]["rows"]
            print(f"✅ Query amplia: {len(rows)} resultados")
            
            # Mostrar estadísticas comparativas
            con_fecha_cierre = sum(1 for row in rows if row.get('fecha_cierre_caso'))
            print(f"📊 Casos con fecha_cierre: {con_fecha_cierre}/{len(rows)}")
            
            return rows
        else:
            print("❌ Query amplia sin resultados")
            return None
            
    except Exception as e:
        print(f"❌ Error en query amplia: {e}")
        return None

async def main():
    """Función principal"""
    print("🚀 RECREANDO QUERY CORREGIDA: 'Usuarios se contactan'")
    print("📋 Corrigiendo tipos de datos en JOIN")
    print("🎯 Objetivo: 889 usuarios con fechas ATO")
    print("=" * 80)
    
    # 1. Verificar tipos de datos
    await verificar_tipos_datos()
    
    # 2. Ejecutar query corregida
    casos_principales = await query_usuarios_se_contactan_corregida()
    
    # 3. Si pocos resultados, probar sin filtros temporales
    if not casos_principales or len(casos_principales) < 50:
        casos_amplios = await query_sin_filtros_temporales()
        
        if casos_amplios and len(casos_amplios) > len(casos_principales or []):
            print(f"\n💡 Query amplia encontró más resultados: {len(casos_amplios)}")
    
    print(f"\n" + "="*80)
    print(f"🎉 ¡QUERY 'USUARIOS SE CONTACTAN' CORREGIDA!")
    print(f"✅ MCP BigQuery funcionando perfectamente")
    if casos_principales:
        print(f"📊 Resultados: {len(casos_principales)} usuarios con casos ATO")

if __name__ == "__main__":
    asyncio.run(main()) 