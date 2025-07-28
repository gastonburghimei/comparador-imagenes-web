#!/usr/bin/env python3
"""
Query recreada: "Se contactan CUENTA HACKER"
Usando las tablas:
- meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW (alias res)
- meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO

Usando MCP BigQuery basado en fury_mcp-pf-bigquery-analizer
"""

import asyncio
import pandas as pd
from mcp_bigquery_setup import MCPBigQueryBasicOperations

async def query_cuenta_hacker():
    """
    Recrear la query que genera los datos del archivo "Se contactan CUENTA HACKER"
    """
    
    print("🔍 RECREANDO QUERY: 'Se contactan CUENTA HACKER'")
    print("📋 Usando tablas: BT_RES_RESTRICTIONS_INFRACTIONS_NW + CONSULTAS_ATO")
    print("🎯 Casos de CUENTA_DE_HACKER")
    print("-" * 80)
    
    # Inicializar MCP BigQuery
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    print("✅ MCP BigQuery inicializado")
    
    # Query recreada basada en la estructura del archivo
    query = """
    SELECT 
        res.SENTENCE_ID,
        res.USER_ID,
        res.INFRACTION_TYPE,
        res.SENTENCE_DATE,
        res.COLOR_DE_TARJETA,
        res.SIT_SITE_ID,
        res.SENTENCE_LAST_STATUS
    FROM 
        `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res
    WHERE 
        res.INFRACTION_TYPE = 'CUENTA_DE_HACKER'
        
        -- Filtro de fechas (ajusta según necesites)
        AND res.SENTENCE_DATE >= '2025-04-01'
        AND res.SENTENCE_DATE <= '2025-05-31'
        
        -- Solo casos activos y rollbacked (como en el archivo)
        AND res.SENTENCE_LAST_STATUS IN ('ACTIVE', 'ROLLBACKED')
        
    ORDER BY 
        res.SENTENCE_DATE DESC,
        res.SENTENCE_ID DESC
    """
    
    try:
        print("📊 Ejecutando query recreada...")
        result = await operations.execute_query(query, 100)  # Primeros 100 para verificar
        
        if result["status"] == "success":
            query_result = result["result"]
            rows = query_result["rows"]
            
            print(f"✅ ¡Query ejecutada exitosamente!")
            print(f"📊 Primeros {len(rows)} resultados de {len(rows)} total")
            
            if query_result.get("job_info"):
                job_info = query_result["job_info"]
                bytes_processed = job_info.get('bytes_processed', 0)
                bytes_billed = job_info.get('bytes_billed', 0)
                print(f"💾 Bytes procesados: {bytes_processed:,}")
                print(f"💰 Bytes facturados: {bytes_billed:,}")
            
            if rows:
                print(f"\n📋 RESULTADOS - Casos CUENTA_DE_HACKER:")
                print("=" * 120)
                print(f"{'SENTENCE_ID':<12} {'USER_ID':<12} {'FECHA':<12} {'SITE':<5} {'ESTADO':<12} {'COLOR':<8}")
                print("=" * 120)
                
                for i, row in enumerate(rows[:10]):  # Mostrar primeros 10
                    sentence_id = str(row.get('SENTENCE_ID', 'N/A'))[:11]
                    user_id = str(row.get('USER_ID', 'N/A'))[:11]
                    fecha = str(row.get('SENTENCE_DATE', 'N/A'))[:11] if row.get('SENTENCE_DATE') else 'N/A'
                    site = str(row.get('SIT_SITE_ID', 'N/A'))[:4]
                    estado = str(row.get('SENTENCE_LAST_STATUS', 'N/A'))[:11]
                    color = str(row.get('COLOR_DE_TARJETA', 'N/A'))[:7]
                    
                    print(f"{sentence_id:<12} {user_id:<12} {fecha:<12} {site:<5} {estado:<12} {color:<8}")
                
                if len(rows) > 10:
                    print(f"... y {len(rows) - 10} más")
                
                # Estadísticas
                print(f"\n📈 ANÁLISIS DE RESULTADOS:")
                
                # Por estado
                estados = {}
                for row in rows:
                    estado = row.get('SENTENCE_LAST_STATUS', 'Sin estado')
                    estados[estado] = estados.get(estado, 0) + 1
                
                print(f"\n📊 Casos por estado:")
                for estado, cantidad in sorted(estados.items(), key=lambda x: x[1], reverse=True):
                    print(f"   {estado}: {cantidad:,} casos")
                
                # Por sitio
                sitios = {}
                for row in rows:
                    sitio = row.get('SIT_SITE_ID', 'Sin sitio')
                    sitios[sitio] = sitios.get(sitio, 0) + 1
                
                print(f"\n🌍 Casos por sitio:")
                for sitio, cantidad in sorted(sitios.items(), key=lambda x: x[1], reverse=True):
                    print(f"   {sitio}: {cantidad:,} casos")
                
                # Por fecha (top 5 días)
                fechas = {}
                for row in rows:
                    fecha = row.get('SENTENCE_DATE', '')
                    if fecha:
                        dia = str(fecha)[:10]  # YYYY-MM-DD
                        fechas[dia] = fechas.get(dia, 0) + 1
                
                print(f"\n📅 Top 5 días con más casos:")
                for fecha, cantidad in sorted(fechas.items(), key=lambda x: x[1], reverse=True)[:5]:
                    print(f"   {fecha}: {cantidad:,} casos")
                
                return rows
                
            else:
                print("⚠️  No se encontraron casos CUENTA_DE_HACKER en el período")
                return None
                
        else:
            print(f"❌ Error en query: {result.get('error', 'Error desconocido')}")
            return None
            
    except Exception as e:
        print(f"❌ Error ejecutando query: {e}")
        return None

async def query_cuenta_hacker_con_consultas_ato():
    """
    Query alternativa combinando con CONSULTAS_ATO para más contexto
    """
    
    print("\n" + "="*80)
    print("🔍 QUERY ALTERNATIVA: CUENTA_HACKER + CONSULTAS_ATO")
    print("📋 Para obtener contexto adicional de casos ATO")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # Query con JOIN a CONSULTAS_ATO para contexto adicional
    query_join = """
    SELECT 
        res.SENTENCE_ID,
        res.USER_ID,
        res.INFRACTION_TYPE,
        res.SENTENCE_DATE,
        res.COLOR_DE_TARJETA,
        res.SIT_SITE_ID,
        res.SENTENCE_LAST_STATUS,
        -- Datos adicionales de CONSULTAS_ATO si existe JOIN
        ato.GCA_ID as caso_ato_id,
        ato.GCA_SUBTYPE as tipo_caso_ato,
        ato.fecha_apertura_caso,
        ato.fecha_cierre_caso
    FROM 
        `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res
    LEFT JOIN 
        `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO` ato
        ON res.USER_ID = ato.GCA_CUST_ID
        AND DATE(ato.fecha_apertura_caso) BETWEEN DATE_SUB(DATE(res.SENTENCE_DATE), INTERVAL 30 DAY) 
                                               AND DATE_ADD(DATE(res.SENTENCE_DATE), INTERVAL 7 DAY)
    WHERE 
        res.INFRACTION_TYPE = 'CUENTA_DE_HACKER'
        AND res.SENTENCE_DATE >= '2025-04-01'
        AND res.SENTENCE_DATE <= '2025-05-31'
        AND res.SENTENCE_LAST_STATUS IN ('ACTIVE', 'ROLLBACKED')
        
    ORDER BY 
        res.SENTENCE_DATE DESC,
        res.SENTENCE_ID DESC
    """
    
    try:
        print("📊 Ejecutando query con contexto ATO...")
        result = await operations.execute_query(query_join, 50)
        
        if result["status"] == "success" and result["result"]["rows"]:
            rows = result["result"]["rows"]
            print(f"✅ Query con JOIN ejecutada: {len(rows)} resultados")
            
            # Contar cuántos tienen contexto ATO
            con_ato = sum(1 for row in rows if row.get('caso_ato_id'))
            print(f"📊 Casos con contexto ATO: {con_ato}/{len(rows)}")
            
            if con_ato > 0:
                print(f"\n📄 Casos con contexto ATO (primeros 5):")
                count = 0
                for row in rows:
                    if row.get('caso_ato_id') and count < 5:
                        print(f"   SENTENCE: {row.get('SENTENCE_ID')} -> ATO CASO: {row.get('caso_ato_id')} ({row.get('tipo_caso_ato')})")
                        count += 1
            
            return rows
        else:
            print("❌ Query con JOIN no retornó resultados")
            return None
            
    except Exception as e:
        print(f"❌ Error en query con JOIN: {e}")
        return None

async def main():
    """Función principal"""
    print("🚀 RECREANDO QUERY: 'Se contactan CUENTA HACKER'")
    print("📋 Basado en archivo Excel + MCP BigQuery")
    print("🎯 Tablas: BT_RES_RESTRICTIONS_INFRACTIONS_NW + CONSULTAS_ATO")
    print("=" * 80)
    
    # Ejecutar query principal
    casos_principales = await query_cuenta_hacker()
    
    # Si hay resultados, ejecutar query con contexto adicional
    if casos_principales:
        casos_con_contexto = await query_cuenta_hacker_con_consultas_ato()
        
        print(f"\n" + "="*80)
        print(f"🎉 ¡QUERIES EJECUTADAS EXITOSAMENTE!")
        print(f"✅ MCP BigQuery funcionando perfectamente")
        print(f"📊 Casos CUENTA_DE_HACKER encontrados: {len(casos_principales) if casos_principales else 0}")
        if casos_con_contexto:
            print(f"📊 Casos con contexto ATO: {len(casos_con_contexto)}")
    else:
        print(f"\n💡 No se encontraron casos en el período especificado")
        print(f"✅ MCP BigQuery funcionando correctamente")

if __name__ == "__main__":
    asyncio.run(main()) 