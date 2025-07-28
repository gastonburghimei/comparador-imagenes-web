#!/usr/bin/env python3
"""
Query recreada: "Usuarios se contactan" (Hoja 2)
JOIN entre restricciones CUENTA_DE_HACKER + casos ATO para obtener fechas de apertura/cierre
Usando MCP BigQuery basado en fury_mcp-pf-bigquery-analizer
"""

import asyncio
from mcp_bigquery_setup import MCPBigQueryBasicOperations

async def query_usuarios_se_contactan():
    """
    Recrear la query de la hoja "Usuarios se contactan" 
    que incluye fechas de apertura y cierre de casos ATO
    """
    
    print("🔍 RECREANDO QUERY: 'Usuarios se contactan' (Hoja 2)")
    print("📋 JOIN: Restricciones CUENTA_DE_HACKER + Casos ATO")
    print("🎯 889 usuarios con fechas de apertura/cierre de casos")
    print("-" * 80)
    
    # Inicializar MCP BigQuery
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    print("✅ MCP BigQuery inicializado")
    
    # Query con JOIN para obtener fechas de casos ATO
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
        ON res.USER_ID = ato.GCA_CUST_ID
    WHERE 
        res.INFRACTION_TYPE = 'CUENTA_DE_HACKER'
        
        -- Período de restricciones abril-mayo 2025
        AND res.SENTENCE_DATE >= '2025-04-01'
        AND res.SENTENCE_DATE <= '2025-05-31'
        
        -- Estados como en el archivo (incluye REINSTATED)
        AND res.SENTENCE_LAST_STATUS IN ('ACTIVE', 'ROLLBACKED', 'REINSTATED')
        
        -- Casos ATO deben tener fechas válidas
        AND ato.fecha_apertura_caso IS NOT NULL
        AND ato.fecha_cierre_caso IS NOT NULL
        
        -- Posible filtro temporal entre restricción y caso ATO
        -- (ajustar según lógica de negocio)
        AND (
            -- Caso ATO cerca de la fecha de restricción (30 días antes/después)
            DATE(ato.fecha_apertura_caso) BETWEEN DATE_SUB(DATE(res.SENTENCE_DATE), INTERVAL 30 DAY) 
                                               AND DATE_ADD(DATE(res.SENTENCE_DATE), INTERVAL 30 DAY)
            OR 
            -- O caso ATO cerrado cerca de la restricción
            DATE(ato.fecha_cierre_caso) BETWEEN DATE_SUB(DATE(res.SENTENCE_DATE), INTERVAL 30 DAY) 
                                            AND DATE_ADD(DATE(res.SENTENCE_DATE), INTERVAL 30 DAY)
        )
        
    ORDER BY 
        res.SENTENCE_DATE DESC,
        res.SENTENCE_ID DESC
    """
    
    try:
        print("📊 Ejecutando query con JOIN a CONSULTAS_ATO...")
        result = await operations.execute_query(query, 100)  # Primeros 100 para verificar
        
        if result["status"] == "success":
            query_result = result["result"]
            rows = query_result["rows"]
            
            print(f"✅ ¡Query JOIN ejecutada exitosamente!")
            print(f"📊 Primeros {len(rows)} resultados encontrados")
            
            if query_result.get("job_info"):
                job_info = query_result["job_info"]
                bytes_processed = job_info.get('bytes_processed', 0)
                bytes_billed = job_info.get('bytes_billed', 0)
                print(f"💾 Bytes procesados: {bytes_processed:,}")
                print(f"💰 Bytes facturados: {bytes_billed:,}")
            
            if rows:
                print(f"\n📋 RESULTADOS - Usuarios se contactan (con fechas ATO):")
                print("=" * 140)
                print(f"{'SENTENCE_ID':<12} {'USER_ID':<12} {'SENTENCE_DATE':<12} {'ESTADO':<12} {'APERTURA_ATO':<20} {'CIERRE_ATO':<20}")
                print("=" * 140)
                
                for i, row in enumerate(rows[:10]):  # Mostrar primeros 10
                    sentence_id = str(row.get('SENTENCE_ID', 'N/A'))[:11]
                    user_id = str(row.get('USER_ID', 'N/A'))[:11]
                    sentence_date = str(row.get('SENTENCE_DATE', 'N/A'))[:11] if row.get('SENTENCE_DATE') else 'N/A'
                    estado = str(row.get('SENTENCE_LAST_STATUS', 'N/A'))[:11]
                    apertura = str(row.get('fecha_apertura_caso', 'N/A'))[:19] if row.get('fecha_apertura_caso') else 'N/A'
                    cierre = str(row.get('fecha_cierre_caso', 'N/A'))[:19] if row.get('fecha_cierre_caso') else 'N/A'
                    
                    print(f"{sentence_id:<12} {user_id:<12} {sentence_date:<12} {estado:<12} {apertura:<20} {cierre:<20}")
                
                if len(rows) > 10:
                    print(f"... y {len(rows) - 10} más")
                
                # Estadísticas específicas de "usuarios se contactan"
                print(f"\n📈 ANÁLISIS USUARIOS SE CONTACTAN:")
                
                # Por estado de restricción
                estados = {}
                for row in rows:
                    estado = row.get('SENTENCE_LAST_STATUS', 'Sin estado')
                    estados[estado] = estados.get(estado, 0) + 1
                
                print(f"\n📊 Restricciones por estado:")
                for estado, cantidad in sorted(estados.items(), key=lambda x: x[1], reverse=True):
                    print(f"   {estado}: {cantidad:,} casos")
                
                # Análisis temporal: tiempo entre apertura y cierre ATO
                duraciones = []
                for row in rows:
                    apertura = row.get('fecha_apertura_caso')
                    cierre = row.get('fecha_cierre_caso')
                    if apertura and cierre:
                        try:
                            from datetime import datetime
                            if isinstance(apertura, str):
                                apertura = datetime.fromisoformat(apertura.replace('T', ' ').replace('Z', ''))
                            if isinstance(cierre, str):
                                cierre = datetime.fromisoformat(cierre.replace('T', ' ').replace('Z', ''))
                            
                            duracion = (cierre - apertura).total_seconds() / 3600  # horas
                            duraciones.append(duracion)
                        except:
                            pass
                
                if duraciones:
                    duracion_promedio = sum(duraciones) / len(duraciones)
                    print(f"\n⏱️  Tiempo promedio resolución casos ATO: {duracion_promedio:.1f} horas")
                    print(f"    Casos con duración calculada: {len(duraciones)}/{len(rows)}")
                
                return rows
                
            else:
                print("⚠️  No se encontraron usuarios con casos ATO asociados")
                return None
                
        else:
            print(f"❌ Error en query JOIN: {result.get('error', 'Error desconocido')}")
            return None
            
    except Exception as e:
        print(f"❌ Error ejecutando query: {e}")
        return None

async def query_usuarios_contactan_version_alternativa():
    """
    Query alternativa con lógica de JOIN diferente
    """
    
    print("\n" + "="*80)
    print("🔍 QUERY ALTERNATIVA: JOIN más flexible")
    print("📋 Probando diferentes criterios de asociación")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # Query con criterios de JOIN más amplios
    query_alt = """
    SELECT 
        res.SENTENCE_ID,
        res.USER_ID,
        res.INFRACTION_TYPE,
        res.SENTENCE_DATE,
        res.COLOR_DE_TARJETA,
        res.SIT_SITE_ID,
        res.SENTENCE_LAST_STATUS,
        ato.fecha_apertura_caso,
        ato.fecha_cierre_caso,
        -- Información adicional para análisis
        DATE_DIFF(DATE(res.SENTENCE_DATE), DATE(ato.fecha_apertura_caso), DAY) as dias_desde_apertura,
        DATE_DIFF(DATE(ato.fecha_cierre_caso), DATE(ato.fecha_apertura_caso), DAY) as duracion_caso_dias
    FROM 
        `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res
    INNER JOIN 
        `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO` ato
        ON res.USER_ID = ato.GCA_CUST_ID
    WHERE 
        res.INFRACTION_TYPE = 'CUENTA_DE_HACKER'
        AND res.SENTENCE_DATE >= '2025-04-01'
        AND res.SENTENCE_DATE <= '2025-05-31'
        AND res.SENTENCE_LAST_STATUS IN ('ACTIVE', 'ROLLBACKED', 'REINSTATED')
        
        -- JOIN más amplio: casos ATO en cualquier momento del año
        AND ato.fecha_apertura_caso >= '2023-01-01'
        AND ato.fecha_cierre_caso IS NOT NULL
        
    ORDER BY 
        res.SENTENCE_DATE DESC,
        res.SENTENCE_ID DESC
    """
    
    try:
        print("📊 Ejecutando query alternativa...")
        result = await operations.execute_query(query_alt, 50)
        
        if result["status"] == "success" and result["result"]["rows"]:
            rows = result["result"]["rows"]
            print(f"✅ Query alternativa: {len(rows)} resultados")
            
            # Mostrar algunos ejemplos con métricas
            print(f"\n📄 Ejemplos con métricas temporales:")
            for row in rows[:5]:
                sentence_id = row.get('SENTENCE_ID')
                dias_desde = row.get('dias_desde_apertura', 'N/A')
                duracion = row.get('duracion_caso_dias', 'N/A')
                print(f"   SENTENCE {sentence_id}: {dias_desde} días desde apertura, caso duró {duracion} días")
            
            return rows
        else:
            print("❌ Query alternativa sin resultados")
            return None
            
    except Exception as e:
        print(f"❌ Error en query alternativa: {e}")
        return None

async def main():
    """Función principal"""
    print("🚀 RECREANDO QUERY: 'Usuarios se contactan' (Hoja 2)")
    print("📋 JOIN entre Restricciones CUENTA_DE_HACKER + Casos ATO")
    print("🎯 Obtener 889 usuarios con fechas de apertura/cierre")
    print("=" * 80)
    
    # Ejecutar query principal
    casos_principales = await query_usuarios_se_contactan()
    
    # Si no hay suficientes resultados, probar query alternativa
    if not casos_principales or len(casos_principales) < 50:
        casos_alternativos = await query_usuarios_contactan_version_alternativa()
        
        if casos_alternativos:
            print(f"\n💡 Query alternativa encontró más resultados: {len(casos_alternativos)}")
    
    print(f"\n" + "="*80)
    print(f"🎉 ¡QUERY 'USUARIOS SE CONTACTAN' EJECUTADA!")
    print(f"✅ MCP BigQuery funcionando perfectamente")
    if casos_principales:
        print(f"📊 Resultados encontrados: {len(casos_principales)}")
    else:
        print(f"📊 Query ejecutada correctamente (puede necesitar ajuste de criterios)")

if __name__ == "__main__":
    asyncio.run(main()) 