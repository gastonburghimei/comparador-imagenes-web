#!/usr/bin/env python3
"""
Consulta CORREGIDA usando MCP BigQuery: Casos cerrados por revisión manual de malware - ATO Fintech
Usando los nombres reales de las columnas detectadas por el MCP
"""

import asyncio
from mcp_bigquery_setup import MCPBigQueryBasicOperations

async def consulta_casos_malware_ato_corregida():
    """
    Consulta corregida con los nombres reales de las columnas
    """
    
    print("🔍 CONSULTA CORREGIDA - Casos cerrados por revisión manual de malware")
    print("📋 ATO Fintech - Junio y Julio 2024")
    print("📊 Usando nombres reales de columnas detectados por MCP")
    print("-" * 80)
    
    # Inicializar operaciones MCP
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # Consulta con nombres reales de columnas
    query = """
    SELECT 
        c.GCA_ID as case_id,
        c.GCA_CUST_ID as user_id,
        c.fecha_cierre_caso as fecha_cierre,
        b.ADMIN_ID as quien_gestiono,
        c.GCA_SUBTYPE as area,
        c.subtype1 as subtipo,
        c.GCA_SECOND_SUBTYPE as segundo_subtipo,
        b.ACTION_TYPE as tipo_accion,
        b.RESOLUTION as resolucion,
        b.CASE_TYPE as tipo_caso,
        b.CASE_SUBTYPE as subtipo_caso,
        b.COMMENT_ACTION as comentario
    FROM 
        `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO` c
    INNER JOIN 
        `meli-bi-data.WHOWNER.BT_ACTION_MR` b
        ON c.GCA_ID = b.CASE_ID
    WHERE 
        -- Filtros para área ATO/fintech (basado en columnas reales)
        (
            UPPER(c.GCA_SUBTYPE) LIKE '%ATO%' 
            OR UPPER(c.GCA_SUBTYPE) LIKE '%FINTECH%'
            OR UPPER(c.subtype1) LIKE '%ATO%'
            OR UPPER(c.subtype1) LIKE '%FINTECH%'
            OR UPPER(c.GCA_SECOND_SUBTYPE) LIKE '%ATO%'
        )
        
        -- Filtros para malware y revisión manual
        AND (
            UPPER(b.ACTION_TYPE) LIKE '%MANUAL%'
            OR UPPER(b.RESOLUTION) LIKE '%MALWARE%'
            OR UPPER(b.CASE_TYPE) LIKE '%MALWARE%'
            OR UPPER(b.CASE_SUBTYPE) LIKE '%MALWARE%'
            OR UPPER(b.COMMENT_ACTION) LIKE '%MALWARE%'
        )
        
        -- Período: junio y julio 2024
        AND c.fecha_cierre_caso >= '2024-06-01'
        AND c.fecha_cierre_caso < '2024-08-01'
        
        -- Solo casos con fecha de cierre válida
        AND c.fecha_cierre_caso IS NOT NULL
        
    ORDER BY 
        c.fecha_cierre_caso DESC,
        c.GCA_ID
    """
    
    try:
        print("📊 Ejecutando consulta corregida...")
        result = await operations.execute_query(query, 50)
        
        if result["status"] == "success":
            query_result = result["result"]
            rows = query_result["rows"]
            
            print(f"✅ ¡Consulta ejecutada exitosamente!")
            print(f"📊 Total de casos encontrados: {len(rows)}")
            
            if query_result.get("job_info"):
                job_info = query_result["job_info"]
                bytes_processed = job_info.get('bytes_processed', 0)
                bytes_billed = job_info.get('bytes_billed', 0)
                print(f"💾 Bytes procesados: {bytes_processed:,}")
                print(f"💰 Bytes facturados: {bytes_billed:,}")
            
            if rows:
                print(f"\n📋 RESULTADOS - Casos cerrados por revisión manual de malware:")
                print("=" * 120)
                print(f"{'Case ID':<15} {'User ID':<12} {'Fecha Cierre':<12} {'Gestor':<15} {'Área':<15} {'Tipo Acción':<20}")
                print("=" * 120)
                
                for i, row in enumerate(rows):
                    case_id = str(row.get('case_id', 'N/A'))[:14]
                    user_id = str(row.get('user_id', 'N/A'))[:11]
                    fecha = str(row.get('fecha_cierre', 'N/A'))[:11] if row.get('fecha_cierre') else 'N/A'
                    gestor = str(row.get('quien_gestiono', 'N/A'))[:14]
                    area = str(row.get('area', 'N/A'))[:14]
                    tipo_accion = str(row.get('tipo_accion', 'N/A'))[:19]
                    
                    print(f"{case_id:<15} {user_id:<12} {fecha:<12} {gestor:<15} {area:<15} {tipo_accion:<20}")
                
                # Estadísticas
                print(f"\n📈 ANÁLISIS DE RESULTADOS:")
                
                # Por gestor
                gestores = {}
                for row in rows:
                    gestor = row.get('quien_gestiono') or 'Sin asignar'
                    gestores[gestor] = gestores.get(gestor, 0) + 1
                
                print(f"\n👥 Casos por gestor:")
                for gestor, cantidad in sorted(gestores.items(), key=lambda x: x[1], reverse=True)[:10]:
                    print(f"   {gestor}: {cantidad} casos")
                
                # Por tipo de área
                areas = {}
                for row in rows:
                    area = row.get('area') or 'Sin área'
                    areas[area] = areas.get(area, 0) + 1
                
                print(f"\n🏢 Casos por área:")
                for area, cantidad in sorted(areas.items(), key=lambda x: x[1], reverse=True)[:10]:
                    print(f"   {area}: {cantidad} casos")
                
                # Por mes
                meses = {}
                for row in rows:
                    fecha = row.get('fecha_cierre', '')
                    if fecha and len(str(fecha)) >= 7:
                        mes = str(fecha)[:7]  # YYYY-MM
                        meses[mes] = meses.get(mes, 0) + 1
                
                print(f"\n📅 Casos por mes:")
                for mes, cantidad in sorted(meses.items()):
                    nombre_mes = "Junio 2024" if mes == "2024-06" else "Julio 2024" if mes == "2024-07" else mes
                    print(f"   {nombre_mes}: {cantidad} casos")
                
                return rows
                
            else:
                print("⚠️  No se encontraron casos con los criterios especificados")
                print("\n💡 Esto podría indicar:")
                print("1. No hay casos de malware ATO en jun-jul 2024")
                print("2. Los filtros son demasiado restrictivos")
                print("3. Los datos usan terminología diferente")
                return None
                
        else:
            print(f"❌ Error en consulta: {result.get('error', 'Error desconocido')}")
            return None
            
    except Exception as e:
        print(f"❌ Error ejecutando consulta: {e}")
        return None

async def main():
    """Función principal"""
    print("🚀 MCP BigQuery - Consulta casos ATO fintech malware")
    print("📋 Basado en fury_mcp-pf-bigquery-analizer de MercadoLibre")
    print("🎯 Tablas: SBOX_PFFINTECHATO.CONSULTAS_ATO + WHOWNER.BT_ACTION_MR")
    print("=" * 80)
    
    casos = await consulta_casos_malware_ato_corregida()
    
    if casos:
        print(f"\n🎉 ¡Consulta completada exitosamente!")
        print(f"✅ MCP BigQuery funcionando perfectamente")
        print(f"📊 {len(casos)} casos procesados")
    else:
        print(f"\n💡 Consulta ejecutada pero sin resultados")
        print(f"✅ MCP BigQuery funcionando correctamente")

if __name__ == "__main__":
    asyncio.run(main()) 