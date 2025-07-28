#!/usr/bin/env python3
"""
Exploración de datos reales para ajustar filtros de consulta
"""

import asyncio
from mcp_bigquery_setup import MCPBigQueryBasicOperations

async def explorar_valores_reales():
    """Explora los valores reales en las columnas para ajustar filtros"""
    
    print("🔍 EXPLORANDO VALORES REALES EN LAS TABLAS")
    print("📊 Para ajustar filtros de la consulta ATO fintech malware")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # 1. Explorar tipos/subtipos en CONSULTAS_ATO
    query_tipos = """
    SELECT 
        GCA_SUBTYPE as area_tipo,
        subtype1 as subtipo_1,
        GCA_SECOND_SUBTYPE as segundo_subtipo,
        COUNT(*) as cantidad_casos
    FROM `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO`
    WHERE 
        fecha_cierre_caso >= '2024-06-01'
        AND fecha_cierre_caso < '2024-08-01'
        AND fecha_cierre_caso IS NOT NULL
    GROUP BY GCA_SUBTYPE, subtype1, GCA_SECOND_SUBTYPE
    ORDER BY cantidad_casos DESC
    LIMIT 20
    """
    
    print("📊 1. Explorando tipos/áreas en CONSULTAS_ATO (jun-jul 2024):")
    result1 = await operations.execute_query(query_tipos, 20)
    
    if result1["status"] == "success" and result1["result"]["rows"]:
        print("✅ Tipos/áreas encontrados:")
        for row in result1["result"]["rows"]:
            area = row.get('area_tipo', 'NULL')
            sub1 = row.get('subtipo_1', 'NULL')
            sub2 = row.get('segundo_subtipo', 'NULL')
            cant = row.get('cantidad_casos', 0)
            print(f"   {area} | {sub1} | {sub2} = {cant:,} casos")
    
    # 2. Explorar tipos de acciones en BT_ACTION_MR
    query_acciones = """
    SELECT 
        ACTION_TYPE as tipo_accion,
        CASE_TYPE as tipo_caso,
        CASE_SUBTYPE as subtipo_caso,
        RESOLUTION as resolucion,
        COUNT(*) as cantidad
    FROM `meli-bi-data.WHOWNER.BT_ACTION_MR` b
    INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO` c
        ON b.CASE_ID = c.GCA_ID
    WHERE 
        c.fecha_cierre_caso >= '2024-06-01'
        AND c.fecha_cierre_caso < '2024-08-01'
    GROUP BY ACTION_TYPE, CASE_TYPE, CASE_SUBTYPE, RESOLUTION
    ORDER BY cantidad DESC
    LIMIT 20
    """
    
    print("\n📊 2. Explorando tipos de acciones en BT_ACTION_MR (jun-jul 2024):")
    result2 = await operations.execute_query(query_acciones, 20)
    
    if result2["status"] == "success" and result2["result"]["rows"]:
        print("✅ Tipos de acciones encontrados:")
        for row in result2["result"]["rows"]:
            accion = row.get('tipo_accion', 'NULL')
            tipo = row.get('tipo_caso', 'NULL')
            subtipo = row.get('subtipo_caso', 'NULL')
            resol = row.get('resolucion', 'NULL')
            cant = row.get('cantidad', 0)
            print(f"   {accion} | {tipo} | {subtipo} | {resol} = {cant:,} casos")
    
    # 3. Buscar términos específicos que podrían ser ATO/fintech/malware
    query_busqueda = """
    SELECT 
        'CONSULTAS_ATO - GCA_SUBTYPE' as tabla_campo,
        GCA_SUBTYPE as valor,
        COUNT(*) as casos
    FROM `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO`
    WHERE 
        fecha_cierre_caso >= '2024-06-01'
        AND fecha_cierre_caso < '2024-08-01'
        AND (
            UPPER(GCA_SUBTYPE) LIKE '%FRAUD%'
            OR UPPER(GCA_SUBTYPE) LIKE '%SECURITY%'
            OR UPPER(GCA_SUBTYPE) LIKE '%RISK%'
            OR UPPER(GCA_SUBTYPE) LIKE '%MALWARE%'
            OR UPPER(GCA_SUBTYPE) LIKE '%ATO%'
            OR UPPER(GCA_SUBTYPE) LIKE '%ACCOUNT%'
        )
    GROUP BY GCA_SUBTYPE
    
    UNION ALL
    
    SELECT 
        'BT_ACTION_MR - ACTION_TYPE' as tabla_campo,
        ACTION_TYPE as valor,
        COUNT(*) as casos
    FROM `meli-bi-data.WHOWNER.BT_ACTION_MR` b
    INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO` c
        ON b.CASE_ID = c.GCA_ID
    WHERE 
        c.fecha_cierre_caso >= '2024-06-01'
        AND c.fecha_cierre_caso < '2024-08-01'
        AND (
            UPPER(b.ACTION_TYPE) LIKE '%MANUAL%'
            OR UPPER(b.ACTION_TYPE) LIKE '%REVIEW%'
            OR UPPER(b.ACTION_TYPE) LIKE '%FRAUD%'
        )
    GROUP BY ACTION_TYPE
    
    ORDER BY casos DESC
    LIMIT 15
    """
    
    print("\n📊 3. Buscando términos relacionados con fraude/seguridad/manual:")
    result3 = await operations.execute_query(query_busqueda, 15)
    
    if result3["status"] == "success" and result3["result"]["rows"]:
        print("✅ Términos relacionados encontrados:")
        for row in result3["result"]["rows"]:
            tabla = row.get('tabla_campo', 'N/A')
            valor = row.get('valor', 'NULL')
            casos = row.get('casos', 0)
            print(f"   {tabla}: '{valor}' = {casos:,} casos")
    
    # 4. Estadísticas generales del período
    query_stats = """
    SELECT 
        COUNT(*) as total_casos_periodo,
        COUNT(DISTINCT c.GCA_CUST_ID) as usuarios_unicos,
        COUNT(DISTINCT b.ADMIN_ID) as gestores_unicos
    FROM `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO` c
    LEFT JOIN `meli-bi-data.WHOWNER.BT_ACTION_MR` b
        ON c.GCA_ID = b.CASE_ID
    WHERE 
        c.fecha_cierre_caso >= '2024-06-01'
        AND c.fecha_cierre_caso < '2024-08-01'
    """
    
    print("\n📊 4. Estadísticas generales del período jun-jul 2024:")
    result4 = await operations.execute_query(query_stats, 1)
    
    if result4["status"] == "success" and result4["result"]["rows"]:
        stats = result4["result"]["rows"][0]
        total = stats.get('total_casos_periodo', 0)
        usuarios = stats.get('usuarios_unicos', 0)
        gestores = stats.get('gestores_unicos', 0)
        print(f"✅ Total casos período: {total:,}")
        print(f"✅ Usuarios únicos: {usuarios:,}")
        print(f"✅ Gestores únicos: {gestores:,}")

async def main():
    print("🚀 EXPLORACIÓN DE DATOS - MCP BigQuery")
    print("📋 Identificando valores reales para filtros ATO fintech malware")
    print("=" * 80)
    
    await explorar_valores_reales()
    
    print("\n" + "=" * 80)
    print("💡 PRÓXIMOS PASOS:")
    print("1. Usar los valores reales encontrados para ajustar filtros")
    print("2. Crear consulta específica con terminología correcta")
    print("3. Obtener los casos que realmente necesitas")

if __name__ == "__main__":
    asyncio.run(main()) 