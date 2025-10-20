#!/usr/bin/env python3
"""
EXPLORACIÓN EXHAUSTIVA: Tablas financieras
Buscar dónde están los movimientos de dinero de los usuarios específicos
"""

import asyncio
import pandas as pd
from mcp_bigquery_setup import MCPBigQueryBasicOperations

# Usuarios objetivo
USUARIOS_OBJETIVO = [1348718991, 468290404, 375845668]

async def explorar_datasets_disponibles():
    """Explorar qué datasets están disponibles"""
    
    print("🔍 EXPLORANDO DATASETS DISPONIBLES")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # Listar datasets
    datasets_result = await operations.list_datasets()
    
    if datasets_result["status"] == "success":
        print("✅ Datasets encontrados:")
        for dataset in datasets_result["result"]["datasets"]:
            dataset_id = dataset["id"]
            print(f"   📂 {dataset_id}")
            
            # Si contiene términos financieros, explorar más
            if any(term in dataset_id.lower() for term in ['pay', 'payment', 'money', 'transfer', 'wallet', 'transaction']):
                print(f"      🎯 Dataset financiero detectado!")
    
    return datasets_result

async def buscar_tablas_financieras():
    """Buscar tablas que puedan contener información financiera"""
    
    print("\n🔍 BUSCANDO TABLAS FINANCIERAS ESPECÍFICAS")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # Buscar tablas en WHOWNER que contengan términos financieros
    query_tablas = """
    SELECT 
        table_name,
        table_type,
        creation_time,
        row_count
    FROM 
        `meli-bi-data.WHOWNER.__TABLES__`
    WHERE 
        table_name LIKE '%PAY%'
        OR table_name LIKE '%PAYMENT%'
        OR table_name LIKE '%MONEY%'
        OR table_name LIKE '%TRANSFER%'
        OR table_name LIKE '%TRANSACTION%'
        OR table_name LIKE '%WALLET%'
        OR table_name LIKE '%BALANCE%'
        OR table_name LIKE '%CREDIT%'
        OR table_name LIKE '%DEBIT%'
    ORDER BY 
        table_name
    """
    
    print("📊 Buscando tablas financieras en WHOWNER...")
    result = await operations.execute_query(query_tablas, 50)
    
    if result["status"] == "success" and result["result"]["rows"]:
        print(f"✅ {len(result['result']['rows'])} tablas financieras encontradas:")
        
        tablas_financieras = []
        for row in result["result"]["rows"]:
            table_name = row['table_name']
            table_type = row.get('table_type', 'N/A')
            row_count = row.get('row_count', 'N/A')
            tablas_financieras.append(table_name)
            print(f"   💰 {table_name} ({table_type}) - {row_count:,} filas" if isinstance(row_count, int) else f"   💰 {table_name} ({table_type}) - {row_count} filas")
        
        return tablas_financieras
    else:
        print("❌ No se encontraron tablas financieras")
        return []

async def verificar_estructura_bt_mp_pay_payments():
    """Verificar la estructura real de BT_MP_PAY_PAYMENTS"""
    
    print("\n🔍 VERIFICANDO ESTRUCTURA DE BT_MP_PAY_PAYMENTS")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # Obtener schema completo
    query_schema = """
    SELECT 
        column_name,
        data_type,
        is_nullable,
        description
    FROM 
        `meli-bi-data.WHOWNER.INFORMATION_SCHEMA.COLUMNS`
    WHERE 
        table_name = 'BT_MP_PAY_PAYMENTS'
    ORDER BY 
        ordinal_position
    LIMIT 20
    """
    
    print("📋 Obteniendo schema de BT_MP_PAY_PAYMENTS...")
    schema_result = await operations.execute_query(query_schema, 25)
    
    if schema_result["status"] == "success" and schema_result["result"]["rows"]:
        print("✅ Columnas principales:")
        for row in schema_result["result"]["rows"]:
            col = row['column_name']
            tipo = row['data_type']
            nullable = row.get('is_nullable', 'N/A')
            desc = row.get('description', 'N/A')
            print(f"   📊 {col} ({tipo}) - Nullable: {nullable}")
    
    # Verificar si tiene datos y rango de fechas
    query_info = """
    SELECT 
        COUNT(*) as total_filas,
        MIN(PAY_CREATION_DATE) as fecha_min,
        MAX(PAY_CREATION_DATE) as fecha_max,
        COUNT(DISTINCT PAY_CUST_ID) as usuarios_unicos
    FROM 
        `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
    """
    
    print("\n📊 Información general de la tabla...")
    info_result = await operations.execute_query(query_info, 5)
    
    if info_result["status"] == "success" and info_result["result"]["rows"]:
        row = info_result["result"]["rows"][0]
        total = row.get('total_filas', 0)
        fecha_min = row.get('fecha_min', 'N/A')
        fecha_max = row.get('fecha_max', 'N/A')
        usuarios = row.get('usuarios_unicos', 0)
        
        print(f"   📈 Total filas: {total:,}")
        print(f"   📅 Fecha mínima: {fecha_min}")
        print(f"   📅 Fecha máxima: {fecha_max}")
        print(f"   👥 Usuarios únicos: {usuarios:,}")

async def buscar_usuarios_en_otras_tablas(tablas_financieras):
    """Buscar si los usuarios están en otras tablas financieras"""
    
    print("\n🔍 BUSCANDO USUARIOS EN OTRAS TABLAS FINANCIERAS")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    usuarios_str = "(" + ",".join(map(str, USUARIOS_OBJETIVO)) + ")"
    
    # Tablas financieras adicionales a revisar (basadas en archivos SQL existentes)
    tablas_adicionales = [
        "BT_MP_PAY_PAYMENTS",
        "BT_MP_MONEY_TRANSFER",  # Probable
        "BT_PAYMENT_TRANSACTIONS",  # Probable
        "BT_WALLET_MOVEMENTS",  # Probable
        "BT_MONEY_OPERATIONS",  # Probable
    ]
    
    for tabla in tablas_adicionales[:3]:  # Probar las primeras 3
        print(f"\n🔍 Verificando tabla: {tabla}")
        
        # Buscar si la tabla existe
        query_existe = f"""
        SELECT 
            COUNT(*) as existe
        FROM 
            `meli-bi-data.WHOWNER.__TABLES__`
        WHERE 
            table_name = '{tabla}'
        """
        
        existe_result = await operations.execute_query(query_existe, 5)
        
        if existe_result["status"] == "success" and existe_result["result"]["rows"]:
            existe = existe_result["result"]["rows"][0].get('existe', 0)
            
            if existe > 0:
                print(f"   ✅ Tabla {tabla} existe")
                
                # Buscar usuarios específicos
                # Intentar varias columnas posibles para USER_ID
                columnas_user_id = ['PAY_CUST_ID', 'USER_ID', 'CUST_ID', 'CUSTOMER_ID']
                
                for col_user in columnas_user_id:
                    query_usuarios = f"""
                    SELECT 
                        {col_user} as user_id,
                        COUNT(*) as registros,
                        MIN(PAY_CREATION_DATE) as fecha_min,
                        MAX(PAY_CREATION_DATE) as fecha_max
                    FROM 
                        `meli-bi-data.WHOWNER.{tabla}`
                    WHERE 
                        {col_user} IN {usuarios_str}
                    GROUP BY 
                        {col_user}
                    ORDER BY 
                        registros DESC
                    """
                    
                    try:
                        usuarios_result = await operations.execute_query(query_usuarios, 10)
                        
                        if usuarios_result["status"] == "success" and usuarios_result["result"]["rows"]:
                            print(f"   🎯 Usuarios encontrados en {tabla}.{col_user}:")
                            for row in usuarios_result["result"]["rows"]:
                                user_id = row['user_id']
                                registros = row['registros']
                                fecha_min = row.get('fecha_min', 'N/A')
                                fecha_max = row.get('fecha_max', 'N/A')
                                print(f"      👤 {user_id}: {registros:,} registros ({fecha_min} - {fecha_max})")
                            break
                        else:
                            continue
                            
                    except Exception as e:
                        # Columna no existe, probar la siguiente
                        continue
                else:
                    print(f"   ⚠️  No se encontraron usuarios en {tabla} (columnas user_id probadas)")
            else:
                print(f"   ❌ Tabla {tabla} no existe")

async def buscar_en_tablas_alternativas():
    """Buscar en tablas con otros nombres comunes"""
    
    print("\n🔍 BUSCANDO EN TABLAS CON NOMBRES ALTERNATIVOS")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    usuarios_str = "(" + ",".join(map(str, USUARIOS_OBJETIVO)) + ")"
    
    # Basado en los archivos SQL, probar estas tablas
    tablas_alternativas = [
        ("SBOX_PFFINTECHATO", "CONSULTAS_ATO", "GCA_CUST_ID"),
        ("WHOWNER", "BT_RES_RESTRICTIONS_INFRACTIONS_NW", "USER_ID"),
    ]
    
    for dataset, tabla, col_user in tablas_alternativas:
        print(f"\n🔍 Buscando movimientos en {dataset}.{tabla}")
        
        query_movimientos = f"""
        SELECT 
            {col_user} as user_id,
            COUNT(*) as registros,
            MIN(EXTRACT(DATE FROM fecha_apertura_caso)) as fecha_min,
            MAX(EXTRACT(DATE FROM fecha_cierre_caso)) as fecha_max
        FROM 
            `meli-bi-data.{dataset}.{tabla}`
        WHERE 
            {col_user} IN {usuarios_str}
        GROUP BY 
            {col_user}
        ORDER BY 
            registros DESC
        """
        
        try:
            result = await operations.execute_query(query_movimientos, 10)
            
            if result["status"] == "success" and result["result"]["rows"]:
                print(f"   ✅ Datos encontrados en {dataset}.{tabla}:")
                for row in result["result"]["rows"]:
                    user_id = row['user_id']
                    registros = row['registros']
                    fecha_min = row.get('fecha_min', 'N/A')
                    fecha_max = row.get('fecha_max', 'N/A')
                    print(f"      👤 {user_id}: {registros:,} registros ({fecha_min} - {fecha_max})")
            else:
                print(f"   ❌ No hay datos para estos usuarios en {dataset}.{tabla}")
                
        except Exception as e:
            print(f"   ❌ Error consultando {dataset}.{tabla}: {str(e)[:100]}...")

async def main():
    """Función principal de exploración"""
    
    print("🕵️ EXPLORACIÓN EXHAUSTIVA: TABLAS FINANCIERAS")
    print("🎯 Usuarios objetivo: " + str(USUARIOS_OBJETIVO))
    print("=" * 80)
    
    try:
        # 1. Explorar datasets disponibles
        await explorar_datasets_disponibles()
        
        # 2. Buscar tablas financieras específicas
        tablas_financieras = await buscar_tablas_financieras()
        
        # 3. Verificar estructura de BT_MP_PAY_PAYMENTS
        await verificar_estructura_bt_mp_pay_payments()
        
        # 4. Buscar usuarios en otras tablas
        await buscar_usuarios_en_otras_tablas(tablas_financieras)
        
        # 5. Buscar en tablas alternativas conocidas
        await buscar_en_tablas_alternativas()
        
        print("\n" + "=" * 80)
        print("🎯 EXPLORACIÓN COMPLETADA")
        
    except Exception as e:
        print(f"❌ Error en exploración: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 