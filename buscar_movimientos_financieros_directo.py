#!/usr/bin/env python3
"""
BÚSQUEDA DIRECTA: Movimientos financieros
Buscar directamente en tablas conocidas de pagos/transferencias
"""

import asyncio
import pandas as pd
from mcp_bigquery_setup import MCPBigQueryBasicOperations

# Usuarios objetivo
USUARIOS_OBJETIVO = [1348718991, 468290404, 375845668]

async def buscar_en_bt_mp_pay_payments():
    """Búsqueda específica en BT_MP_PAY_PAYMENTS con más detalles"""
    
    print("🔍 BÚSQUEDA DETALLADA EN BT_MP_PAY_PAYMENTS")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    usuarios_str = "(" + ",".join(map(str, USUARIOS_OBJETIVO)) + ")"
    
    # 1. Verificar si los usuarios existen EN CUALQUIER FECHA
    query_usuarios_general = f"""
    SELECT 
        PAY_CUST_ID as user_id,
        COUNT(*) as total_registros,
        MIN(PAY_CREATION_DATE) as primera_transaccion,
        MAX(PAY_CREATION_DATE) as ultima_transaccion,
        COUNT(DISTINCT PAY_STATUS_ID) as estados_diferentes,
        SUM(CASE WHEN PAY_STATUS_ID = 'approved' THEN 1 ELSE 0 END) as transacciones_aprobadas
    FROM 
        `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
    WHERE 
        PAY_CUST_ID IN {usuarios_str}
    GROUP BY 
        PAY_CUST_ID
    ORDER BY 
        total_registros DESC
    """
    
    print("📊 Buscando usuarios en TODA la tabla BT_MP_PAY_PAYMENTS...")
    result = await operations.execute_query(query_usuarios_general, 10)
    
    if result["status"] == "success" and result["result"]["rows"]:
        print(f"✅ {len(result['result']['rows'])} usuarios encontrados en BT_MP_PAY_PAYMENTS:")
        
        usuarios_df = pd.DataFrame(result["result"]["rows"])
        
        for _, row in usuarios_df.iterrows():
            user_id = row['user_id']
            total = row['total_registros']
            primera = row['primera_transaccion']
            ultima = row['ultima_transaccion']
            aprobadas = row['transacciones_aprobadas']
            
            print(f"   👤 Usuario {user_id}:")
            print(f"      📈 Total registros: {total:,}")
            print(f"      ✅ Transacciones aprobadas: {aprobadas:,}")
            print(f"      📅 Rango: {primera} → {ultima}")
        
        return usuarios_df
    else:
        print("❌ No se encontraron usuarios en BT_MP_PAY_PAYMENTS")
        return None

async def buscar_movimientos_periodo_especifico():
    """Buscar movimientos en períodos específicos alrededor de las infracciones"""
    
    print("\n🔍 BÚSQUEDA POR PERÍODOS ESPECÍFICOS")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # Fechas de infracciones conocidas (del script anterior)
    periodos_usuarios = [
        (375845668, "2025-05-07", "2025-04-07"),  # 30 días antes de 2025-05-07
        (468290404, "2025-03-28", "2025-02-26"),  # 30 días antes de 2025-03-28
        (1348718991, "2025-02-10", "2025-01-11"), # 30 días antes de 2025-02-10
    ]
    
    for user_id, fecha_infraccion, fecha_inicio in periodos_usuarios:
        print(f"\n👤 Usuario {user_id} - Período: {fecha_inicio} → {fecha_infraccion}")
        
        query_movimientos = f"""
        SELECT 
            PAY_PAYMENT_ID,
            PAY_CUST_ID,
            PAY_CREATION_DATE,
            PAY_STATUS_ID,
            PAY_OPERATION_TYPE_ID,
            PAY_PM_TYPE_ID,
            PAY_TRANSACTION_DOL_AMT,
            PAY_TOTAL_PAID_DOL_AMT,
            
            -- Clasificar movimientos
            CASE 
                WHEN PAY_OPERATION_TYPE_ID IN ('account_fund', 'money_in', 'transfer_in') THEN 'INGRESO'
                WHEN PAY_OPERATION_TYPE_ID IN ('money_out', 'transfer_out', 'cashout', 'withdrawal') THEN 'RETIRO'
                WHEN PAY_OPERATION_TYPE_ID LIKE '%exchange%' THEN 'INTERCAMBIO'
                ELSE PAY_OPERATION_TYPE_ID
            END as tipo_movimiento
            
        FROM 
            `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
        WHERE 
            PAY_CUST_ID = {user_id}
            AND PAY_CREATION_DATE >= '{fecha_inicio}'
            AND PAY_CREATION_DATE < '{fecha_infraccion}'
            AND PAY_TRANSACTION_DOL_AMT > 0
            
        ORDER BY 
            PAY_CREATION_DATE ASC
        LIMIT 50
        """
        
        result = await operations.execute_query(query_movimientos, 50)
        
        if result["status"] == "success" and result["result"]["rows"]:
            movimientos = pd.DataFrame(result["result"]["rows"])
            
            print(f"   💰 {len(movimientos)} movimientos encontrados")
            
            # Agrupar por tipo de movimiento
            tipos = movimientos.groupby('tipo_movimiento').agg({
                'PAY_TRANSACTION_DOL_AMT': ['count', 'sum']
            }).round(2)
            
            print("   📊 Resumen por tipo:")
            for tipo_mov in tipos.index:
                count = tipos.loc[tipo_mov, ('PAY_TRANSACTION_DOL_AMT', 'count')]
                suma = tipos.loc[tipo_mov, ('PAY_TRANSACTION_DOL_AMT', 'sum')]
                print(f"      {tipo_mov}: {count:,} operaciones, ${suma:,.2f} USD")
            
            # Mostrar primeros movimientos
            print("   🔍 Primeros movimientos:")
            for _, mov in movimientos.head(5).iterrows():
                fecha = mov['PAY_CREATION_DATE']
                tipo = mov['tipo_movimiento']
                monto = mov['PAY_TRANSACTION_DOL_AMT']
                operacion = mov['PAY_OPERATION_TYPE_ID']
                print(f"      {fecha}: {tipo} ${monto:.2f} ({operacion})")
                
        else:
            print(f"   ❌ No se encontraron movimientos para período específico")

async def explorar_tipos_operacion_disponibles():
    """Explorar qué tipos de operaciones existen en la tabla"""
    
    print("\n🔍 EXPLORANDO TIPOS DE OPERACIONES DISPONIBLES")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    query_tipos = """
    SELECT 
        PAY_OPERATION_TYPE_ID,
        COUNT(*) as frecuencia,
        AVG(PAY_TRANSACTION_DOL_AMT) as monto_promedio,
        SUM(PAY_TRANSACTION_DOL_AMT) as monto_total
    FROM 
        `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
    WHERE 
        PAY_CREATION_DATE >= '2024-01-01'
        AND PAY_STATUS_ID = 'approved'
        AND PAY_TRANSACTION_DOL_AMT > 0
    GROUP BY 
        PAY_OPERATION_TYPE_ID
    ORDER BY 
        frecuencia DESC
    LIMIT 20
    """
    
    print("📊 Tipos de operaciones más comunes (2024+):")
    result = await operations.execute_query(query_tipos, 25)
    
    if result["status"] == "success" and result["result"]["rows"]:
        print("✅ Tipos de operaciones encontrados:")
        for row in result["result"]["rows"]:
            tipo = row['PAY_OPERATION_TYPE_ID']
            freq = row['frecuencia']
            promedio = row['monto_promedio']
            total = row['monto_total']
            
            # Clasificar el tipo
            if any(term in tipo.lower() for term in ['fund', 'in', 'deposit']):
                categoria = "💰 INGRESO"
            elif any(term in tipo.lower() for term in ['out', 'withdraw', 'cash']):
                categoria = "💸 RETIRO"
            elif 'exchange' in tipo.lower():
                categoria = "🔄 INTERCAMBIO"
            else:
                categoria = "❓ OTRO"
            
            print(f"   {categoria} {tipo}: {freq:,} ops (${promedio:.2f} prom, ${total:,.0f} total)")

async def buscar_en_otras_tablas_pagos():
    """Buscar en otras posibles tablas de pagos"""
    
    print("\n🔍 BUSCANDO EN OTRAS TABLAS DE PAGOS")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    usuarios_str = "(" + ",".join(map(str, USUARIOS_OBJETIVO)) + ")"
    
    # Posibles tablas alternativas
    tablas_alternativas = [
        "BT_TRANSACTIONS",
        "BT_MONEY_MOVEMENTS", 
        "BT_TRANSFERS",
        "BT_WALLET_TRANSACTIONS",
        "BT_PAYMENT_OPERATIONS"
    ]
    
    for tabla in tablas_alternativas:
        print(f"\n🔍 Verificando tabla: {tabla}")
        
        # Verificar si existe
        query_existe = f"""
        SELECT COUNT(*) as existe
        FROM `meli-bi-data.WHOWNER.__TABLES__`
        WHERE table_name = '{tabla}'
        """
        
        try:
            existe_result = await operations.execute_query(query_existe, 5)
            
            if existe_result["status"] == "success" and existe_result["result"]["rows"]:
                existe = existe_result["result"]["rows"][0].get('existe', 0)
                
                if existe > 0:
                    print(f"   ✅ Tabla {tabla} encontrada")
                    # Aquí podríamos buscar usuarios específicos
                else:
                    print(f"   ❌ Tabla {tabla} no existe")
            else:
                print(f"   ❌ Error verificando {tabla}")
                
        except Exception as e:
            print(f"   ❌ Error con {tabla}: {str(e)[:80]}...")

async def main():
    """Función principal"""
    
    print("🎯 BÚSQUEDA DIRECTA: MOVIMIENTOS FINANCIEROS")
    print("👥 Usuarios: " + str(USUARIOS_OBJETIVO))
    print("=" * 80)
    
    try:
        # 1. Buscar en BT_MP_PAY_PAYMENTS general
        usuarios_encontrados = await buscar_en_bt_mp_pay_payments()
        
        # 2. Buscar en períodos específicos
        await buscar_movimientos_periodo_especifico()
        
        # 3. Explorar tipos de operaciones
        await explorar_tipos_operacion_disponibles()
        
        # 4. Buscar en otras tablas
        await buscar_en_otras_tablas_pagos()
        
        print("\n" + "=" * 80)
        print("✅ BÚSQUEDA COMPLETADA")
        
        if usuarios_encontrados is not None:
            print("🎯 Los usuarios SÍ tienen movimientos financieros")
            print("💡 Puede que el período de búsqueda original sea demasiado restrictivo")
        else:
            print("❌ Los usuarios NO tienen movimientos en BT_MP_PAY_PAYMENTS")
            print("💡 Pueden estar en otras tablas o datasets")
        
    except Exception as e:
        print(f"❌ Error en búsqueda: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 