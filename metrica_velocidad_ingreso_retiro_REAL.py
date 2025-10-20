#!/usr/bin/env python3
"""
MÉTRICA REAL DE VELOCIDAD: Ingreso → Retiro
Usuarios: 1348718991, 468290404, 375845668

FLUJO REAL:
1. INGRESO: Buscar en BT_MP_ACC_MOVEMENTS cuando ENTRA dinero al usuario
2. RETIRO: Buscar en payments/payouts/etc cuando SALE dinero
3. VELOCIDAD: Tiempo entre ingreso → retiro

Sin depender de marcas ATO/DTO - solo flujo de dinero real.
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from mcp_bigquery_setup import MCPBigQueryBasicOperations

# Usuarios objetivo
USUARIOS_OBJETIVO = [1348718991, 468290404, 375845668]

async def buscar_ingresos_acc_movements():
    """PASO 1: Buscar INGRESOS en BT_MP_ACC_MOVEMENTS"""
    
    print("💰 PASO 1: BUSCANDO INGRESOS EN BT_MP_ACC_MOVEMENTS")
    print("📋 Cuando ENTRA dinero a las cuentas de los usuarios")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    usuarios_str = "(" + ",".join(map(str, USUARIOS_OBJETIVO)) + ")"
    
    # Buscar ingresos (movimientos positivos) en ACC_MOVEMENTS
    query_ingresos = f"""
    SELECT 
        ACC_CUST_ID as user_id,
        ACC_ID as movement_id,
        ACC_CREATED_DATE as fecha_ingreso,
        ACC_TOTAL_AMOUNT as monto_ingreso,
        ACC_OPERATION_TYPE as tipo_operacion,
        ACC_STATUS as estado,
        ACC_CURRENCY_ID as moneda,
        
        -- Clasificar tipo de ingreso
        CASE 
            WHEN ACC_OPERATION_TYPE IN ('DEPOSIT', 'TRANSFER_IN', 'CREDIT', 'FUND') THEN 'INGRESO_DIRECTO'
            WHEN ACC_OPERATION_TYPE LIKE '%TRANSFER%' AND ACC_TOTAL_AMOUNT > 0 THEN 'TRANSFERENCIA_RECIBIDA'
            WHEN ACC_TOTAL_AMOUNT > 0 THEN 'INGRESO_OTRO'
            ELSE 'NO_INGRESO'
        END as tipo_ingreso
        
    FROM 
        `meli-bi-data.WHOWNER.BT_MP_ACC_MOVEMENTS`
    WHERE 
        ACC_CUST_ID IN {usuarios_str}
        AND ACC_TOTAL_AMOUNT > 0
        AND ACC_STATUS = 'APPROVED'
        
    ORDER BY 
        ACC_CUST_ID, ACC_CREATED_DATE DESC
    LIMIT 100
    """
    
    print("📊 Ejecutando búsqueda de ingresos...")
    result = await operations.execute_query(query_ingresos, 100)
    
    if result["status"] == "success" and result["result"]["rows"]:
        ingresos_df = pd.DataFrame(result["result"]["rows"])
        
        print(f"🎯 ¡INGRESOS ENCONTRADOS! {len(ingresos_df)} movimientos")
        
        # Resumen por usuario
        print("\n📈 RESUMEN DE INGRESOS:")
        for user_id in ingresos_df['user_id'].unique():
            user_data = ingresos_df[ingresos_df['user_id'] == user_id]
            total_ops = len(user_data)
            total_monto = user_data['monto_ingreso'].sum()
            fecha_primera = user_data['fecha_ingreso'].min()
            fecha_ultima = user_data['fecha_ingreso'].max()
            
            print(f"\n   👤 Usuario {user_id}:")
            print(f"      💰 Ingresos: {total_ops} operaciones")
            print(f"      💰 Monto total: ${total_monto:,.2f}")
            print(f"      📅 Período: {fecha_primera} → {fecha_ultima}")
            
            # Tipos de ingreso
            tipos = user_data['tipo_ingreso'].value_counts()
            print(f"      🔍 Tipos: {dict(tipos)}")
            
            # Mostrar primeros ingresos
            print(f"      🔍 Primeros ingresos:")
            for _, ingreso in user_data.head(3).iterrows():
                fecha = ingreso['fecha_ingreso']
                monto = ingreso['monto_ingreso']
                tipo = ingreso['tipo_operacion']
                print(f"         {fecha}: ${monto:.2f} ({tipo})")
        
        return ingresos_df
    else:
        print("❌ No se encontraron ingresos en BT_MP_ACC_MOVEMENTS")
        return None

async def buscar_retiros_en_todas_tablas(ingresos_df):
    """PASO 2: Buscar RETIROS en todas las tablas disponibles"""
    
    print("\n💸 PASO 2: BUSCANDO RETIROS EN TODAS LAS TABLAS")
    print("📋 Payments, Payouts y cualquier salida de dinero")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    resultados_velocidad = []
    
    for user_id in ingresos_df['user_id'].unique():
        user_ingresos = ingresos_df[ingresos_df['user_id'] == user_id]
        
        print(f"\n👤 Usuario {user_id} - Buscando retiros después de {len(user_ingresos)} ingresos")
        
        # Para cada ingreso, buscar retiros posteriores
        for _, ingreso in user_ingresos.head(10).iterrows():  # Límite para no saturar
            fecha_ingreso = ingreso['fecha_ingreso']
            monto_ingreso = ingreso['monto_ingreso']
            movement_id_ingreso = ingreso['movement_id']
            
            # Buscar retiros en período de 30 días
            fecha_limite = pd.to_datetime(fecha_ingreso) + timedelta(days=30)
            
            print(f"   🔍 Ingreso ${monto_ingreso:.2f} el {fecha_ingreso}")
            
            # TABLA 1: BT_MP_PAYOUTS (retiros)
            retiros_payouts = await buscar_retiros_payouts(user_id, fecha_ingreso, fecha_limite, operations)
            
            # TABLA 2: BT_MP_PAY_PAYMENTS (pagos)
            retiros_payments = await buscar_retiros_payments(user_id, fecha_ingreso, fecha_limite, operations)
            
            # TABLA 3: Más movimientos en ACC_MOVEMENTS (egresos)
            retiros_acc = await buscar_egresos_acc_movements(user_id, fecha_ingreso, fecha_limite, movement_id_ingreso, operations)
            
            # Combinar todos los retiros
            todos_retiros = []
            todos_retiros.extend(retiros_payouts)
            todos_retiros.extend(retiros_payments) 
            todos_retiros.extend(retiros_acc)
            
            if todos_retiros:
                # Ordenar por fecha y tomar el primero
                todos_retiros.sort(key=lambda x: x['fecha_retiro'])
                primer_retiro = todos_retiros[0]
                
                # Calcular velocidad
                fecha_retiro = pd.to_datetime(primer_retiro['fecha_retiro'])
                fecha_ing_dt = pd.to_datetime(fecha_ingreso)
                velocidad_horas = (fecha_retiro - fecha_ing_dt).total_seconds() / 3600
                
                resultados_velocidad.append({
                    'user_id': user_id,
                    'movement_id_ingreso': movement_id_ingreso,
                    'operation_id_retiro': primer_retiro['operation_id'],
                    'fecha_ingreso': fecha_ing_dt,
                    'fecha_retiro': fecha_retiro,
                    'monto_ingreso': monto_ingreso,
                    'monto_retiro': primer_retiro['monto_retiro'],
                    'velocidad_horas': velocidad_horas,
                    'velocidad_dias': velocidad_horas / 24,
                    'tipo_ingreso': ingreso['tipo_operacion'],
                    'tipo_retiro': primer_retiro['tipo_retiro'],
                    'fuente_retiro': primer_retiro['fuente'],
                    'tabla_retiro': primer_retiro['tabla']
                })
                
                # Mostrar resultado inmediato
                if velocidad_horas < 1:
                    tiempo_str = f"{velocidad_horas*60:.0f} minutos"
                elif velocidad_horas < 24:
                    tiempo_str = f"{velocidad_horas:.1f} horas"
                else:
                    tiempo_str = f"{velocidad_horas/24:.1f} días"
                
                print(f"      ✅ Retiro encontrado: {tiempo_str} después")
                print(f"         ${monto_ingreso:.2f} → ${primer_retiro['monto_retiro']:.2f} ({primer_retiro['tabla']})")
                
                if velocidad_horas < 24:
                    print(f"         🚨 ALERTA: Retiro rápido!")
            else:
                print(f"      ❌ No se encontraron retiros en 30 días")
    
    return resultados_velocidad

async def buscar_retiros_payouts(user_id, fecha_ingreso, fecha_limite, operations):
    """Buscar retiros en BT_MP_PAYOUTS"""
    
    query_payouts = f"""
    SELECT 
        PAYOUT_ID as operation_id,
        PAYOUT_DATE_CREATED as fecha_retiro,
        PAYOUT_AMT as monto_retiro,
        PAYOUT_TYPE as tipo_retiro
    FROM 
        `meli-bi-data.WHOWNER.BT_MP_PAYOUTS`
    WHERE 
        PAYOUT_CUST_ID = {user_id}
        AND PAYOUT_DATE_CREATED > '{fecha_ingreso}'
        AND PAYOUT_DATE_CREATED <= '{fecha_limite}'
        AND PAYOUT_STATUS = 'APPROVED'
        AND PAYOUT_AMT > 0
        
    ORDER BY PAYOUT_DATE_CREATED ASC
    LIMIT 10
    """
    
    try:
        result = await operations.execute_query(query_payouts, 15)
        
        if result["status"] == "success" and result["result"]["rows"]:
            retiros = []
            for row in result["result"]["rows"]:
                retiros.append({
                    'operation_id': row['operation_id'],
                    'fecha_retiro': row['fecha_retiro'],
                    'monto_retiro': row['monto_retiro'],
                    'tipo_retiro': row['tipo_retiro'],
                    'fuente': 'PAYOUT',
                    'tabla': 'BT_MP_PAYOUTS'
                })
            return retiros
    except Exception as e:
        print(f"      ⚠️  Error en PAYOUTS: {str(e)[:50]}...")
    
    return []

async def buscar_retiros_payments(user_id, fecha_ingreso, fecha_limite, operations):
    """Buscar retiros en BT_MP_PAY_PAYMENTS"""
    
    query_payments = f"""
    SELECT 
        PAY_PAYMENT_ID as operation_id,
        pay_move_date as fecha_retiro,
        ABS(PAY_TRANSACTION_DOL_AMT) as monto_retiro,
        PAY_OPERATION_TYPE_ID as tipo_retiro
    FROM 
        `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
    WHERE 
        CUS_CUST_ID_SEL = {user_id}
        AND pay_move_date > '{fecha_ingreso}'
        AND pay_move_date <= '{fecha_limite}'
        AND pay_status_id = 'approved'
        AND tpv_flag = 1
        AND (
            PAY_OPERATION_TYPE_ID IN ('money_out', 'transfer_out', 'withdrawal', 'cashout')
            OR PAY_TRANSACTION_DOL_AMT < 0
        )
        
    ORDER BY pay_move_date ASC
    LIMIT 10
    """
    
    try:
        result = await operations.execute_query(query_payments, 15)
        
        if result["status"] == "success" and result["result"]["rows"]:
            retiros = []
            for row in result["result"]["rows"]:
                retiros.append({
                    'operation_id': row['operation_id'],
                    'fecha_retiro': row['fecha_retiro'],
                    'monto_retiro': row['monto_retiro'],
                    'tipo_retiro': row['tipo_retiro'],
                    'fuente': 'PAYMENT',
                    'tabla': 'BT_MP_PAY_PAYMENTS'
                })
            return retiros
    except Exception as e:
        print(f"      ⚠️  Error en PAYMENTS: {str(e)[:50]}...")
    
    return []

async def buscar_egresos_acc_movements(user_id, fecha_ingreso, fecha_limite, movement_id_ingreso, operations):
    """Buscar egresos en la misma tabla ACC_MOVEMENTS"""
    
    query_egresos = f"""
    SELECT 
        ACC_ID as operation_id,
        ACC_CREATED_DATE as fecha_retiro,
        ABS(ACC_TOTAL_AMOUNT) as monto_retiro,
        ACC_OPERATION_TYPE as tipo_retiro
    FROM 
        `meli-bi-data.WHOWNER.BT_MP_ACC_MOVEMENTS`
    WHERE 
        ACC_CUST_ID = {user_id}
        AND ACC_CREATED_DATE > '{fecha_ingreso}'
        AND ACC_CREATED_DATE <= '{fecha_limite}'
        AND ACC_ID != '{movement_id_ingreso}'
        AND ACC_STATUS = 'APPROVED'
        AND ACC_TOTAL_AMOUNT < 0
        
    ORDER BY ACC_CREATED_DATE ASC
    LIMIT 10
    """
    
    try:
        result = await operations.execute_query(query_egresos, 15)
        
        if result["status"] == "success" and result["result"]["rows"]:
            retiros = []
            for row in result["result"]["rows"]:
                retiros.append({
                    'operation_id': row['operation_id'],
                    'fecha_retiro': row['fecha_retiro'],
                    'monto_retiro': row['monto_retiro'],
                    'tipo_retiro': row['tipo_retiro'],
                    'fuente': 'EGRESO',
                    'tabla': 'BT_MP_ACC_MOVEMENTS'
                })
            return retiros
    except Exception as e:
        print(f"      ⚠️  Error en ACC_MOVEMENTS egresos: {str(e)[:50]}...")
    
    return []

async def generar_reporte_velocidades_final(resultados_velocidad):
    """PASO 3: Análisis final y reporte"""
    
    print("\n⚡ PASO 3: ANÁLISIS FINAL DE VELOCIDADES")
    print("🎯 Velocidad real: Ingreso → Retiro (cualquier tabla)")
    print("=" * 80)
    
    if not resultados_velocidad:
        print("❌ No se encontraron pares ingreso→retiro para calcular velocidades")
        return
    
    df_velocidades = pd.DataFrame(resultados_velocidad)
    
    print("📊 ESTADÍSTICAS GENERALES:")
    print(f"   👥 Usuarios con actividad: {df_velocidades['user_id'].nunique()}")
    print(f"   🔄 Pares ingreso→retiro: {len(df_velocidades)}")
    print(f"   ⚡ Velocidad promedio: {df_velocidades['velocidad_horas'].mean():.1f} horas ({df_velocidades['velocidad_horas'].mean()/24:.1f} días)")
    print(f"   📊 Velocidad mediana: {df_velocidades['velocidad_horas'].median():.1f} horas ({df_velocidades['velocidad_horas'].median()/24:.1f} días)")
    print(f"   🔥 Velocidad mínima: {df_velocidades['velocidad_horas'].min():.1f} horas")
    print(f"   🐌 Velocidad máxima: {df_velocidades['velocidad_horas'].max():.1f} horas")
    
    # Alertas críticas
    alertas_1h = df_velocidades[df_velocidades['velocidad_horas'] < 1]
    alertas_24h = df_velocidades[df_velocidades['velocidad_horas'] < 24]
    alertas_1sem = df_velocidades[df_velocidades['velocidad_horas'] < 168]  # 7 días
    
    print(f"\n🚨 ALERTAS CRÍTICAS:")
    print(f"   🔥 Retiros < 1 hora: {len(alertas_1h)} casos")
    print(f"   ⚠️  Retiros < 24 horas: {len(alertas_24h)} casos")
    print(f"   📊 Retiros < 1 semana: {len(alertas_1sem)} casos")
    
    # Análisis por tabla de retiro
    print(f"\n📋 ANÁLISIS POR TABLA DE RETIRO:")
    tablas_retiro = df_velocidades['tabla_retiro'].value_counts()
    for tabla, count in tablas_retiro.items():
        tabla_data = df_velocidades[df_velocidades['tabla_retiro'] == tabla]
        vel_promedio = tabla_data['velocidad_horas'].mean()
        print(f"   🔧 {tabla}: {count} retiros (⚡ {vel_promedio:.1f}h promedio)")
    
    # Análisis por usuario
    print(f"\n👥 ANÁLISIS DETALLADO POR USUARIO:")
    for user_id in df_velocidades['user_id'].unique():
        user_data = df_velocidades[df_velocidades['user_id'] == user_id]
        
        print(f"\n   👤 Usuario {user_id}:")
        print(f"      🔄 Operaciones: {len(user_data)}")
        print(f"      ⚡ Velocidad promedio: {user_data['velocidad_horas'].mean():.1f}h ({user_data['velocidad_horas'].mean()/24:.1f} días)")
        print(f"      🔥 Velocidad mínima: {user_data['velocidad_horas'].min():.1f}h")
        print(f"      💰 Monto promedio ingreso: ${user_data['monto_ingreso'].mean():.2f}")
        print(f"      💸 Monto promedio retiro: ${user_data['monto_retiro'].mean():.2f}")
        
        # Casos más rápidos del usuario
        user_rapidos = user_data.nsmallest(3, 'velocidad_horas')
        print(f"      🔥 Top 3 más rápidos:")
        for _, caso in user_rapidos.iterrows():
            horas = caso['velocidad_horas']
            if horas < 1:
                tiempo_str = f"{horas*60:.0f} minutos"
            else:
                tiempo_str = f"{horas:.1f} horas"
            tabla = caso['tabla_retiro']
            print(f"         • {tiempo_str}: ${caso['monto_ingreso']:.2f} → ${caso['monto_retiro']:.2f} ({tabla})")
        
        # Tablas más usadas por usuario
        user_tablas = user_data['tabla_retiro'].value_counts()
        print(f"      📋 Tablas usadas: {dict(user_tablas)}")
    
    # Exportar resultados
    fecha_actual = datetime.now().strftime('%Y%m%d_%H%M')
    archivo_excel = f"VELOCIDAD_INGRESO_RETIRO_REAL_{fecha_actual}.xlsx"
    
    try:
        with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
            # Hoja principal
            df_velocidades.to_excel(writer, sheet_name='Velocidades_Detalle', index=False)
            
            # Alertas críticas
            if len(alertas_24h) > 0:
                alertas_24h.to_excel(writer, sheet_name='Alertas_24h', index=False)
            
            if len(alertas_1h) > 0:
                alertas_1h.to_excel(writer, sheet_name='Alertas_1h', index=False)
            
            # Resumen por usuario
            resumen = df_velocidades.groupby('user_id').agg({
                'velocidad_horas': ['count', 'mean', 'median', 'min', 'max'],
                'monto_ingreso': 'mean',
                'monto_retiro': 'mean'
            }).round(2)
            
            resumen.columns = ['operaciones', 'vel_prom_h', 'vel_med_h', 'vel_min_h', 'vel_max_h', 'monto_prom_ing', 'monto_prom_ret']
            resumen.to_excel(writer, sheet_name='Resumen_Usuario')
        
        print(f"\n✅ Reporte exportado: {archivo_excel}")
        
    except Exception as e:
        print(f"❌ Error exportando: {e}")

async def main():
    """Función principal"""
    
    print("🎯 MÉTRICA REAL DE VELOCIDAD: INGRESO → RETIRO")
    print("👥 Usuarios: " + str(USUARIOS_OBJETIVO))
    print("💰 Ingreso: BT_MP_ACC_MOVEMENTS (cuando entra dinero)")
    print("💸 Retiro: Todas las tablas (cuando sale dinero)")
    print("=" * 80)
    
    try:
        # 1. Buscar ingresos en ACC_MOVEMENTS
        ingresos_df = await buscar_ingresos_acc_movements()
        
        if ingresos_df is not None and len(ingresos_df) > 0:
            # 2. Buscar retiros en todas las tablas
            resultados_velocidad = await buscar_retiros_en_todas_tablas(ingresos_df)
            
            # 3. Análisis final y reporte
            await generar_reporte_velocidades_final(resultados_velocidad)
        else:
            print("❌ No se pueden calcular velocidades sin ingresos")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 