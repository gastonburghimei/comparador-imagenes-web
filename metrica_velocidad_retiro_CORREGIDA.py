#!/usr/bin/env python3
"""
MÉTRICA CORREGIDA: Velocidad de retiro de dinero
Usuarios: 1348718991, 468290404, 375845668

ESTRUCTURA CORREGIDA basada en archivos SQL existentes:
- Columna: CUS_CUST_ID_SEL (no PAY_CUST_ID)
- Fecha: pay_move_date (no PAY_CREATION_DATE)
- Filtros: pay_status_id = 'approved' AND tpv_flag = 1
- Período: 90 días antes de la infracción
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from mcp_bigquery_setup import MCPBigQueryBasicOperations

# Usuarios objetivo
USUARIOS_OBJETIVO = [1348718991, 468290404, 375845668]

async def obtener_fechas_infracciones_corregida():
    """Obtener fechas de infracciones (igual que antes pero confirmando)"""
    
    print("🔍 STEP 1: Obteniendo fechas de infracciones CUENTA DE HACKER")
    print(f"👥 Usuarios objetivo: {USUARIOS_OBJETIVO}")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    usuarios_str = "(" + ",".join(map(str, USUARIOS_OBJETIVO)) + ")"
    
    query_infracciones = f"""
    SELECT 
        USER_ID,
        SENTENCE_ID,
        INFRACTION_TYPE,
        SENTENCE_DATE as fecha_infraccion,
        SENTENCE_LAST_STATUS,
        COLOR_DE_TARJETA,
        SIT_SITE_ID
    FROM 
        `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW`
    WHERE 
        USER_ID IN {usuarios_str}
        AND INFRACTION_TYPE = 'CUENTA_DE_HACKER'
        AND SENTENCE_LAST_STATUS IN ('ACTIVE', 'ROLLBACKED', 'REINSTATED')
    ORDER BY 
        USER_ID, SENTENCE_DATE DESC
    """
    
    print("📊 Ejecutando query de infracciones...")
    result = await operations.execute_query(query_infracciones, 50)
    
    if result["status"] == "success" and result["result"]["rows"]:
        print(f"✅ {len(result['result']['rows'])} infracciones encontradas:")
        
        infracciones_df = pd.DataFrame(result["result"]["rows"])
        
        for _, row in infracciones_df.iterrows():
            user_id = row['USER_ID']
            fecha_inf = row['fecha_infraccion']
            sentence_id = row['SENTENCE_ID']
            status = row['SENTENCE_LAST_STATUS']
            print(f"   👤 Usuario {user_id}: Infracción {sentence_id} el {fecha_inf} ({status})")
        
        return infracciones_df
    else:
        print("❌ No se encontraron infracciones")
        return None

async def analizar_movimientos_corregido(infracciones_df):
    """Analizar movimientos usando la estructura CORRECTA"""
    
    print("\n🔍 STEP 2: Analizando movimientos con ESTRUCTURA CORREGIDA")
    print("💰 Usando: CUS_CUST_ID_SEL + pay_move_date + tpv_flag")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    resultados_usuarios = []
    
    for _, infraccion in infracciones_df.iterrows():
        user_id = infraccion['USER_ID']
        fecha_infraccion = infraccion['fecha_infraccion']
        sentence_id = infraccion['SENTENCE_ID']
        
        print(f"\n👤 Usuario {user_id} (Infracción: {fecha_infraccion})")
        
        # Query corregida usando la estructura real
        query_movimientos = f"""
        SELECT 
            PAY_PAYMENT_ID,
            CUS_CUST_ID_SEL as user_id,
            pay_move_date as fecha_movimiento,
            PAY_CREATION_DATE as fecha_creacion,
            pay_status_id as estado,
            PAY_OPERATION_TYPE_ID as tipo_operacion,
            PAY_PM_TYPE_ID as metodo_pago,
            PAY_TRANSACTION_DOL_AMT as monto_usd,
            tpv_flag,
            
            -- Clasificar movimientos según patrones reales
            CASE 
                WHEN PAY_OPERATION_TYPE_ID IN ('account_fund', 'money_in', 'deposit') THEN 'INGRESO'
                WHEN PAY_OPERATION_TYPE_ID IN ('money_out', 'withdrawal', 'cashout') THEN 'RETIRO'
                WHEN PAY_OPERATION_TYPE_ID LIKE '%transfer%' AND PAY_TRANSACTION_DOL_AMT > 0 THEN 'TRANSFERENCIA_IN'
                WHEN PAY_OPERATION_TYPE_ID LIKE '%transfer%' AND PAY_TRANSACTION_DOL_AMT < 0 THEN 'TRANSFERENCIA_OUT'
                WHEN PAY_OPERATION_TYPE_ID LIKE '%exchange%' THEN 'INTERCAMBIO'
                ELSE 'OTRO'
            END as tipo_movimiento
            
        FROM 
            `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
        WHERE 
            CUS_CUST_ID_SEL = {user_id}
            AND pay_move_date >= DATE_SUB(DATE('{fecha_infraccion}'), INTERVAL 90 DAY)
            AND pay_move_date <= DATE('{fecha_infraccion}')
            AND pay_status_id = 'approved'
            AND tpv_flag = 1
            AND PAY_TRANSACTION_DOL_AMT != 0
            
        ORDER BY 
            pay_move_date ASC, PAY_CREATION_DATE ASC
        """
        
        print("   📊 Ejecutando query corregida...")
        result = await operations.execute_query(query_movimientos, 100)
        
        if result["status"] == "success" and result["result"]["rows"]:
            movimientos = pd.DataFrame(result["result"]["rows"])
            
            print(f"   ✅ {len(movimientos)} movimientos encontrados (90 días pre-infracción)")
            
            # Análisis detallado
            print("   📈 Resumen por tipo de movimiento:")
            resumen = movimientos.groupby('tipo_movimiento').agg({
                'monto_usd': ['count', 'sum', 'mean']
            }).round(2)
            
            for tipo in resumen.index:
                count = resumen.loc[tipo, ('monto_usd', 'count')]
                suma = resumen.loc[tipo, ('monto_usd', 'sum')]
                promedio = resumen.loc[tipo, ('monto_usd', 'mean')]
                print(f"      {tipo}: {count:,} ops, ${suma:,.2f} total, ${promedio:.2f} promedio")
            
            # Calcular velocidades de retiro
            ingresos = movimientos[movimientos['tipo_movimiento'].isin(['INGRESO', 'TRANSFERENCIA_IN'])]
            retiros = movimientos[movimientos['tipo_movimiento'].isin(['RETIRO', 'TRANSFERENCIA_OUT'])]
            
            print(f"   🎯 Análisis de velocidad:")
            print(f"      📈 Ingresos: {len(ingresos)} operaciones")
            print(f"      📉 Retiros: {len(retiros)} operaciones")
            
            if len(ingresos) > 0 and len(retiros) > 0:
                # Calcular velocidades
                velocidades = []
                
                for _, ingreso in ingresos.iterrows():
                    fecha_ingreso = pd.to_datetime(ingreso['fecha_movimiento'])
                    monto_ingreso = abs(ingreso['monto_usd'])
                    
                    # Buscar retiros posteriores
                    retiros_posteriores = retiros[
                        pd.to_datetime(retiros['fecha_movimiento']) >= fecha_ingreso
                    ].sort_values('fecha_movimiento')
                    
                    if len(retiros_posteriores) > 0:
                        primer_retiro = retiros_posteriores.iloc[0]
                        fecha_retiro = pd.to_datetime(primer_retiro['fecha_movimiento'])
                        monto_retiro = abs(primer_retiro['monto_usd'])
                        
                        # Calcular velocidad
                        velocidad_horas = (fecha_retiro - fecha_ingreso).total_seconds() / 3600
                        
                        velocidades.append({
                            'user_id': user_id,
                            'sentence_id': sentence_id,
                            'fecha_ingreso': fecha_ingreso,
                            'fecha_retiro': fecha_retiro,
                            'monto_ingreso': monto_ingreso,
                            'monto_retiro': monto_retiro,
                            'velocidad_horas': velocidad_horas,
                            'velocidad_dias': velocidad_horas / 24,
                            'tipo_ingreso': ingreso['tipo_operacion'],
                            'tipo_retiro': primer_retiro['tipo_operacion'],
                            'payment_id_ingreso': ingreso['PAY_PAYMENT_ID'],
                            'payment_id_retiro': primer_retiro['PAY_PAYMENT_ID']
                        })
                
                if velocidades:
                    velocidad_promedio = sum(v['velocidad_horas'] for v in velocidades) / len(velocidades)
                    velocidad_minima = min(v['velocidad_horas'] for v in velocidades)
                    
                    print(f"      ⚡ {len(velocidades)} pares ingreso→retiro identificados")
                    print(f"      ⚡ Velocidad promedio: {velocidad_promedio:.1f} horas ({velocidad_promedio/24:.1f} días)")
                    print(f"      🔥 Velocidad mínima: {velocidad_minima:.1f} horas ({velocidad_minima/24:.1f} días)")
                    
                    # Mostrar las 3 operaciones más rápidas
                    velocidades_ordenadas = sorted(velocidades, key=lambda x: x['velocidad_horas'])[:3]
                    print(f"      🔥 Top 3 más rápidas:")
                    for i, v in enumerate(velocidades_ordenadas, 1):
                        print(f"         {i}. {v['velocidad_horas']:.1f}h: ${v['monto_ingreso']:.2f} → ${v['monto_retiro']:.2f}")
                    
                    resultados_usuarios.extend(velocidades)
                else:
                    print("      ⚠️  No se pudieron emparejar ingresos con retiros")
            else:
                print("      ⚠️  Insuficientes movimientos para calcular velocidad")
                
            # Mostrar algunos movimientos de ejemplo
            print("   🔍 Primeros 5 movimientos:")
            for _, mov in movimientos.head(5).iterrows():
                fecha = mov['fecha_movimiento']
                tipo = mov['tipo_movimiento']
                monto = mov['monto_usd']
                operacion = mov['tipo_operacion']
                print(f"      {fecha}: {tipo} ${monto:.2f} ({operacion})")
                
        else:
            print("   ❌ No se encontraron movimientos en el período")
    
    return resultados_usuarios

async def generar_reporte_velocidad_final(resultados_usuarios):
    """Generar reporte final completo"""
    
    print("\n📊 STEP 3: REPORTE FINAL - VELOCIDAD DE RETIRO CORREGIDA")
    print("🎯 Métrica crítica para patrones ATO/CUENTA DE HACKER")
    print("=" * 80)
    
    if not resultados_usuarios:
        print("❌ No se pudieron calcular velocidades")
        return
    
    df_velocidades = pd.DataFrame(resultados_usuarios)
    
    # Estadísticas generales
    print("📈 ESTADÍSTICAS GENERALES:")
    print(f"   👥 Usuarios analizados: {df_velocidades['user_id'].nunique()}")
    print(f"   🔄 Pares ingreso→retiro: {len(df_velocidades)}")
    print(f"   ⚡ Velocidad promedio: {df_velocidades['velocidad_horas'].mean():.1f} horas ({df_velocidades['velocidad_horas'].mean()/24:.1f} días)")
    print(f"   📊 Velocidad mediana: {df_velocidades['velocidad_horas'].median():.1f} horas ({df_velocidades['velocidad_horas'].median()/24:.1f} días)")
    print(f"   🔥 Velocidad mínima: {df_velocidades['velocidad_horas'].min():.1f} horas")
    print(f"   🐌 Velocidad máxima: {df_velocidades['velocidad_horas'].max():.1f} horas")
    
    # Análisis de alertas
    alertas_24h = df_velocidades[df_velocidades['velocidad_horas'] < 24]
    alertas_1h = df_velocidades[df_velocidades['velocidad_horas'] < 1]
    
    print(f"\n🚨 ALERTAS CRÍTICAS:")
    print(f"   ⚠️  Retiros < 24 horas: {len(alertas_24h)} casos")
    print(f"   🔥 Retiros < 1 hora: {len(alertas_1h)} casos")
    
    if len(alertas_1h) > 0:
        print(f"\n   🔥 CASOS ULTRA-RÁPIDOS (< 1 hora):")
        for _, caso in alertas_1h.head(5).iterrows():
            minutos = caso['velocidad_horas'] * 60
            print(f"      👤 Usuario {caso['user_id']}: {minutos:.0f} minutos (${caso['monto_ingreso']:.2f} → ${caso['monto_retiro']:.2f})")
    
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
        
        # Casos críticos del usuario
        user_24h = user_data[user_data['velocidad_horas'] < 24]
        if len(user_24h) > 0:
            print(f"      🚨 Casos < 24h: {len(user_24h)}")
    
    # Exportar resultados
    fecha_actual = datetime.now().strftime('%Y%m%d_%H%M')
    archivo_excel = f"VELOCIDAD_RETIRO_CUENTA_HACKER_CORREGIDA_{fecha_actual}.xlsx"
    
    try:
        with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
            # Hoja principal
            df_velocidades.to_excel(writer, sheet_name='Velocidades_Detalle', index=False)
            
            # Hoja de alertas críticas
            if len(alertas_24h) > 0:
                alertas_24h.to_excel(writer, sheet_name='Alertas_24h', index=False)
            
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
    """Función principal corregida"""
    
    print("🚨 MÉTRICA VELOCIDAD RETIRO - VERSIÓN CORREGIDA")
    print("🎯 Usuarios: " + str(USUARIOS_OBJETIVO))
    print("✅ Usando estructura real: CUS_CUST_ID_SEL + pay_move_date + tpv_flag")
    print("=" * 80)
    
    try:
        # Paso 1: Obtener infracciones
        infracciones_df = await obtener_fechas_infracciones_corregida()
        
        if infracciones_df is not None and len(infracciones_df) > 0:
            # Paso 2: Analizar movimientos con estructura corregida
            resultados = await analizar_movimientos_corregido(infracciones_df)
            
            # Paso 3: Generar reporte final
            await generar_reporte_velocidad_final(resultados)
        else:
            print("❌ No se pueden continuar sin infracciones")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 