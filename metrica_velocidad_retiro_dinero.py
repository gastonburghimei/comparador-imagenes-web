#!/usr/bin/env python3
"""
MÉTRICA CRÍTICA: Velocidad de retiro de dinero
Usuarios específicos: 1348718991, 468290404, 375845668

Analizar:
1. Fecha de infracción por CUENTA DE HACKER
2. Movimientos de dinero ANTERIORES a la infracción
3. Velocidad entre ingreso y retiro de fondos

Tablas clave:
- meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW (infracciones)
- meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS (pagos/transacciones)
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from mcp_bigquery_setup import MCPBigQueryBasicOperations

# Usuarios específicos a analizar
USUARIOS_OBJETIVO = [1348718991, 468290404, 375845668]

async def obtener_fechas_infracciones():
    """
    Paso 1: Obtener las fechas de infracción por CUENTA DE HACKER de los usuarios objetivo
    """
    
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
        
        # Mostrar resumen
        for _, row in infracciones_df.iterrows():
            user_id = row['USER_ID']
            fecha_inf = row['fecha_infraccion']
            sentence_id = row['SENTENCE_ID']
            status = row['SENTENCE_LAST_STATUS']
            print(f"   👤 Usuario {user_id}: Infracción {sentence_id} el {fecha_inf} ({status})")
        
        return infracciones_df
    else:
        print("❌ No se encontraron infracciones para estos usuarios")
        return None

async def analizar_movimientos_dinero(infracciones_df):
    """
    Paso 2: Analizar movimientos de dinero ANTES de las infracciones
    """
    
    print("\n🔍 STEP 2: Analizando movimientos de dinero PRE-INFRACCIÓN")
    print("💰 Buscando ingresos y retiros de fondos")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # Primero explorar la estructura de la tabla de pagos
    print("📋 Explorando estructura de BT_MP_PAY_PAYMENTS...")
    
    query_schema = """
    SELECT 
        column_name,
        data_type,
        description
    FROM 
        `meli-bi-data.WHOWNER.INFORMATION_SCHEMA.COLUMNS`
    WHERE 
        table_name = 'BT_MP_PAY_PAYMENTS'
        AND column_name IN (
            'PAY_PAYMENT_ID', 'PAY_CUST_ID', 'PAY_TRANSACTION_DOL_AMT',
            'PAY_TOTAL_PAID_DOL_AMT', 'PAY_STATUS_ID', 'PAY_OPERATION_TYPE_ID',
            'PAY_PM_TYPE_ID', 'PAY_CREATION_DATE', 'PAY_APPROVED_DATE'
        )
    ORDER BY 
        ordinal_position
    """
    
    schema_result = await operations.execute_query(query_schema, 20)
    
    if schema_result["status"] == "success" and schema_result["result"]["rows"]:
        print("✅ Columnas clave encontradas:")
        for row in schema_result["result"]["rows"]:
            col = row['column_name']
            tipo = row['data_type']
            desc = row.get('description', 'N/A')
            print(f"   📊 {col} ({tipo}): {desc}")
    
    # Analizar movimientos para cada usuario
    resultados_usuarios = []
    
    for _, infraccion in infracciones_df.iterrows():
        user_id = infraccion['USER_ID']
        fecha_infraccion = infraccion['fecha_infraccion']
        sentence_id = infraccion['SENTENCE_ID']
        
        print(f"\n👤 Analizando Usuario {user_id} (Infracción: {fecha_infraccion})")
        
        # Buscar movimientos 30 días antes de la infracción
        fecha_limite = pd.to_datetime(fecha_infraccion) - timedelta(days=30)
        fecha_limite_str = fecha_limite.strftime('%Y-%m-%d')
        
        query_movimientos = f"""
        SELECT 
            PAY_PAYMENT_ID,
            PAY_CUST_ID as user_id,
            PAY_CREATION_DATE as fecha_creacion,
            PAY_APPROVED_DATE as fecha_aprobacion,
            PAY_STATUS_ID as estado,
            PAY_OPERATION_TYPE_ID as tipo_operacion,
            PAY_PM_TYPE_ID as metodo_pago,
            PAY_TRANSACTION_DOL_AMT as monto_transaccion_usd,
            PAY_TOTAL_PAID_DOL_AMT as monto_total_usd,
            
            -- Clasificar tipo de movimiento
            CASE 
                WHEN PAY_OPERATION_TYPE_ID IN ('account_fund', 'transfer', 'money_in') THEN 'INGRESO'
                WHEN PAY_OPERATION_TYPE_ID IN ('money_out', 'withdrawal', 'transfer_out', 'cashout') THEN 'RETIRO'
                WHEN PAY_OPERATION_TYPE_ID LIKE '%money_exchange%' THEN 'INTERCAMBIO'
                ELSE 'OTRO'
            END as tipo_movimiento
            
        FROM 
            `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
        WHERE 
            PAY_CUST_ID = {user_id}
            AND PAY_CREATION_DATE >= '{fecha_limite_str}'
            AND PAY_CREATION_DATE < '{fecha_infraccion}'
            AND PAY_STATUS_ID = 'approved'
            AND PAY_TRANSACTION_DOL_AMT > 0
            
        ORDER BY 
            PAY_CREATION_DATE ASC
        """
        
        mov_result = await operations.execute_query(query_movimientos, 100)
        
        if mov_result["status"] == "success" and mov_result["result"]["rows"]:
            movimientos = pd.DataFrame(mov_result["result"]["rows"])
            
            print(f"   💰 {len(movimientos)} movimientos encontrados en 30 días previos")
            
            # Análisis de velocidad
            ingresos = movimientos[movimientos['tipo_movimiento'] == 'INGRESO']
            retiros = movimientos[movimientos['tipo_movimiento'] == 'RETIRO']
            
            print(f"   📈 Ingresos: {len(ingresos)} ({ingresos['monto_transaccion_usd'].sum():.2f} USD)")
            print(f"   📉 Retiros: {len(retiros)} ({retiros['monto_transaccion_usd'].sum():.2f} USD)")
            
            # Calcular velocidad promedio
            if len(ingresos) > 0 and len(retiros) > 0:
                # Para cada ingreso, buscar el primer retiro posterior
                velocidades = []
                
                for _, ingreso in ingresos.iterrows():
                    fecha_ingreso = pd.to_datetime(ingreso['fecha_creacion'])
                    retiros_posteriores = retiros[pd.to_datetime(retiros['fecha_creacion']) > fecha_ingreso]
                    
                    if len(retiros_posteriores) > 0:
                        primer_retiro = retiros_posteriores.iloc[0]
                        fecha_retiro = pd.to_datetime(primer_retiro['fecha_creacion'])
                        
                        velocidad_horas = (fecha_retiro - fecha_ingreso).total_seconds() / 3600
                        velocidades.append({
                            'user_id': user_id,
                            'sentence_id': sentence_id,
                            'fecha_ingreso': fecha_ingreso,
                            'fecha_retiro': fecha_retiro,
                            'monto_ingreso': ingreso['monto_transaccion_usd'],
                            'monto_retiro': primer_retiro['monto_transaccion_usd'],
                            'velocidad_horas': velocidad_horas,
                            'velocidad_dias': velocidad_horas / 24,
                            'tipo_ingreso': ingreso['tipo_operacion'],
                            'tipo_retiro': primer_retiro['tipo_operacion']
                        })
                
                if velocidades:
                    velocidad_promedio = sum(v['velocidad_horas'] for v in velocidades) / len(velocidades)
                    print(f"   ⚡ Velocidad promedio: {velocidad_promedio:.1f} horas ({velocidad_promedio/24:.1f} días)")
                    print(f"   🎯 {len(velocidades)} pares ingreso→retiro identificados")
                    
                    resultados_usuarios.extend(velocidades)
                else:
                    print("   ⚠️  No se pudieron emparejar ingresos con retiros")
            else:
                print("   ⚠️  Insuficientes movimientos para calcular velocidad")
        else:
            print(f"   ❌ No se encontraron movimientos para usuario {user_id}")
    
    return resultados_usuarios

async def generar_reporte_final(resultados_usuarios):
    """
    Paso 3: Generar reporte final con métricas
    """
    
    print("\n📊 STEP 3: REPORTE FINAL - VELOCIDAD DE RETIRO")
    print("🎯 Métrica crítica para detección de patrones ATO")
    print("=" * 80)
    
    if not resultados_usuarios:
        print("❌ No se pudieron calcular velocidades para ningún usuario")
        return
    
    # Convertir a DataFrame
    df_velocidades = pd.DataFrame(resultados_usuarios)
    
    # Estadísticas generales
    print("📈 ESTADÍSTICAS GENERALES:")
    print(f"   👥 Usuarios analizados: {df_velocidades['user_id'].nunique()}")
    print(f"   🔄 Operaciones analizadas: {len(df_velocidades)}")
    print(f"   ⚡ Velocidad promedio: {df_velocidades['velocidad_horas'].mean():.1f} horas")
    print(f"   📊 Velocidad mediana: {df_velocidades['velocidad_horas'].median():.1f} horas")
    print(f"   🔥 Velocidad mínima: {df_velocidades['velocidad_horas'].min():.1f} horas")
    print(f"   🐌 Velocidad máxima: {df_velocidades['velocidad_horas'].max():.1f} horas")
    
    # Análisis por usuario
    print("\n👥 ANÁLISIS POR USUARIO:")
    for user_id in df_velocidades['user_id'].unique():
        user_data = df_velocidades[df_velocidades['user_id'] == user_id]
        velocidad_promedio_user = user_data['velocidad_horas'].mean()
        
        print(f"\n   👤 Usuario {user_id}:")
        print(f"      🔄 Operaciones: {len(user_data)}")
        print(f"      ⚡ Velocidad promedio: {velocidad_promedio_user:.1f} horas ({velocidad_promedio_user/24:.1f} días)")
        print(f"      💰 Monto promedio ingreso: ${user_data['monto_ingreso'].mean():.2f}")
        print(f"      💸 Monto promedio retiro: ${user_data['monto_retiro'].mean():.2f}")
        
        # Mostrar operaciones más rápidas
        operaciones_rapidas = user_data[user_data['velocidad_horas'] < 24].sort_values('velocidad_horas')
        if len(operaciones_rapidas) > 0:
            print(f"      🔥 Operaciones < 24h: {len(operaciones_rapidas)}")
            for _, op in operaciones_rapidas.head(3).iterrows():
                print(f"         • {op['velocidad_horas']:.1f}h: ${op['monto_ingreso']:.2f} → ${op['monto_retiro']:.2f}")
    
    # Exportar a Excel
    fecha_actual = datetime.now().strftime('%Y%m%d_%H%M')
    archivo_excel = f"velocidad_retiro_usuarios_CUENTA_HACKER_{fecha_actual}.xlsx"
    
    try:
        with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
            # Hoja principal con todos los datos
            df_velocidades.to_excel(writer, sheet_name='Velocidades_Detalle', index=False)
            
            # Hoja resumen por usuario
            resumen = df_velocidades.groupby('user_id').agg({
                'velocidad_horas': ['count', 'mean', 'median', 'min', 'max'],
                'monto_ingreso': 'mean',
                'monto_retiro': 'mean'
            }).round(2)
            
            resumen.columns = ['operaciones', 'vel_promedio_h', 'vel_mediana_h', 'vel_min_h', 'vel_max_h', 'monto_prom_ingreso', 'monto_prom_retiro']
            resumen.to_excel(writer, sheet_name='Resumen_Por_Usuario')
        
        print(f"\n✅ Reporte exportado: {archivo_excel}")
        
    except Exception as e:
        print(f"❌ Error exportando a Excel: {e}")

async def main():
    """Función principal"""
    
    print("🚨 MÉTRICA CRÍTICA: VELOCIDAD DE RETIRO DE DINERO")
    print("🎯 Usuarios sospechosos de CUENTA DE HACKER")
    print("📊 Análisis pre-infracción para detectar patrones ATO")
    print("=" * 80)
    
    try:
        # Paso 1: Obtener fechas de infracciones
        infracciones_df = await obtener_fechas_infracciones()
        
        if infracciones_df is not None and len(infracciones_df) > 0:
            # Paso 2: Analizar movimientos de dinero
            resultados = await analizar_movimientos_dinero(infracciones_df)
            
            # Paso 3: Generar reporte final
            await generar_reporte_final(resultados)
        else:
            print("❌ No se pueden continuar sin fechas de infracciones")
            
    except Exception as e:
        print(f"❌ Error en análisis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 