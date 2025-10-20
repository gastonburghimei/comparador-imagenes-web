#!/usr/bin/env python3
"""
BÚSQUEDA DIRECTA EN ATO_BQ
Usuarios: 1348718991, 468290404, 375845668

Usar columnas conocidas de los archivos SQL existentes:
- cust_id (beneficiario) - YA PROBADO
- Buscar en otros campos posibles
- Sin filtros restrictivos para ver si aparecen en cualquier forma
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from mcp_bigquery_setup import MCPBigQueryBasicOperations

# Usuarios objetivo
USUARIOS_OBJETIVO = [1348718991, 468290404, 375845668]

async def busqueda_exhaustiva_ato_bq():
    """Búsqueda exhaustiva sin filtros restrictivos"""
    
    print("🔍 BÚSQUEDA EXHAUSTIVA EN ATO_BQ")
    print("📋 Sin filtros restrictivos - cualquier estado/marca")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    usuarios_str = "(" + ",".join(map(str, USUARIOS_OBJETIVO)) + ")"
    
    # Búsqueda 1: Como beneficiarios (cust_id) SIN FILTROS
    print("\n🔍 1. Búsqueda como BENEFICIARIOS (cust_id) - SIN FILTROS:")
    
    query_beneficiarios = f"""
    SELECT 
        cust_id as user_id,
        COUNT(*) as total_operaciones,
        MIN(op_datetime) as fecha_primera,
        MAX(op_datetime) as fecha_ultima,
        SUM(op_amt) as monto_total,
        COUNT(DISTINCT status_id) as estados_diferentes,
        SUM(CASE WHEN marca_ato = 1 THEN 1 ELSE 0 END) as marcados_ato,
        SUM(CASE WHEN contramarca = 0 THEN 1 ELSE 0 END) as sin_contramarca,
        COUNT(DISTINCT tipo_robo) as tipos_robo_diferentes
    FROM 
        `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
    WHERE 
        cust_id IN {usuarios_str}
    GROUP BY 
        cust_id
    ORDER BY 
        total_operaciones DESC
    """
    
    result = await operations.execute_query(query_beneficiarios, 10)
    
    beneficiarios_encontrados = False
    
    if result["status"] == "success" and result["result"]["rows"]:
        beneficiarios_encontrados = True
        print("   ✅ ¡USUARIOS ENCONTRADOS como beneficiarios!")
        
        for row in result["result"]["rows"]:
            user_id = row['user_id']
            total = row['total_operaciones']
            fecha_primera = row['fecha_primera']
            fecha_ultima = row['fecha_ultima']
            monto = row['monto_total']
            marcados = row['marcados_ato']
            sin_contra = row['sin_contramarca']
            tipos = row['tipos_robo_diferentes']
            
            print(f"\n      👤 Usuario {user_id}:")
            print(f"         📈 Total operaciones: {total:,}")
            print(f"         💰 Monto total: ${monto:,.2f}")
            print(f"         📅 Período: {fecha_primera} → {fecha_ultima}")
            print(f"         🎯 Marcados ATO: {marcados:,}")
            print(f"         ✅ Sin contramarca: {sin_contra:,}")
            print(f"         🔍 Tipos robo diferentes: {tipos}")
    else:
        print("   ❌ No encontrados como beneficiarios")
    
    # Búsqueda 2: Muestreo general para ver qué columnas existen
    print("\n🔍 2. MUESTREO GENERAL - Explorando columnas disponibles:")
    
    query_muestra = f"""
    SELECT *
    FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
    WHERE cust_id IN {usuarios_str}
    LIMIT 5
    """
    
    result_muestra = await operations.execute_query(query_muestra, 10)
    
    if result_muestra["status"] == "success" and result_muestra["result"]["rows"]:
        print("   ✅ Muestra de datos encontrada")
        
        # Obtener nombres de columnas del primer resultado
        if result_muestra["result"]["rows"]:
            primer_row = result_muestra["result"]["rows"][0]
            columnas_disponibles = list(primer_row.keys())
            
            print(f"   📊 Columnas disponibles: {len(columnas_disponibles)}")
            
            # Buscar columnas que podrían contener IDs
            columnas_id = [col for col in columnas_disponibles if 
                          any(term in col.lower() for term in ['id', 'user', 'cust', 'sender', 'receiver'])]
            
            print(f"   🎯 Columnas con IDs: {columnas_id}")
            
            return beneficiarios_encontrados, columnas_disponibles, columnas_id
    else:
        print("   ❌ No se pudo obtener muestra")
        return beneficiarios_encontrados, [], []

async def analisis_detallado_operaciones():
    """Análisis detallado de operaciones encontradas"""
    
    print("\n📊 ANÁLISIS DETALLADO DE OPERACIONES")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    usuarios_str = "(" + ",".join(map(str, USUARIOS_OBJETIVO)) + ")"
    
    # Query detallada para analizar las operaciones
    query_detallada = f"""
    SELECT 
        cust_id,
        operation_id,
        op_datetime,
        op_dt,
        op_amt,
        status_id,
        marca_ato,
        contramarca,
        tipo_robo,
        flow_type,
        payment_method,
        pay_operation_type_id,
        vertical,
        producto,
        site_id,
        tier_ato,
        casuistica,
        device_id,
        
        -- Clasificaciones
        CASE 
            WHEN op_amt > 0 THEN 'INGRESO'
            WHEN op_amt < 0 THEN 'EGRESO'
            ELSE 'NEUTRO'
        END as tipo_movimiento,
        
        CASE 
            WHEN marca_ato = 1 AND contramarca = 0 THEN 'ATO_CONFIRMADO'
            WHEN marca_ato = 1 AND contramarca = 1 THEN 'ATO_CONTRAMARCADO'
            WHEN marca_ato = 0 THEN 'NO_ATO'
            ELSE 'INDEFINIDO'
        END as estado_ato
        
    FROM 
        `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
    WHERE 
        cust_id IN {usuarios_str}
        
    ORDER BY 
        cust_id, op_datetime DESC
    LIMIT 100
    """
    
    result = await operations.execute_query(query_detallada, 100)
    
    if result["status"] == "success" and result["result"]["rows"]:
        operaciones_df = pd.DataFrame(result["result"]["rows"])
        
        print(f"✅ {len(operaciones_df)} operaciones encontradas")
        
        # Análisis por usuario
        for user_id in operaciones_df['cust_id'].unique():
            user_ops = operaciones_df[operaciones_df['cust_id'] == user_id]
            
            print(f"\n👤 USUARIO {user_id}:")
            print(f"   📈 Total operaciones: {len(user_ops)}")
            
            # Estadísticas de ATO
            ato_confirmados = user_ops[user_ops['estado_ato'] == 'ATO_CONFIRMADO']
            ato_contramarcados = user_ops[user_ops['estado_ato'] == 'ATO_CONTRAMARCADO']
            no_ato = user_ops[user_ops['estado_ato'] == 'NO_ATO']
            
            print(f"   🎯 ATO confirmados: {len(ato_confirmados)}")
            print(f"   ⚠️  ATO contramarcados: {len(ato_contramarcados)}")
            print(f"   ✅ No ATO: {len(no_ato)}")
            
            # Análisis de movimientos
            ingresos = user_ops[user_ops['tipo_movimiento'] == 'INGRESO']
            egresos = user_ops[user_ops['tipo_movimiento'] == 'EGRESO']
            
            monto_ingresos = ingresos['op_amt'].sum()
            monto_egresos = abs(egresos['op_amt'].sum())
            
            print(f"   💰 Ingresos: {len(ingresos)} ops, ${monto_ingresos:,.2f}")
            print(f"   💸 Egresos: {len(egresos)} ops, ${monto_egresos:,.2f}")
            print(f"   📊 Balance neto: ${monto_ingresos - monto_egresos:,.2f}")
            
            # Período de actividad
            fecha_primera = user_ops['op_datetime'].min()
            fecha_ultima = user_ops['op_datetime'].max()
            print(f"   📅 Período: {fecha_primera} → {fecha_ultima}")
            
            # Tipos de robo
            if len(ato_confirmados) > 0:
                tipos_robo = ato_confirmados['tipo_robo'].value_counts()
                print(f"   🔍 Tipos robo ATO: {dict(tipos_robo)}")
            
            # ANÁLISIS DE VELOCIDAD si hay ingresos ATO confirmados
            ingresos_ato = ato_confirmados[ato_confirmados['tipo_movimiento'] == 'INGRESO']
            
            if len(ingresos_ato) > 0:
                print(f"\n   ⚡ ANÁLISIS DE VELOCIDAD DE RETIRO:")
                await analizar_velocidades_usuario(user_id, ingresos_ato, operations)
            
            # Mostrar operaciones más relevantes
            print(f"\n   💰 Top 5 operaciones ATO confirmadas:")
            if len(ato_confirmados) > 0:
                top_ato = ato_confirmados.nlargest(5, 'op_amt')
                for _, op in top_ato.iterrows():
                    fecha = op['op_datetime']
                    monto = op['op_amt']
                    tipo_robo = op['tipo_robo']
                    movimiento = op['tipo_movimiento']
                    metodo = op.get('payment_method', 'N/A')
                    print(f"      {fecha}: ${monto:,.2f} ({movimiento}, {tipo_robo}, {metodo})")
            else:
                print("      ❌ No hay operaciones ATO confirmadas")
                
        return operaciones_df
    else:
        print("❌ No se encontraron operaciones detalladas")
        return None

async def analizar_velocidades_usuario(user_id, ingresos_ato, operations):
    """Analizar velocidades de retiro para un usuario específico"""
    
    velocidades_encontradas = []
    
    print(f"      🔍 Analizando {len(ingresos_ato)} ingresos ATO confirmados...")
    
    for _, ingreso in ingresos_ato.head(5).iterrows():  # Límite de 5 para no saturar
        fecha_ingreso = ingreso['op_datetime']
        monto_ingreso = ingreso['op_amt']
        operation_id_ingreso = ingreso['operation_id']
        
        # Buscar egresos posteriores (en 30 días)
        fecha_limite = pd.to_datetime(fecha_ingreso) + timedelta(days=30)
        
        query_egresos = f"""
        SELECT 
            operation_id,
            op_datetime,
            op_amt,
            pay_operation_type_id,
            payment_method,
            tipo_robo
        FROM 
            `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
        WHERE 
            cust_id = {user_id}
            AND op_datetime > '{fecha_ingreso}'
            AND op_datetime <= '{fecha_limite}'
            AND operation_id != '{operation_id_ingreso}'
            AND op_amt < 0
            
        ORDER BY op_datetime ASC
        LIMIT 10
        """
        
        result = await operations.execute_query(query_egresos, 15)
        
        if result["status"] == "success" and result["result"]["rows"]:
            egresos = pd.DataFrame(result["result"]["rows"])
            
            if len(egresos) > 0:
                primer_egreso = egresos.iloc[0]
                fecha_egreso = pd.to_datetime(primer_egreso['op_datetime'])
                fecha_ing_dt = pd.to_datetime(fecha_ingreso)
                monto_egreso = abs(primer_egreso['op_amt'])
                
                # Calcular velocidad
                velocidad_horas = (fecha_egreso - fecha_ing_dt).total_seconds() / 3600
                velocidad_dias = velocidad_horas / 24
                
                velocidades_encontradas.append(velocidad_horas)
                
                # Formato de tiempo legible
                if velocidad_horas < 1:
                    tiempo_str = f"{velocidad_horas*60:.0f} minutos"
                elif velocidad_horas < 24:
                    tiempo_str = f"{velocidad_horas:.1f} horas"
                else:
                    tiempo_str = f"{velocidad_dias:.1f} días"
                
                print(f"      💨 {tiempo_str}: ${monto_ingreso:,.2f} → ${monto_egreso:,.2f}")
                
                if velocidad_horas < 24:
                    print(f"         🚨 ALERTA: Retiro rápido!")
    
    # Resumen de velocidades
    if velocidades_encontradas:
        vel_promedio = sum(velocidades_encontradas) / len(velocidades_encontradas)
        vel_minima = min(velocidades_encontradas)
        vel_maxima = max(velocidades_encontradas)
        
        print(f"\n      📊 RESUMEN VELOCIDADES:")
        print(f"         ⚡ Promedio: {vel_promedio:.1f} horas ({vel_promedio/24:.1f} días)")
        print(f"         🔥 Mínima: {vel_minima:.1f} horas")
        print(f"         🐌 Máxima: {vel_maxima:.1f} horas")
        print(f"         📈 Total pares: {len(velocidades_encontradas)}")
        
        # Clasificación de riesgo
        rapidos = sum(1 for v in velocidades_encontradas if v < 24)
        ultra_rapidos = sum(1 for v in velocidades_encontradas if v < 1)
        
        if ultra_rapidos > 0:
            print(f"         🔥 CRÍTICO: {ultra_rapidos} retiros < 1 hora")
        elif rapidos > 0:
            print(f"         ⚠️  ALTO RIESGO: {rapidos} retiros < 24 horas")
        else:
            print(f"         ✅ NORMAL: Todos los retiros > 24 horas")

async def main():
    """Función principal"""
    
    print("🎯 BÚSQUEDA DIRECTA Y ANÁLISIS DE VELOCIDAD ATO/DTO")
    print("👥 Usuarios: " + str(USUARIOS_OBJETIVO))
    print("🔍 Análisis exhaustivo sin filtros restrictivos")
    print("=" * 80)
    
    try:
        # 1. Búsqueda exhaustiva inicial
        beneficiarios_encontrados, columnas_disponibles, columnas_id = await busqueda_exhaustiva_ato_bq()
        
        if beneficiarios_encontrados:
            # 2. Análisis detallado de operaciones
            operaciones_df = await analisis_detallado_operaciones()
            
            print("\n" + "=" * 80)
            print("🎯 CONCLUSIONES:")
            print("✅ Usuarios encontrados en operaciones ATO/DTO")
            print("📊 Análisis de velocidad de retiro completado")
            
            if operaciones_df is not None:
                ato_confirmados = operaciones_df[operaciones_df['estado_ato'] == 'ATO_CONFIRMADO']
                if len(ato_confirmados) > 0:
                    print(f"🚨 {len(ato_confirmados)} operaciones ATO confirmadas detectadas")
                    print("⚡ Métricas de velocidad de retiro calculadas")
                else:
                    print("⚠️  No hay operaciones ATO confirmadas para análisis de velocidad")
        else:
            print("\n" + "=" * 80)
            print("❌ No se encontraron usuarios en ato_bq")
            print("💡 Los usuarios podrían no estar involucrados en operaciones ATO/DTO")
            print("🔍 Verificar si están en otras tablas o períodos diferentes")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 