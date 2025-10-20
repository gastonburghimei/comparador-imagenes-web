#!/usr/bin/env python3
"""
INVESTIGACIÓN COMPLETA: Usuarios en operaciones ATO/DTO
Usuarios: 1348718991, 468290404, 375845668

Buscar en TODAS las formas posibles que estos usuarios aparezcan en ato_bq:
1. Como beneficiarios (cust_id) - YA PROBADO
2. Como emisores (otras columnas)
3. En cualquier campo relacionado
4. En diferentes períodos de tiempo
5. Con diferentes estados/marcas
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from mcp_bigquery_setup import MCPBigQueryBasicOperations

# Usuarios objetivo
USUARIOS_OBJETIVO = [1348718991, 468290404, 375845668]

async def explorar_esquema_ato_bq():
    """Explorar estructura completa de ato_bq"""
    
    print("🔍 EXPLORANDO ESTRUCTURA COMPLETA DE ATO_BQ")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # Obtener todas las columnas
    query_schema = """
    SELECT 
        column_name,
        data_type,
        description
    FROM 
        `meli-bi-data.SBOX_PFFINTECHATO.INFORMATION_SCHEMA.COLUMNS`
    WHERE 
        table_name = 'ato_bq'
    ORDER BY 
        ordinal_position
    """
    
    print("📋 Obteniendo esquema completo de ato_bq...")
    result = await operations.execute_query(query_schema, 50)
    
    if result["status"] == "success" and result["result"]["rows"]:
        print("✅ Columnas encontradas:")
        
        columnas_usuario = []
        todas_columnas = []
        
        for row in result["result"]["rows"]:
            col = row['column_name']
            tipo = row['data_type']
            desc = row.get('description', 'N/A')
            todas_columnas.append(col)
            
            # Identificar columnas que podrían contener IDs de usuario
            if any(term in col.lower() for term in ['user', 'cust', 'id', 'sender', 'receiver', 'from', 'to']):
                columnas_usuario.append(col)
                print(f"   🎯 {col} ({tipo}): {desc}")
            else:
                print(f"      {col} ({tipo})")
        
        print(f"\n📊 Total columnas: {len(todas_columnas)}")
        print(f"🎯 Columnas potenciales de usuario: {columnas_usuario}")
        
        return columnas_usuario, todas_columnas
    else:
        print("❌ No se pudo obtener esquema")
        return [], []

async def buscar_usuarios_en_todas_columnas(columnas_usuario):
    """Buscar usuarios en todas las columnas relevantes"""
    
    print("\n🔍 BÚSQUEDA EXHAUSTIVA EN TODAS LAS COLUMNAS")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    usuarios_str = "(" + ",".join(map(str, USUARIOS_OBJETIVO)) + ")"
    
    resultados_por_columna = {}
    
    # Buscar en cada columna potencial
    for columna in columnas_usuario:
        print(f"\n🔍 Buscando en columna: {columna}")
        
        query_busqueda = f"""
        SELECT 
            {columna},
            COUNT(*) as total_registros,
            MIN(op_datetime) as fecha_min,
            MAX(op_datetime) as fecha_max,
            COUNT(DISTINCT status_id) as estados_diferentes,
            SUM(CASE WHEN marca_ato = 1 THEN 1 ELSE 0 END) as marcados_ato,
            SUM(CASE WHEN contramarca = 0 THEN 1 ELSE 0 END) as sin_contramarca
        FROM 
            `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
        WHERE 
            {columna} IN {usuarios_str}
        GROUP BY 
            {columna}
        ORDER BY 
            total_registros DESC
        """
        
        try:
            result = await operations.execute_query(query_busqueda, 10)
            
            if result["status"] == "success" and result["result"]["rows"]:
                print(f"   ✅ ¡ENCONTRADOS en {columna}!")
                
                resultados_por_columna[columna] = pd.DataFrame(result["result"]["rows"])
                
                for _, row in resultados_por_columna[columna].iterrows():
                    user_id = row[columna]
                    total = row['total_registros']
                    fecha_min = row['fecha_min']
                    fecha_max = row['fecha_max']
                    marcados = row['marcados_ato']
                    sin_contra = row['sin_contramarca']
                    
                    print(f"      👤 Usuario {user_id}:")
                    print(f"         📈 Total operaciones: {total:,}")
                    print(f"         📅 Período: {fecha_min} → {fecha_max}")
                    print(f"         🎯 Marcados ATO: {marcados:,}")
                    print(f"         ✅ Sin contramarca: {sin_contra:,}")
            else:
                print(f"   ❌ No encontrados en {columna}")
                
        except Exception as e:
            print(f"   ❌ Error buscando en {columna}: {str(e)[:100]}...")
    
    return resultados_por_columna

async def analizar_operaciones_detalladas(resultados_por_columna):
    """Análisis detallado de las operaciones encontradas"""
    
    print("\n📊 ANÁLISIS DETALLADO DE OPERACIONES ENCONTRADAS")
    print("=" * 80)
    
    if not resultados_por_columna:
        print("❌ No se encontraron usuarios en ninguna columna de ato_bq")
        return
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    usuarios_str = "(" + ",".join(map(str, USUARIOS_OBJETIVO)) + ")"
    
    for columna, df_resultados in resultados_por_columna.items():
        print(f"\n🔍 DETALLE DE OPERACIONES EN COLUMNA: {columna}")
        print("-" * 60)
        
        # Query detallada para esta columna
        query_detalle = f"""
        SELECT 
            {columna} as user_id,
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
            
            -- Clasificar si es ingreso o egreso
            CASE 
                WHEN op_amt > 0 THEN 'INGRESO'
                WHEN op_amt < 0 THEN 'EGRESO'
                ELSE 'NEUTRO'
            END as tipo_movimiento,
            
            -- Determinar rol del usuario
            CASE 
                WHEN '{columna}' = 'cust_id' THEN 'BENEFICIARIO'
                WHEN '{columna}' LIKE '%sender%' THEN 'EMISOR'
                WHEN '{columna}' LIKE '%from%' THEN 'ORIGEN'
                WHEN '{columna}' LIKE '%to%' THEN 'DESTINO'
                ELSE 'PARTICIPANTE'
            END as rol_usuario
            
        FROM 
            `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
        WHERE 
            {columna} IN {usuarios_str}
            AND marca_ato = 1
            AND contramarca = 0
            
        ORDER BY 
            {columna}, op_datetime DESC
        LIMIT 50
        """
        
        result = await operations.execute_query(query_detalle, 50)
        
        if result["status"] == "success" and result["result"]["rows"]:
            operaciones_df = pd.DataFrame(result["result"]["rows"])
            
            print(f"✅ {len(operaciones_df)} operaciones ATO confirmadas")
            
            # Análisis por usuario
            for user_id in operaciones_df['user_id'].unique():
                user_ops = operaciones_df[operaciones_df['user_id'] == user_id]
                
                print(f"\n   👤 Usuario {user_id} (ROL: {user_ops['rol_usuario'].iloc[0]}):")
                
                # Estadísticas generales
                total_monto = user_ops['op_amt'].sum()
                ingresos = user_ops[user_ops['tipo_movimiento'] == 'INGRESO']
                egresos = user_ops[user_ops['tipo_movimiento'] == 'EGRESO']
                
                print(f"      📈 Total operaciones: {len(user_ops)}")
                print(f"      💰 Monto neto: ${total_monto:,.2f}")
                print(f"      📊 Ingresos: {len(ingresos)} (${ingresos['op_amt'].sum():,.2f})")
                print(f"      📊 Egresos: {len(egresos)} (${abs(egresos['op_amt'].sum()):,.2f})")
                
                # Tipos de robo
                tipos_robo = user_ops['tipo_robo'].value_counts()
                print(f"      🔍 Tipos robo: {dict(tipos_robo)}")
                
                # Análisis temporal
                fecha_primera = user_ops['op_datetime'].min()
                fecha_ultima = user_ops['op_datetime'].max()
                print(f"      📅 Período actividad: {fecha_primera} → {fecha_ultima}")
                
                # Si es beneficiario, buscar retiros posteriores
                if user_ops['rol_usuario'].iloc[0] == 'BENEFICIARIO' and len(ingresos) > 0:
                    print(f"\n      🔍 ANÁLISIS DE VELOCIDAD DE RETIRO:")
                    await analizar_velocidad_retiro_usuario(user_id, ingresos, operations)
                
                # Mostrar operaciones más grandes
                print(f"\n      💰 Top 5 operaciones:")
                top_ops = user_ops.nlargest(5, 'op_amt')
                for _, op in top_ops.iterrows():
                    fecha = op['op_datetime']
                    monto = op['op_amt']
                    tipo = op['tipo_robo']
                    movimiento = op['tipo_movimiento']
                    metodo = op.get('payment_method', 'N/A')
                    print(f"         {fecha}: ${monto:,.2f} ({movimiento}, {tipo}, {metodo})")

async def analizar_velocidad_retiro_usuario(user_id, ingresos, operations):
    """Analizar velocidad de retiro específica para un usuario"""
    
    velocidades = []
    
    for _, ingreso in ingresos.head(3).iterrows():  # Solo primeros 3 para no saturar
        fecha_ingreso = ingreso['op_datetime']
        monto_ingreso = ingreso['op_amt']
        operation_id_ingreso = ingreso['operation_id']
        
        # Buscar retiros/egresos posteriores en 30 días
        fecha_limite = pd.to_datetime(fecha_ingreso) + timedelta(days=30)
        
        query_retiros = f"""
        SELECT 
            operation_id,
            op_datetime,
            op_amt,
            pay_operation_type_id,
            payment_method
        FROM 
            `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
        WHERE 
            cust_id = {user_id}
            AND op_datetime > '{fecha_ingreso}'
            AND op_datetime <= '{fecha_limite}'
            AND operation_id != '{operation_id_ingreso}'
            AND (op_amt < 0 OR pay_operation_type_id IN ('money_out', 'transfer_out', 'withdrawal'))
            
        ORDER BY op_datetime ASC
        LIMIT 5
        """
        
        result = await operations.execute_query(query_retiros, 10)
        
        if result["status"] == "success" and result["result"]["rows"]:
            retiros = pd.DataFrame(result["result"]["rows"])
            
            if len(retiros) > 0:
                primer_retiro = retiros.iloc[0]
                fecha_retiro = pd.to_datetime(primer_retiro['op_datetime'])
                fecha_ing_dt = pd.to_datetime(fecha_ingreso)
                
                velocidad_horas = (fecha_retiro - fecha_ing_dt).total_seconds() / 3600
                
                velocidades.append(velocidad_horas)
                
                if velocidad_horas < 1:
                    tiempo_str = f"{velocidad_horas*60:.0f} minutos"
                else:
                    tiempo_str = f"{velocidad_horas:.1f} horas"
                
                print(f"         💨 {tiempo_str}: ${monto_ingreso:.2f} → ${abs(primer_retiro['op_amt']):.2f}")
    
    if velocidades:
        vel_promedio = sum(velocidades) / len(velocidades)
        vel_minima = min(velocidades)
        
        print(f"         ⚡ Velocidad promedio: {vel_promedio:.1f} horas")
        print(f"         🔥 Velocidad mínima: {vel_minima:.1f} horas")
        
        if vel_minima < 24:
            print(f"         🚨 ALERTA: Retiro rápido detectado!")

async def main():
    """Función principal"""
    
    print("🕵️ INVESTIGACIÓN COMPLETA: USUARIOS EN OPERACIONES ATO/DTO")
    print("👥 Usuarios: " + str(USUARIOS_OBJETIVO))
    print("🔍 Buscando en TODOS los roles posibles en ato_bq")
    print("=" * 80)
    
    try:
        # 1. Explorar esquema completo
        columnas_usuario, todas_columnas = await explorar_esquema_ato_bq()
        
        if not columnas_usuario:
            print("❌ No se pudo obtener esquema de ato_bq")
            return
        
        # 2. Buscar usuarios en todas las columnas relevantes
        resultados_por_columna = await buscar_usuarios_en_todas_columnas(columnas_usuario)
        
        # 3. Análisis detallado de operaciones encontradas
        await analizar_operaciones_detalladas(resultados_por_columna)
        
        print("\n" + "=" * 80)
        print("🎯 INVESTIGACIÓN COMPLETADA")
        
        if resultados_por_columna:
            print("✅ Usuarios encontrados en operaciones ATO/DTO")
            print("📊 Datos disponibles para análisis de velocidad")
        else:
            print("❌ Usuarios NO encontrados en ningún rol en ato_bq")
            print("💡 Podrían no estar involucrados en operaciones ATO/DTO confirmadas")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 