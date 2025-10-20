#!/usr/bin/env python3
"""
🏛️ ANÁLISIS OFICIAL CUENTA_HACKER - USUARIO 1105384237
Siguiendo el esquema oficial exacto de Mercado Pago
"""

import asyncio
import sys
from datetime import datetime
from analizador_mcp_real_integrado import AnalizadorMCPRealIntegrado

async def analizar_usuario_1105384237():
    """Análisis completo oficial del usuario 1105384237"""
    
    analizador = AnalizadorMCPRealIntegrado()
    
    if not await analizador.initialize():
        print("❌ Error inicializando BigQuery")
        return
    
    user_id = "760785507"
    
    print("🏛️ ANÁLISIS OFICIAL CUENTA_HACKER - MERCADO PAGO")
    print("=" * 80)
    print(f"👤 USUARIO: {user_id}")
    print(f"📅 ANÁLISIS: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 80)
    
    # PREREQUISITO: Verificar transacciones ATO/DTO
    print("🔍 PREREQUISITO: VERIFICAR TRANSACCIONES ATO/DTO")
    transacciones_ato = await verificar_transacciones_ato_dto_corregido(analizador, user_id)
    
    if transacciones_ato['cantidad'] == 0:
        print("⚠️  USUARIO NO DEBERÍA ESTAR EN ANÁLISIS")
        print("   → 0 transacciones ATO/DTO detectadas")
        print("   → Este usuario no cumple prerequisito para ser CUENTA_HACKER")
        print("\n🎯 DECISIÓN FINAL: NO_APLICA")
        return {'decision': 'NO_APLICA', 'motivo': 'Sin transacciones ATO/DTO'}
    
    print(f"   ✅ Prerequisito cumplido: {transacciones_ato['cantidad']} transacciones ATO/DTO")
    print(f"   💰 Monto total: ${transacciones_ato['monto']:,.2f}")
    
    cantidad_transacciones = transacciones_ato['cantidad']
    
    # FLUJO 1: 1 transacción ATO/DTO
    if cantidad_transacciones == 1:
        print("\n📋 FLUJO 1: ANÁLISIS PARA 1 TRANSACCIÓN ATO/DTO")
        print("-" * 60)
        return await ejecutar_flujo_1(analizador, user_id, transacciones_ato)
    
    # FLUJO 2: 2+ transacciones ATO/DTO
    else:
        print(f"\n📋 FLUJO 2: ANÁLISIS PARA {cantidad_transacciones} TRANSACCIONES ATO/DTO")
        print("-" * 60)
        return await ejecutar_flujo_2(analizador, user_id, transacciones_ato)

async def verificar_transacciones_ato_dto_corregido(analizador, user_id):
    """Verificar transacciones ATO/DTO usando la tabla oficial cta_hacker_chequeo_final"""
    try:
        query = f"""
        SELECT 
            USER_ID,
            cant_trans_marcadas,
            monto_marcado,
            cant_trans_total,
            monto_recibido_total,
            fecha_creacion_cuenta,
            dias_cuenta_activa
        FROM `meli-bi-data.SBOX_PFFINTECHATO.cta_hacker_chequeo_final`
        WHERE USER_ID = {user_id}
        LIMIT 1
        """
        
        result = await analizador.operations.execute_query(query)
        
        # Extraer los datos correctamente del resultado MCP
        if result and result.get('status') == 'success':
            rows = result.get('result', {}).get('rows', [])
            
            if rows and len(rows) > 0:
                row = rows[0]
                
                cantidad = row.get('cant_trans_marcadas', 0) or 0
                monto = row.get('monto_marcado', 0) or 0
                
                print(f"   📊 Datos de tabla oficial:")
                print(f"   📊 Transacciones marcadas: {cantidad}")
                print(f"   📊 Monto marcado: ${monto:,.2f}")
                print(f"   📊 Transacciones totales: {row.get('cant_trans_total', 0)}")
                print(f"   📊 Monto total recibido: ${row.get('monto_recibido_total', 0):,.2f}")
                
                return {
                    'cantidad': cantidad,
                    'monto': monto,
                    'cant_trans_total': row.get('cant_trans_total', 0),
                    'monto_recibido_total': row.get('monto_recibido_total', 0),
                    'fecha_creacion_cuenta': row.get('fecha_creacion_cuenta'),
                    'dias_cuenta_activa': row.get('dias_cuenta_activa')
                }
            else:
                print(f"   ❌ Usuario no encontrado en tabla oficial")
                return {'cantidad': 0, 'monto': 0}
        else:
            print(f"   ❌ Error en consulta o usuario no encontrado")
            return {'cantidad': 0, 'monto': 0}
            
    except Exception as e:
        print(f"   ❌ Error verificando transacciones: {e}")
        return {'cantidad': 0, 'monto': 0}

async def verificar_antiguedad_corregida(analizador, user_id, datos_transacciones=None):
    """Verificar antigüedad con umbral de 30 días usando datos de la tabla oficial"""
    
    # Si ya tenemos los datos de la tabla oficial, usarlos
    if datos_transacciones and 'dias_cuenta_activa' in datos_transacciones:
        dias_antiguedad = datos_transacciones.get('dias_cuenta_activa', 0) or 0
        fecha_creacion = datos_transacciones.get('fecha_creacion_cuenta')
        
        es_nueva = dias_antiguedad <= 30  # 30 días según esquema oficial
        
        print(f"   📅 Fecha creación: {fecha_creacion}")
        print(f"   ⏰ Días de antigüedad: {dias_antiguedad}")
        print(f"   📊 ¿Es cuenta nueva? {'❌ Sí' if es_nueva else '✅ No'} (umbral: 30 días)")
        
        return {
            'es_nueva': es_nueva,
            'dias_antiguedad': dias_antiguedad,
            'fecha_creacion': fecha_creacion
        }
    
    # Fallback: consultar directamente si no tenemos los datos
    try:
        query = f"""
        SELECT 
            CUS_RU_SINCE_DT as fecha_creacion,
            DATE_DIFF(CURRENT_DATE(), DATE(CUS_RU_SINCE_DT), DAY) as dias_antiguedad
        FROM `meli-bi-data.WHOWNER.LK_CUS_CUSTOMERS_DATA`
        WHERE CUS_CUST_ID = {user_id}
        """
        
        result = await analizador.operations.execute_query(query)
        
        if result and len(result) > 0:
            row = result[0] if isinstance(result[0], dict) else dict(zip(['fecha_creacion', 'dias_antiguedad'], result[0]))
            dias_antiguedad = row.get('dias_antiguedad', 0) or 0
            fecha_creacion = row.get('fecha_creacion')
            
            es_nueva = dias_antiguedad <= 30  # 30 días según esquema oficial
            
            print(f"   📅 Fecha creación: {fecha_creacion}")
            print(f"   ⏰ Días de antigüedad: {dias_antiguedad}")
            print(f"   📊 ¿Es cuenta nueva? {'❌ Sí' if es_nueva else '✅ No'} (umbral: 30 días)")
            
            return {
                'es_nueva': es_nueva,
                'dias_antiguedad': dias_antiguedad,
                'fecha_creacion': fecha_creacion
            }
        else:
            print(f"   ❌ No se encontraron datos de antigüedad")
            return {'es_nueva': True, 'dias_antiguedad': 0, 'fecha_creacion': None}
            
    except Exception as e:
        print(f"   ❌ Error verificando antigüedad: {e}")
        return {'es_nueva': True, 'dias_antiguedad': 0, 'fecha_creacion': None}

async def verificar_porcentaje_corregido(analizador, user_id):
    """Verificar porcentaje con umbral del 15%"""
    try:
        query = f"""
        WITH ato_transacciones AS (
            SELECT 
                COUNT(DISTINCT operation_id) as cant_ato,
                ROUND(SUM(op_amt), 2) as monto_ato
            FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
            WHERE CAST(GCA_CUST_ID AS STRING) = '{user_id}'
                AND status_id = 'A'
                AND contramarca = 0
                AND flow_type NOT IN ('PI', 'MF')
        ),
        total_transacciones AS (
            SELECT 
                COUNT(DISTINCT PAY_PAYMENT_ID) as cant_total,
                ROUND(SUM(PAY_TRANSACTION_DOL_AMT), 2) as monto_total
            FROM `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
            WHERE CUS_CUST_ID_SEL = {user_id}
                AND pay_status_id NOT IN ('rejected', 'pending')
                AND tpv_flag = 1
        )
        SELECT 
            a.cant_ato,
            a.monto_ato,
            t.cant_total,
            t.monto_total,
            ROUND(100.0 * a.cant_ato / NULLIF(t.cant_total, 0), 2) as porcentaje_cantidad,
            ROUND(100.0 * a.monto_ato / NULLIF(t.monto_total, 0), 2) as porcentaje_monto
        FROM ato_transacciones a
        CROSS JOIN total_transacciones t
        """
        
        result = await analizador.operations.execute_query(query)
        
        if result and len(result) > 0:
            row = result[0]
            porcentaje_cantidad = row.get('porcentaje_cantidad', 0) or 0
            porcentaje_monto = row.get('porcentaje_monto', 0) or 0
            
            # Umbral 15% según esquema oficial
            supera_umbral = porcentaje_cantidad >= 15 or porcentaje_monto >= 15
            
            print(f"   📊 Transacciones ATO/DTO: {row.get('cant_ato', 0)}")
            print(f"   📊 Transacciones totales: {row.get('cant_total', 0)}")
            print(f"   📊 % en cantidad: {porcentaje_cantidad:.1f}%")
            print(f"   📊 % en monto: {porcentaje_monto:.1f}%")
            print(f"   📊 ¿Supera 15%? {'❌ Sí' if supera_umbral else '✅ No'}")
            
            return {
                'porcentaje_cantidad': porcentaje_cantidad,
                'porcentaje_monto': porcentaje_monto,
                'supera_umbral': supera_umbral,
                'cant_ato': row.get('cant_ato', 0),
                'cant_total': row.get('cant_total', 0),
                'monto_ato': row.get('monto_ato', 0),
                'monto_total': row.get('monto_total', 0)
            }
        else:
            print(f"   ❌ No se pudieron calcular porcentajes")
            return {'porcentaje_cantidad': 0, 'porcentaje_monto': 0, 'supera_umbral': False}
            
    except Exception as e:
        print(f"   ❌ Error calculando porcentajes: {e}")
        return {'porcentaje_cantidad': 0, 'porcentaje_monto': 0, 'supera_umbral': False}

async def verificar_marcas_oficiales(analizador, user_id):
    """Verificar marcas relevantes según lista oficial en ambas tablas"""
    
    # Lista oficial de marcas relevantes según esquema
    marcas_oficiales = [
        'big_sellers', 'top_big_sellers', 'comerciales', 'key_users', 'referidos', 
        'legales', 'influencers', 'personalidades_prominentes', 
        'usuarios_test_productivos', 'cuenta_interna', 'usuarios_tpv_high'
    ]
    
    marcas_encontradas = []
    marcas_completas = ""
    
    # 1. Buscar en base_ato_escalabilidad_final
    try:
        query1 = f"""
        SELECT marcas
        FROM `meli-bi-data.SBOX_PFFINTECHATO.base_ato_escalabilidad_final`
        WHERE CAST(user_id AS STRING) = '{user_id}'
        LIMIT 1
        """
        
        result1 = await analizador.operations.execute_query(query1)
        
        if result1 and result1.get('status') == 'success':
            rows1 = result1.get('result', {}).get('rows', [])
            if rows1:
                marcas_base = rows1[0].get('marcas', '')
                marcas_completas += f"Base escalabilidad: {marcas_base} "
                
                if isinstance(marcas_base, list):
                    marcas_texto = ' '.join(str(tag) for tag in marcas_base)
                else:
                    marcas_texto = str(marcas_base) if marcas_base else ''
                
                for marca in marcas_oficiales:
                    # Variaciones específicas para cada marca
                    variaciones = [
                        marca,
                        marca.replace('_', ' '),
                        marca.replace('usuarios_', ''),
                        marca.replace('top_', ''),
                        marca.replace('_sellers', '_seller'),
                        marca.replace('_users', '_user')
                    ]
                    
                    for variacion in variaciones:
                        if variacion.lower() in marcas_texto.lower():
                            if marca not in marcas_encontradas:
                                marcas_encontradas.append(marca)
                            break
    except Exception as e:
        print(f"   ⚠️  Error en base_ato_escalabilidad_final: {e}")
    
    # 2. Buscar en LK_CUS_CUSTOMERS_DATA
    try:
        query2 = f"""
        SELECT cus_internal_tags
        FROM `meli-bi-data.WHOWNER.LK_CUS_CUSTOMERS_DATA`
        WHERE CUS_CUST_ID = {user_id}
        LIMIT 1
        """
        
        result2 = await analizador.operations.execute_query(query2)
        
        if result2 and result2.get('status') == 'success':
            rows2 = result2.get('result', {}).get('rows', [])
            if rows2:
                tags_customer = rows2[0].get('cus_internal_tags', '')
                marcas_completas += f"Customer tags: {tags_customer}"
                
                if isinstance(tags_customer, list):
                    tags_texto = ' '.join(str(tag) for tag in tags_customer)
                else:
                    tags_texto = str(tags_customer) if tags_customer else ''
                
                for marca in marcas_oficiales:
                    # Variaciones específicas para cada marca
                    variaciones = [
                        marca,
                        marca.replace('_', ' '),
                        marca.replace('usuarios_', ''),
                        marca.replace('top_', ''),
                        marca.replace('_sellers', '_seller'),
                        marca.replace('_users', '_user')
                    ]
                    
                    for variacion in variaciones:
                        if variacion.lower() in tags_texto.lower():
                            if marca not in marcas_encontradas:
                                marcas_encontradas.append(marca)
                            break
    except Exception as e:
        print(f"   ⚠️  Error en LK_CUS_CUSTOMERS_DATA: {e}")
    
    tiene_marcas = len(marcas_encontradas) > 0
    
    if marcas_encontradas:
        print(f"   ✅ Marcas relevantes encontradas: {', '.join(marcas_encontradas)}")
    else:
        print(f"   ❌ No se encontraron marcas relevantes oficiales")
        print(f"   📋 Marcas encontradas: {marcas_completas}")
    
    return {
        'tiene_marcas': tiene_marcas,
        'marcas_encontradas': marcas_encontradas,
        'marcas_completas': marcas_completas
    }

async def verificar_velocidad_6_horas(analizador, user_id):
    """Verificar velocidad de retirada con umbral de 6 horas"""
    try:
        query = f"""
        WITH ingresos AS (
            SELECT 
                MOV_CREATED_DATETIME as fecha_ingreso,
                MOV_DOL_AMOUNT as monto_ingreso
            FROM `meli-bi-data.WHOWNER.BT_MP_ACC_MOVEMENTS`
            WHERE CUS_CUST_ID_SEL = {user_id}
                AND MOV_TYPE = 'IN'
                AND MOV_DOL_AMOUNT > 0
                AND MOV_CREATED_DATETIME >= '2024-01-01'
        ),
        retiros AS (
            SELECT 
                pay_move_date as fecha_retiro,
                PAY_TRANSACTION_DOL_AMT as monto_retiro,
                PAY_PAYMENT_METHOD_TYPE as metodo
            FROM `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
            WHERE CUS_CUST_ID_SEL = {user_id}
                AND PAY_TRANSACTION_DOL_AMT > 0
                AND pay_move_date >= '2024-01-01'
                AND pay_status_id = 'approved'
        )
        SELECT 
            i.fecha_ingreso,
            i.monto_ingreso,
            r.fecha_retiro,
            r.monto_retiro,
            r.metodo,
            DATETIME_DIFF(r.fecha_retiro, i.fecha_ingreso, HOUR) as horas_diferencia
        FROM ingresos i
        JOIN retiros r ON r.fecha_retiro >= i.fecha_ingreso
            AND r.fecha_retiro <= DATETIME_ADD(i.fecha_ingreso, INTERVAL 7 DAY)
        ORDER BY i.fecha_ingreso, r.fecha_retiro
        LIMIT 10
        """
        
        result = await analizador.operations.execute_query(query)
        
        if result and len(result) > 0:
            retiros_rapidos = [r for r in result if r.get('horas_diferencia', 999) <= 6]
            es_rapida = len(retiros_rapidos) > 0
            
            print(f"   📊 Retiros analizados: {len(result)}")
            print(f"   ⚡ Retiros rápidos (≤6 horas): {len(retiros_rapidos)}")
            
            if retiros_rapidos:
                for retiro in retiros_rapidos:
                    horas = retiro.get('horas_diferencia', 0)
                    monto = retiro.get('monto_retiro', 0)
                    print(f"     • ${monto:,.2f} retirado en {horas} horas")
            
            print(f"   📊 ¿Velocidad rápida? {'❌ Sí' if es_rapida else '✅ No'} (≤6 horas)")
            
            return {
                'es_rapida': es_rapida,
                'total_retiros': len(result),
                'retiros_rapidos': len(retiros_rapidos),
                'detalles': retiros_rapidos
            }
        else:
            print(f"   ❌ No se encontraron datos de retiros")
            return {'es_rapida': False, 'total_retiros': 0, 'retiros_rapidos': 0, 'detalles': []}
            
    except Exception as e:
        print(f"   ❌ Error verificando velocidad: {e}")
        return {'es_rapida': False, 'total_retiros': 0, 'retiros_rapidos': 0, 'detalles': []}

async def verificar_contacto_oficial(analizador, user_id):
    """Verificar contacto desconociendo pagos"""
    try:
        query = f"""
        SELECT 
            case_id,
            fecha_apertura_caso,
            subtype1,
            CURRENT_DATE() as fecha_actual
        FROM `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO`
        WHERE user_id = {user_id}
            AND subtype1 = 'cuenta_de_hacker'
            AND fecha_apertura_caso >= '2024-01-01'
        ORDER BY fecha_apertura_caso DESC
        LIMIT 5
        """
        
        result = await analizador.operations.execute_query(query)
        
        if result and len(result) > 0:
            tiene_contacto = True
            casos_contacto = len(result)
            
            print(f"   ✅ Usuario contactó desconociendo pagos")
            print(f"   📞 Casos de contacto: {casos_contacto}")
            
            for caso in result:
                case_id = caso.get('case_id', 'N/A')
                fecha = caso.get('fecha_apertura_caso', 'N/A')
                print(f"     • Caso {case_id}: {fecha}")
            
            return {
                'tiene_contacto': tiene_contacto,
                'casos_contacto': casos_contacto,
                'detalles': result
            }
        else:
            print(f"   ❌ No hay registro de contacto del usuario")
            return {'tiene_contacto': False, 'casos_contacto': 0, 'detalles': []}
            
    except Exception as e:
        print(f"   ❌ Error verificando contacto: {e}")
        return {'tiene_contacto': False, 'casos_contacto': 0, 'detalles': []}

async def ejecutar_flujo_1(analizador, user_id, transacciones_ato):
    """Ejecutar Flujo 1: 1 transacción ATO/DTO"""
    
    # PASO 1: Verificar antigüedad de cuenta
    print("1️⃣ EVALUANDO ANTIGÜEDAD DE CUENTA...")
    antiguedad = await verificar_antiguedad_corregida(analizador, user_id, transacciones_ato)
    
    if antiguedad['es_nueva']:
        print("\n🏛️ DECISIÓN FINAL: ✅ CUENTA_HACKER_CONFIRMADA")
        print("📝 MOTIVO: Cuenta nueva (≤30 días) con transacción ATO/DTO")
        return {
            'decision': 'CUENTA_HACKER_CONFIRMADA',
            'motivo': 'Cuenta nueva con transacción ATO/DTO',
            'detalles': {'antiguedad': antiguedad, 'transacciones': transacciones_ato}
        }
    
    # PASO 2: Verificar porcentaje de transacciones ATO/DTO vs total
    print("\n2️⃣ EVALUANDO % TRANSACCIONES ATO/DTO VS TOTAL...")
    porcentaje = await verificar_porcentaje_corregido(analizador, user_id)
    
    if porcentaje['supera_umbral']:
        print("\n🏛️ DECISIÓN FINAL: ✅ CUENTA_HACKER_CONFIRMADA")
        print("📝 MOTIVO: Porcentaje ATO/DTO ≥15% del total de transacciones")
        return {
            'decision': 'CUENTA_HACKER_CONFIRMADA',
            'motivo': 'Alto porcentaje de transacciones ATO/DTO (≥15%)',
            'detalles': {'antiguedad': antiguedad, 'porcentaje': porcentaje, 'transacciones': transacciones_ato}
        }
    else:
        print("\n🏛️ DECISIÓN FINAL: ❌ CUENTA_HACKER_DESESTIMADA")
        print("📝 MOTIVO: Cuenta antigua con bajo porcentaje ATO/DTO (<15%)")
        return {
            'decision': 'CUENTA_HACKER_DESESTIMADA',
            'motivo': 'Cuenta antigua con bajo porcentaje ATO/DTO (<15%)',
            'detalles': {'antiguedad': antiguedad, 'porcentaje': porcentaje, 'transacciones': transacciones_ato}
        }

async def ejecutar_flujo_2(analizador, user_id, transacciones_ato):
    """Ejecutar Flujo 2: 2+ transacciones ATO/DTO"""
    
    # PASO 1: Verificar antigüedad de cuenta
    print("1️⃣ EVALUANDO ANTIGÜEDAD DE CUENTA...")
    antiguedad = await verificar_antiguedad_corregida(analizador, user_id, transacciones_ato)
    
    if antiguedad['es_nueva']:
        print("\n🏛️ DECISIÓN FINAL: ✅ CUENTA_HACKER_CONFIRMADA")
        print("📝 MOTIVO: Cuenta nueva (≤30 días) con múltiples transacciones ATO/DTO")
        return {
            'decision': 'CUENTA_HACKER_CONFIRMADA',
            'motivo': 'Cuenta nueva con múltiples transacciones ATO/DTO',
            'detalles': {'antiguedad': antiguedad, 'transacciones': transacciones_ato}
        }
    
    # PASO 2: Verificar marcas relevantes
    print("\n2️⃣ EVALUANDO MARCAS RELEVANTES...")
    marcas = await verificar_marcas_oficiales(analizador, user_id)
    
    if not marcas['tiene_marcas']:
        print("\n🏛️ DECISIÓN FINAL: ✅ CUENTA_HACKER_CONFIRMADA")
        print("📝 MOTIVO: Sin marcas relevantes que protejan al usuario")
        return {
            'decision': 'CUENTA_HACKER_CONFIRMADA',
            'motivo': 'Sin marcas relevantes que protejan al usuario',
            'detalles': {'antiguedad': antiguedad, 'marcas': marcas, 'transacciones': transacciones_ato}
        }
    
    # PASO 3: Solicitar datos MCP para cruces de riesgo
    print("\n3️⃣ EVALUANDO CRUCES DE RIESGO...")
    print("🚀 DATOS MCP REQUERIDOS:")
    print(f"   Ejecutar en Cursor:")
    print(f"   1. @get_subgraph id=\"user-{user_id}\" relations=[\"uses_device\", \"uses_card\", \"validate_phone\", \"validate_person\", \"withdrawal_bank_account\"]")
    print(f"   2. @get_user_hops_to_fraud id={user_id} boundLevel=\"HIGH_TRUST\"")
    print()
    print("❓ Una vez que obtengas los datos MCP, compártelos para continuar el análisis.")
    print("   Por ahora continuaré con los pasos que no requieren MCP...")
    
    # PASO 4: Verificar velocidad de retirada
    print("\n4️⃣ EVALUANDO VELOCIDAD DE RETIRADA...")
    velocidad = await verificar_velocidad_6_horas(analizador, user_id)
    
    if velocidad['es_rapida']:
        print("\n🏛️ DECISIÓN FINAL: ✅ CUENTA_HACKER_CONFIRMADA")
        print("📝 MOTIVO: Velocidad de retirada rápida (≤6 horas)")
        return {
            'decision': 'CUENTA_HACKER_CONFIRMADA',
            'motivo': 'Velocidad de retirada rápida (≤6 horas)',
            'detalles': {'antiguedad': antiguedad, 'marcas': marcas, 'velocidad': velocidad, 'transacciones': transacciones_ato}
        }
    
    # PASO 5: Verificar cantidad de transacciones
    cantidad_transacciones = transacciones_ato['cantidad']
    print(f"\n5️⃣ EVALUANDO CANTIDAD DE TRANSACCIONES...")
    print(f"   📊 Cantidad: {cantidad_transacciones} transacciones ATO/DTO")
    
    if cantidad_transacciones == 2:
        print("\n🏛️ DECISIÓN FINAL: ❌ CUENTA_HACKER_DESESTIMADA")
        print("📝 MOTIVO: Solo 2 transacciones ATO/DTO sin otros factores de riesgo")
        return {
            'decision': 'CUENTA_HACKER_DESESTIMADA',
            'motivo': 'Solo 2 transacciones ATO/DTO sin otros factores de riesgo',
            'detalles': {'antiguedad': antiguedad, 'marcas': marcas, 'velocidad': velocidad, 'transacciones': transacciones_ato}
        }
    
    # PASO 6: Verificar contacto/desconocimiento
    print("\n6️⃣ EVALUANDO CONTACTO/DESCONOCIMIENTO...")
    contacto = await verificar_contacto_oficial(analizador, user_id)
    
    if contacto['tiene_contacto']:
        print("\n🏛️ DECISIÓN FINAL: ❌ CUENTA_HACKER_DESESTIMADA")
        print("📝 MOTIVO: Usuario contactó desconociendo los pagos")
        return {
            'decision': 'CUENTA_HACKER_DESESTIMADA',
            'motivo': 'Usuario contactó desconociendo los pagos',
            'detalles': {'antiguedad': antiguedad, 'marcas': marcas, 'velocidad': velocidad, 'contacto': contacto, 'transacciones': transacciones_ato}
        }
    else:
        print("\n🏛️ DECISIÓN FINAL: ✅ CUENTA_HACKER_CONFIRMADA")
        print("📝 MOTIVO: 3+ transacciones ATO/DTO sin contacto desconociendo")
        return {
            'decision': 'CUENTA_HACKER_CONFIRMADA',
            'motivo': '3+ transacciones ATO/DTO sin contacto desconociendo',
            'detalles': {'antiguedad': antiguedad, 'marcas': marcas, 'velocidad': velocidad, 'contacto': contacto, 'transacciones': transacciones_ato}
        }

if __name__ == "__main__":
    asyncio.run(analizar_usuario_1105384237())