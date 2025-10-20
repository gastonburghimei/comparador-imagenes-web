#!/usr/bin/env python3
"""
🚀 ANALIZADOR CUENTA_HACKER CON MCP REAL INTEGRADO
Versión actualizada que usa datos reales del Account Relations MCP
"""

import asyncio
import sys
from datetime import datetime
from mcp_bigquery_setup import MCPBigQueryBasicOperations

class AnalizadorMCPRealIntegrado:
    """
    Analizador que implementa el esquema oficial MP con datos MCP reales
    """
    
    def __init__(self):
        self.operations = None
        
    async def initialize(self):
        """Inicializar conexión a BigQuery"""
        try:
            self.operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
            await self.operations.initialize()
            return True
        except Exception as e:
            print(f"❌ Error conectando a BigQuery: {e}")
            return False

    def procesar_subgraph_mcp(self, subgraph_json):
        """
        Procesar datos del subgrafo MCP y extraer cruces de riesgo
        """
        cruces_detectados = []
        usuarios_relacionados = []
        
        if not subgraph_json or 'edges' not in subgraph_json:
            return {
                'tiene_cruces': False,
                'tipos_cruces': [],
                'usuarios_relacionados': [],
                'total_cruces': 0
            }
        
        # Analizar edges para encontrar usuarios conectados
        edges = subgraph_json['edges']
        user_id_target = None
        
        # Identificar el usuario principal
        for edge in edges:
            if edge['source'].startswith('user-') and edge['target'].startswith('user-'):
                continue  # Skip user-to-user direct connections
            if edge['source'].startswith('user-'):
                user_id_target = edge['source']
                break
        
        if not user_id_target:
            # Buscar en nodes si no se encuentra en edges
            for node in subgraph_json.get('nodes', []):
                if node['id'].startswith('user-') and node.get('properties', {}).get('is_driver_user', False):
                    user_id_target = node['id']
                    break
        
        # Agrupar conexiones por tipo de recurso compartido
        dispositivos_compartidos = {}
        tarjetas_compartidas = {}
        telefonos_compartidos = {}
        personas_compartidas = {}
        cuentas_bancarias_compartidas = {}
        
        for edge in edges:
            source_user = edge['source']
            target_resource = edge['target']
            edge_type = edge['label']
            
            # Solo procesar edges que NO sean del usuario principal
            if source_user != user_id_target and source_user.startswith('user-'):
                
                if target_resource.startswith('device-'):
                    if target_resource not in dispositivos_compartidos:
                        dispositivos_compartidos[target_resource] = []
                    if source_user not in dispositivos_compartidos[target_resource]:
                        dispositivos_compartidos[target_resource].append(source_user)
                        
                elif target_resource.startswith('card-'):
                    if target_resource not in tarjetas_compartidas:
                        tarjetas_compartidas[target_resource] = []
                    if source_user not in tarjetas_compartidas[target_resource]:
                        tarjetas_compartidas[target_resource].append(source_user)
                        
                elif target_resource.startswith('phone-'):
                    if target_resource not in telefonos_compartidos:
                        telefonos_compartidos[target_resource] = []
                    if source_user not in telefonos_compartidos[target_resource]:
                        telefonos_compartidos[target_resource].append(source_user)
                        
                elif target_resource.startswith('person-'):
                    if target_resource not in personas_compartidas:
                        personas_compartidas[target_resource] = []
                    if source_user not in personas_compartidas[target_resource]:
                        personas_compartidas[target_resource].append(source_user)
                        
                elif target_resource.startswith('bank_account-'):
                    if target_resource not in cuentas_bancarias_compartidas:
                        cuentas_bancarias_compartidas[target_resource] = []
                    if source_user not in cuentas_bancarias_compartidas[target_resource]:
                        cuentas_bancarias_compartidas[target_resource].append(source_user)
        
        # Generar cruces detectados
        total_usuarios_conectados = set()
        
        if dispositivos_compartidos:
            usuarios_dispositivos = []
            for device, users in dispositivos_compartidos.items():
                usuarios_dispositivos.extend(users)
                total_usuarios_conectados.update(users)
            
            cruces_detectados.append({
                'tipo': 'Dispositivos compartidos',
                'cantidad': len(dispositivos_compartidos),
                'usuarios_afectados': len(set(usuarios_dispositivos)),
                'detalles': f'{len(dispositivos_compartidos)} dispositivos compartidos con {len(set(usuarios_dispositivos))} usuarios',
                'usuarios': list(set(usuarios_dispositivos))
            })
        
        if tarjetas_compartidas:
            usuarios_tarjetas = []
            for card, users in tarjetas_compartidas.items():
                usuarios_tarjetas.extend(users)
                total_usuarios_conectados.update(users)
            
            cruces_detectados.append({
                'tipo': 'Tarjetas compartidas',
                'cantidad': len(tarjetas_compartidas),
                'usuarios_afectados': len(set(usuarios_tarjetas)),
                'detalles': f'{len(tarjetas_compartidas)} tarjetas compartidas con {len(set(usuarios_tarjetas))} usuarios',
                'usuarios': list(set(usuarios_tarjetas))
            })
        
        if telefonos_compartidos:
            usuarios_telefonos = []
            for phone, users in telefonos_compartidos.items():
                usuarios_telefonos.extend(users)
                total_usuarios_conectados.update(users)
            
            cruces_detectados.append({
                'tipo': 'Teléfonos compartidos',
                'cantidad': len(telefonos_compartidos),
                'usuarios_afectados': len(set(usuarios_telefonos)),
                'detalles': f'{len(telefonos_compartidos)} teléfonos compartidos con {len(set(usuarios_telefonos))} usuarios',
                'usuarios': list(set(usuarios_telefonos))
            })
        
        if personas_compartidas:
            usuarios_personas = []
            for person, users in personas_compartidas.items():
                usuarios_personas.extend(users)
                total_usuarios_conectados.update(users)
            
            cruces_detectados.append({
                'tipo': 'Identidades compartidas',
                'cantidad': len(personas_compartidas),
                'usuarios_afectados': len(set(usuarios_personas)),
                'detalles': f'{len(personas_compartidas)} identidades compartidas con {len(set(usuarios_personas))} usuarios',
                'usuarios': list(set(usuarios_personas))
            })
        
        if cuentas_bancarias_compartidas:
            usuarios_cuentas = []
            for account, users in cuentas_bancarias_compartidas.items():
                usuarios_cuentas.extend(users)
                total_usuarios_conectados.update(users)
            
            cruces_detectados.append({
                'tipo': 'Cuentas bancarias compartidas',
                'cantidad': len(cuentas_bancarias_compartidas),
                'usuarios_afectados': len(set(usuarios_cuentas)),
                'detalles': f'{len(cuentas_bancarias_compartidas)} cuentas bancarias compartidas con {len(set(usuarios_cuentas))} usuarios',
                'usuarios': list(set(usuarios_cuentas))
            })
        
        usuarios_relacionados = [user.replace('user-', '') for user in total_usuarios_conectados]
        
        return {
            'tiene_cruces': len(cruces_detectados) > 0,
            'tipos_cruces': cruces_detectados,
            'usuarios_relacionados': usuarios_relacionados,
            'total_cruces': len(cruces_detectados),
            'total_usuarios_conectados': len(total_usuarios_conectados)
        }

    def procesar_fraud_hops(self, fraud_hops_json):
        """
        Procesar datos de hops to fraud del MCP
        """
        if not fraud_hops_json:
            return {
                'usuarios_fraudulentos_conectados': 0,
                'tipos_fraude': [],
                'tiene_fraude_conectado': False
            }
        
        fraud_confirmed = fraud_hops_json.get('fraudConfirmedUserCount', 0)
        fraud_almost = fraud_hops_json.get('fraudAlmostUserCount', 0)
        fraud_maybe = fraud_hops_json.get('fraudMaybeUserCount', 0)
        fraud_ato = fraud_hops_json.get('fraudAtoUserCount', 0)
        
        total_fraud = fraud_confirmed + fraud_almost + fraud_maybe + fraud_ato
        
        tipos_fraude = []
        if fraud_confirmed > 0:
            tipos_fraude.append(f"Fraude confirmado ({fraud_confirmed})")
        if fraud_almost > 0:
            tipos_fraude.append(f"Fraude casi confirmado ({fraud_almost})")
        if fraud_maybe > 0:
            tipos_fraude.append(f"Fraude posible ({fraud_maybe})")
        if fraud_ato > 0:
            tipos_fraude.append(f"Fraude ATO ({fraud_ato})")
        
        return {
            'usuarios_fraudulentos_conectados': total_fraud,
            'tipos_fraude': tipos_fraude,
            'tiene_fraude_conectado': total_fraud > 0,
            'detalle_por_tipo': {
                'fraud_confirmed': fraud_confirmed,
                'fraud_almost': fraud_almost,
                'fraud_maybe': fraud_maybe,
                'fraud_ato': fraud_ato
            }
        }

    async def analizar_usuario_con_mcp_real(self, user_id, subgraph_data=None, fraud_hops_data=None):
        """
        Analizar usuario usando datos MCP reales proporcionados por el usuario
        """
        
        print("🚀 ANALIZADOR CUENTA_HACKER CON MCP REAL INTEGRADO")
        print("=" * 80)
        print(f"👤 USUARIO: {user_id}")
        print(f"📅 ANÁLISIS: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 80)
        
        if not subgraph_data:
            print("⚠️  DATOS MCP REQUERIDOS:")
            print("   Para análisis completo, ejecutar en Cursor:")
            print(f"   1. @get_subgraph id=\"user-{user_id}\" relations=[\"uses_device\", \"uses_card\", \"validate_phone\", \"validate_person\", \"withdrawal_bank_account\"]")
            print(f"   2. @get_user_hops_to_fraud id={user_id} boundLevel=\"HIGH_TRUST\"")
            print()
            return None
        
        # PREREQUISITO: Verificar transacciones ATO/DTO
        print("🔍 PREREQUISITO: VERIFICAR TRANSACCIONES ATO/DTO")
        transacciones_ato = await self._verificar_transacciones_ato_dto(user_id)
        
        if transacciones_ato['cantidad'] == 0:
            print("⚠️  USUARIO NO DEBERÍA ESTAR EN ANÁLISIS")
            print("   → 0 transacciones ATO/DTO detectadas")
            print("   → Este usuario no cumple prerequisito para ser CUENTA_HACKER")
            return {
                'usuario_id': user_id,
                'decision_final': 'NO_APLICA',
                'motivo': 'Sin transacciones ATO/DTO',
                'prerequisito_cumplido': False
            }
        
        print(f"   ✅ Prerequisito cumplido: {transacciones_ato['cantidad']} transacciones ATO/DTO")
        print(f"   💰 Monto: ${transacciones_ato['monto']:,.2f}")
        
        cantidad_transacciones = transacciones_ato['cantidad']
        
        # FLUJO 1: 1 transacción ATO/DTO
        if cantidad_transacciones == 1:
            return await self._ejecutar_flujo_1(user_id, transacciones_ato)
        
        # FLUJO 2: 2+ transacciones ATO/DTO
        else:
            return await self._ejecutar_flujo_2_con_mcp(user_id, transacciones_ato, subgraph_data, fraud_hops_data)

    async def _ejecutar_flujo_2_con_mcp(self, user_id, transacciones_ato, subgraph_data, fraud_hops_data):
        """Ejecutar Flujo 2 con datos MCP reales"""
        
        print("\n📋 FLUJO 2: ANÁLISIS PARA 2+ TRANSACCIONES ATO/DTO")
        print("-" * 60)
        
        # PASO 1: Verificar antigüedad de cuenta
        print("1️⃣ EVALUANDO ANTIGÜEDAD DE CUENTA...")
        antiguedad = await self._verificar_antiguedad_cuenta(user_id)
        
        if not antiguedad['es_antigua']:
            resultado_final = await self._generar_decision_final(
                user_id, 'CUENTA_HACKER_CONFIRMADA', 
                'Cuenta nueva con múltiples transacciones ATO/DTO', 
                transacciones_ato, antiguedad
            )
            return resultado_final
        
        # PASO 2: Verificar marcas relevantes
        print("\n2️⃣ EVALUANDO MARCAS RELEVANTES...")
        marcas = await self._verificar_marcas_relevantes(user_id)
        
        if marcas['tiene_marcas']:
            resultado_final = await self._generar_decision_final(
                user_id, 'CUENTA_HACKER_DESCARTADA', 
                'Usuario con marcas relevantes (protegido)', 
                transacciones_ato, antiguedad, marcas
            )
            return resultado_final
        
        # PASO 3: Verificar cruces de riesgo con MCP REAL
        print("\n3️⃣ EVALUANDO CRUCES DE RIESGO CON MCP REAL...")
        cruces_mcp = self.procesar_subgraph_mcp(subgraph_data)
        fraud_analysis = self.procesar_fraud_hops(fraud_hops_data) if fraud_hops_data else None
        
        print(f"   📊 CRUCES DETECTADOS:")
        if cruces_mcp['tiene_cruces']:
            print(f"   ❌ {cruces_mcp['total_cruces']} tipos de cruces detectados")
            print(f"   👥 {cruces_mcp['total_usuarios_conectados']} usuarios conectados")
            for cruce in cruces_mcp['tipos_cruces']:
                print(f"     • {cruce['tipo']}: {cruce['detalles']}")
        else:
            print(f"   ✅ Sin cruces detectados")
        
        if fraud_analysis and fraud_analysis['tiene_fraude_conectado']:
            print(f"   🚨 USUARIOS FRAUDULENTOS CONECTADOS:")
            print(f"     • Total: {fraud_analysis['usuarios_fraudulentos_conectados']}")
            for tipo in fraud_analysis['tipos_fraude']:
                print(f"     • {tipo}")
            
            resultado_final = await self._generar_decision_final(
                user_id, 'CUENTA_HACKER_CONFIRMADA', 
                'Cruces con usuarios fraudulentos detectados', 
                transacciones_ato, antiguedad, marcas, cruces_mcp, fraud_analysis
            )
            return resultado_final
        
        if cruces_mcp['tiene_cruces']:
            resultado_final = await self._generar_decision_final(
                user_id, 'CUENTA_HACKER_CONFIRMADA', 
                'Cruces de riesgo detectados sin usuarios fraudulentos específicos', 
                transacciones_ato, antiguedad, marcas, cruces_mcp, fraud_analysis
            )
            return resultado_final
        
        # PASO 4: Verificar velocidad de retirada
        print("\n4️⃣ EVALUANDO VELOCIDAD DE RETIRADA...")
        velocidad = await self._verificar_velocidad_retirada(user_id)
        
        if velocidad['es_rapida']:
            resultado_final = await self._generar_decision_final(
                user_id, 'CUENTA_HACKER_CONFIRMADA', 
                'Velocidad de retirada rápida detectada', 
                transacciones_ato, antiguedad, marcas, cruces_mcp, fraud_analysis, velocidad
            )
            return resultado_final
        
        # PASO 5: Verificar contacto/desconocimiento
        print("\n5️⃣ EVALUANDO CONTACTO/DESCONOCIMIENTO...")
        contacto = await self._verificar_contacto_desconocimiento(user_id)
        
        if contacto['tiene_contacto']:
            resultado_final = await self._generar_decision_final(
                user_id, 'CUENTA_HACKER_DESCARTADA', 
                'Usuario contactó reportando desconocimiento', 
                transacciones_ato, antiguedad, marcas, cruces_mcp, fraud_analysis, velocidad, contacto
            )
            return resultado_final
        
        # Si llegamos aquí, no hay evidencia suficiente
        resultado_final = await self._generar_decision_final(
            user_id, 'CUENTA_HACKER_DESCARTADA', 
            'Sin evidencia suficiente para confirmar cuenta hacker', 
            transacciones_ato, antiguedad, marcas, cruces_mcp, fraud_analysis, velocidad, contacto
        )
        return resultado_final

    async def _verificar_transacciones_ato_dto(self, user_id):
        """Verificar transacciones ATO/DTO del usuario"""
        try:
            query = f"""
            SELECT 
                COUNT(DISTINCT operation_id) as cantidad_transacciones,
                ROUND(SUM(op_amt), 2) as monto_total
            FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
            WHERE CAST(GCA_CUST_ID AS STRING) = '{user_id}'
                AND status_id = 'A'
                AND contramarca = 0
                AND flow_type NOT IN ('PI', 'MF')
            """
            
            result = await self.operations.execute_query(query)
            
            if result and len(result) > 0:
                cantidad = result[0].get('cantidad_transacciones', 0)
                monto = result[0].get('monto_total', 0)
                
                return {
                    'cantidad': cantidad,
                    'monto': monto if monto else 0
                }
            else:
                return {'cantidad': 0, 'monto': 0}
                
        except Exception as e:
            print(f"   ❌ Error verificando transacciones: {e}")
            return {'cantidad': 0, 'monto': 0}

    async def _verificar_antiguedad_cuenta(self, user_id):
        """Verificar antigüedad de la cuenta"""
        try:
            query = f"""
            SELECT 
                CUS_RU_SINCE_DT as fecha_creacion,
                DATE_DIFF(CURRENT_DATE(), DATE(CUS_RU_SINCE_DT), DAY) as dias_antiguedad
            FROM `meli-bi-data.WHOWNER.LK_CUS_CUSTOMERS_DATA`
            WHERE CUS_CUST_ID = {user_id}
            """
            
            result = await self.operations.execute_query(query)
            
            if result and len(result) > 0:
                dias_antiguedad = result[0].get('dias_antiguedad', 0)
                fecha_creacion = result[0].get('fecha_creacion')
                
                es_antigua = dias_antiguedad >= 90  # 3 meses o más
                
                print(f"   📅 Fecha creación: {fecha_creacion}")
                print(f"   ⏰ Días de antigüedad: {dias_antiguedad}")
                print(f"   📊 ¿Es cuenta antigua? {'✅ Sí' if es_antigua else '❌ No'} (umbral: 90 días)")
                
                return {
                    'es_antigua': es_antigua,
                    'dias_antiguedad': dias_antiguedad,
                    'fecha_creacion': fecha_creacion
                }
            else:
                print(f"   ❌ No se encontraron datos de antigüedad")
                return {'es_antigua': False, 'dias_antiguedad': 0, 'fecha_creacion': None}
                
        except Exception as e:
            print(f"   ❌ Error verificando antigüedad: {e}")
            return {'es_antigua': False, 'dias_antiguedad': 0, 'fecha_creacion': None}

    async def _verificar_marcas_relevantes(self, user_id):
        """Verificar marcas relevantes del usuario"""
        try:
            query = f"""
            SELECT marcas
            FROM `meli-bi-data.SBOX_PFFINTECHATO.base_ato_escalabilidad_final`
            WHERE user_id = {user_id}
            LIMIT 1
            """
            
            result = await self.operations.execute_query(query)
            
            marcas_buscadas = [
                'big_sellers', 'comerciales', 'key_users', 'referidos', 'legales',
                'influencers', 'personalidades_prominentes', 'usuarios_test_productivos',
                'cuenta_interna', 'usuarios_tpv_high', 'protected_user', 'partners',
                'vendors', 'salary_portability', 'cartera_asesorada', 'usuarios_cbt',
                'tiendas_oficiales'
            ]
            
            if result and len(result) > 0:
                marcas = result[0].get('marcas', '')
                
                if isinstance(marcas, list):
                    marcas_texto = ' '.join(str(tag) for tag in marcas)
                else:
                    marcas_texto = str(marcas) if marcas else ''
                
                marcas_encontradas = []
                for marca in marcas_buscadas:
                    if marca.lower() in marcas_texto.lower():
                        marcas_encontradas.append(marca)
                
                tiene_marcas = len(marcas_encontradas) > 0
                
                if marcas_encontradas:
                    print(f"   ✅ Marcas relevantes encontradas: {', '.join(marcas_encontradas)}")
                else:
                    print(f"   ❌ No se encontraron marcas relevantes")
                
                return {
                    'tiene_marcas': tiene_marcas,
                    'marcas_encontradas': marcas_encontradas,
                    'marcas_completas': marcas_texto
                }
            else:
                print(f"   ❌ No se encontraron datos del usuario")
                return {'tiene_marcas': False, 'marcas_encontradas': [], 'marcas_completas': ''}
                
        except Exception as e:
            print(f"   ❌ Error verificando marcas: {e}")
            return {'tiene_marcas': False, 'marcas_encontradas': [], 'marcas_completas': ''}

    async def _verificar_velocidad_retirada(self, user_id):
        """Verificar velocidad de retirada de dinero"""
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
                DATE_DIFF(DATE(r.fecha_retiro), DATE(i.fecha_ingreso), DAY) as dias_diferencia
            FROM ingresos i
            JOIN retiros r ON DATE(r.fecha_retiro) >= DATE(i.fecha_ingreso)
                AND DATE(r.fecha_retiro) <= DATE_ADD(DATE(i.fecha_ingreso), INTERVAL 30 DAY)
            ORDER BY i.fecha_ingreso, r.fecha_retiro
            LIMIT 10
            """
            
            result = await self.operations.execute_query(query)
            
            if result and len(result) > 0:
                retiros_rapidos = [r for r in result if r.get('dias_diferencia', 999) <= 3]
                es_rapida = len(retiros_rapidos) > 0
                
                print(f"   📊 Retiros analizados: {len(result)}")
                print(f"   ⚡ Retiros rápidos (≤3 días): {len(retiros_rapidos)}")
                
                if retiros_rapidos:
                    for retiro in retiros_rapidos:
                        print(f"     • ${retiro['monto_retiro']:,.2f} retirado en {retiro['dias_diferencia']} días")
                
                print(f"   📊 ¿Velocidad rápida? {'❌ Sí' if es_rapida else '✅ No'}")
                
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

    async def _verificar_contacto_desconocimiento(self, user_id):
        """Verificar si el usuario contactó reportando desconocimiento"""
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
            
            result = await self.operations.execute_query(query)
            
            if result and len(result) > 0:
                tiene_contacto = True
                casos_contacto = len(result)
                
                print(f"   ✅ Usuario contactó reportando cuenta hacker")
                print(f"   📞 Casos de contacto: {casos_contacto}")
                
                for caso in result:
                    print(f"     • Caso {caso['case_id']}: {caso['fecha_apertura_caso']}")
                
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

    async def _ejecutar_flujo_1(self, user_id, transacciones_ato):
        """Ejecutar Flujo 1 para 1 transacción ATO/DTO"""
        print("\n📋 FLUJO 1: ANÁLISIS PARA 1 TRANSACCIÓN ATO/DTO")
        print("-" * 60)
        
        # PASO 1: Verificar antigüedad de cuenta
        print("1️⃣ EVALUANDO ANTIGÜEDAD DE CUENTA...")
        antiguedad = await self._verificar_antiguedad_cuenta(user_id)
        
        if not antiguedad['es_antigua']:
            resultado_final = await self._generar_decision_final(
                user_id, 'CUENTA_HACKER_CONFIRMADA', 
                'Cuenta nueva con transacción ATO/DTO', 
                transacciones_ato, antiguedad
            )
            return resultado_final
        
        # PASO 2: Verificar porcentaje de transacciones ATO/DTO vs total
        print("\n2️⃣ EVALUANDO % TRANSACCIONES ATO/DTO VS TOTAL...")
        porcentaje = await self._verificar_porcentaje_transacciones(user_id)
        
        if porcentaje['porcentaje_cantidad'] >= 90 or porcentaje['porcentaje_monto'] >= 90:
            resultado_final = await self._generar_decision_final(
                user_id, 'CUENTA_HACKER_CONFIRMADA', 
                'Alto porcentaje de transacciones ATO/DTO', 
                transacciones_ato, antiguedad, None, None, None, None, None, porcentaje
            )
            return resultado_final
        else:
            resultado_final = await self._generar_decision_final(
                user_id, 'CUENTA_HACKER_DESCARTADA', 
                'Bajo porcentaje de transacciones ATO/DTO en cuenta antigua', 
                transacciones_ato, antiguedad, None, None, None, None, None, porcentaje
            )
            return resultado_final

    async def _verificar_porcentaje_transacciones(self, user_id):
        """Verificar porcentaje de transacciones ATO/DTO vs total"""
        try:
            query = f"""
            WITH ato_transacciones AS (
                SELECT 
                    COUNT(DISTINCT operation_id) as cant_ato,
                    ROUND(SUM(op_amt), 2) as monto_ato
                FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
                WHERE GCA_CUST_ID = '{user_id}'
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
            
            result = await self.operations.execute_query(query)
            
            if result and len(result) > 0:
                row = result[0]
                porcentaje_cantidad = row.get('porcentaje_cantidad', 0) or 0
                porcentaje_monto = row.get('porcentaje_monto', 0) or 0
                
                print(f"   📊 Transacciones ATO/DTO: {row.get('cant_ato', 0)}")
                print(f"   📊 Transacciones totales: {row.get('cant_total', 0)}")
                print(f"   📊 % en cantidad: {porcentaje_cantidad:.1f}%")
                print(f"   📊 % en monto: {porcentaje_monto:.1f}%")
                
                return {
                    'porcentaje_cantidad': porcentaje_cantidad,
                    'porcentaje_monto': porcentaje_monto,
                    'cant_ato': row.get('cant_ato', 0),
                    'cant_total': row.get('cant_total', 0),
                    'monto_ato': row.get('monto_ato', 0),
                    'monto_total': row.get('monto_total', 0)
                }
            else:
                print(f"   ❌ No se pudieron calcular porcentajes")
                return {'porcentaje_cantidad': 0, 'porcentaje_monto': 0}
                
        except Exception as e:
            print(f"   ❌ Error calculando porcentajes: {e}")
            return {'porcentaje_cantidad': 0, 'porcentaje_monto': 0}

    async def _generar_decision_final(self, user_id, decision, motivo, transacciones_ato, 
                                     antiguedad=None, marcas=None, cruces_mcp=None, 
                                     fraud_analysis=None, velocidad=None, contacto=None, 
                                     porcentaje=None):
        """Generar decisión final y reporte completo"""
        
        print("\n" + "=" * 80)
        print("🏛️ DECISIÓN FINAL - ANÁLISIS CUENTA_HACKER")
        print("=" * 80)
        print(f"👤 USUARIO: {user_id}")
        print(f"🎯 DECISIÓN: {decision}")
        print(f"📝 MOTIVO: {motivo}")
        print("-" * 80)
        
        # Generar reporte detallado
        reporte = {
            'usuario_id': user_id,
            'decision_final': decision,
            'motivo': motivo,
            'fecha_analisis': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'prerequisito_cumplido': True,
            'transacciones_ato': transacciones_ato,
            'pasos_evaluados': []
        }
        
        if antiguedad:
            reporte['antiguedad_cuenta'] = antiguedad
            print(f"📅 ANTIGÜEDAD: {'✅ Cuenta antigua' if antiguedad['es_antigua'] else '❌ Cuenta nueva'} ({antiguedad['dias_antiguedad']} días)")
        
        if marcas:
            reporte['marcas_relevantes'] = marcas
            print(f"🏷️  MARCAS: {'✅ Protegido' if marcas['tiene_marcas'] else '❌ Sin protección'}")
        
        if cruces_mcp:
            reporte['cruces_riesgo'] = cruces_mcp
            print(f"🔗 CRUCES: {'❌ Detectados' if cruces_mcp['tiene_cruces'] else '✅ Sin cruces'} ({cruces_mcp.get('total_cruces', 0)} tipos)")
        
        if fraud_analysis:
            reporte['analisis_fraude'] = fraud_analysis
            print(f"🚨 FRAUDE: {'❌ Conectado' if fraud_analysis['tiene_fraude_conectado'] else '✅ Sin conexión'} ({fraud_analysis['usuarios_fraudulentos_conectados']} usuarios)")
        
        if velocidad:
            reporte['velocidad_retirada'] = velocidad
            print(f"⚡ VELOCIDAD: {'❌ Rápida' if velocidad['es_rapida'] else '✅ Normal'} ({velocidad['retiros_rapidos']} retiros rápidos)")
        
        if contacto:
            reporte['contacto_usuario'] = contacto
            print(f"📞 CONTACTO: {'✅ Reportó' if contacto['tiene_contacto'] else '❌ No reportó'} ({contacto['casos_contacto']} casos)")
        
        if porcentaje:
            reporte['porcentaje_transacciones'] = porcentaje
            print(f"📊 PORCENTAJE: {porcentaje['porcentaje_cantidad']:.1f}% cantidad, {porcentaje['porcentaje_monto']:.1f}% monto")
        
        print("=" * 80)
        
        return reporte

async def main():
    """Función principal para probar el analizador con datos MCP reales"""
    
    analizador = AnalizadorMCPRealIntegrado()
    
    if not await analizador.initialize():
        print("❌ Error inicializando BigQuery")
        return
    
    # Ejemplo de uso - aquí debes pegar los datos MCP reales
    user_id = "1539173000"  # Usuario que estábamos testeando para cruces
    
    print("🚀 Para análisis completo, ejecutar en Cursor:")
    print(f"1. @get_subgraph id=\"user-{user_id}\" relations=[\"uses_device\", \"uses_card\", \"validate_phone\", \"validate_person\", \"withdrawal_bank_account\"]")
    print(f"2. @get_user_hops_to_fraud id={user_id} boundLevel=\"HIGH_TRUST\"")
    print("\nLuego pegar los resultados JSON en el código...\n")
    
    # DATOS MCP REALES OBTENIDOS - aquí van los datos que copiaste
    subgraph_data = {
        "edges": [
            {"id": "uses_device-1539173000-65b5825289c234e3dc34e556", "label": "uses_device", "source": "user-1539173000", "target": "device-65b5825289c234e3dc34e556", "properties": {"start_date": "2024-01-27T22:23:15Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-669e66dc67d98773dfbdd748", "label": "uses_device", "source": "user-1539173000", "target": "device-669e66dc67d98773dfbdd748", "properties": {"start_date": "2024-07-22T14:11:48Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-66ca3b9fdf580810c64518fb", "label": "uses_device", "source": "user-1539173000", "target": "device-66ca3b9fdf580810c64518fb", "properties": {"start_date": "2024-08-24T20:00:19Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-66576149432115469e6082ec", "label": "uses_device", "source": "user-1539173000", "target": "device-66576149432115469e6082ec", "properties": {"start_date": "2024-05-29T17:10:19Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-68058598eb0163257218268c", "label": "uses_device", "source": "user-1539173000", "target": "device-68058598eb0163257218268c", "properties": {"start_date": "2025-04-20T23:55:51Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-6588d83e172f774f2c6a7a76", "label": "uses_device", "source": "user-1539173000", "target": "device-6588d83e172f774f2c6a7a76", "properties": {"start_date": "2024-01-14T04:18:15Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-6762da2e78a3fc74ac2b15e5", "label": "uses_device", "source": "user-1539173000", "target": "device-6762da2e78a3fc74ac2b15e5", "properties": {"start_date": "2025-02-17T10:56:26Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-64998566d2ed1bfd33ff8c8f", "label": "uses_device", "source": "user-1539173000", "target": "device-64998566d2ed1bfd33ff8c8f", "properties": {"start_date": "2023-11-10T13:42:17Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-675d72f7040a659c1c22e19f", "label": "uses_device", "source": "user-1539173000", "target": "device-675d72f7040a659c1c22e19f", "properties": {"start_date": "2025-02-17T14:28:47Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-685c66686bd6b1cb9012bfe3", "label": "uses_device", "source": "user-1539173000", "target": "device-685c66686bd6b1cb9012bfe3", "properties": {"start_date": "2025-06-25T21:14:31Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-6596aadce08d0096897a75ae", "label": "uses_device", "source": "user-1539173000", "target": "device-6596aadce08d0096897a75ae", "properties": {"start_date": "2024-01-04T12:56:38Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-6787bea46d3396c62e8a333e", "label": "uses_device", "source": "user-1539173000", "target": "device-6787bea46d3396c62e8a333e", "properties": {"start_date": "2025-01-15T13:58:24Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-675f5a93b55028b5f7fa6d0c", "label": "uses_device", "source": "user-1539173000", "target": "device-675f5a93b55028b5f7fa6d0c", "properties": {"start_date": "2025-04-17T13:06:54Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-67ef39bce19a8b80a1bcf9de", "label": "uses_device", "source": "user-1539173000", "target": "device-67ef39bce19a8b80a1bcf9de", "properties": {"start_date": "2025-04-04T01:46:28Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-678992c9fb423f05aa032594", "label": "uses_device", "source": "user-1539173000", "target": "device-678992c9fb423f05aa032594", "properties": {"start_date": "2025-01-16T23:14:17Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-665f260c1194a96a2fd05dca", "label": "uses_device", "source": "user-1539173000", "target": "device-665f260c1194a96a2fd05dca", "properties": {"start_date": "2024-06-04T14:35:31Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-685c606142d3887c484bcadd", "label": "uses_device", "source": "user-1539173000", "target": "device-685c606142d3887c484bcadd", "properties": {"start_date": "2025-06-25T20:49:48Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-685c64135573f902288ae4d5", "label": "uses_device", "source": "user-1539173000", "target": "device-685c64135573f902288ae4d5", "properties": {"start_date": "2025-06-25T21:04:07Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-6808418c42b222671423b877", "label": "uses_device", "source": "user-1539173000", "target": "device-6808418c42b222671423b877", "properties": {"start_date": "2025-04-23T01:29:24Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-654b711879036fb9ef8f5aa5", "label": "uses_device", "source": "user-1539173000", "target": "device-654b711879036fb9ef8f5aa5", "properties": {"start_date": "2023-11-08T11:29:28Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-654e3f33acf6134e478b6688", "label": "uses_device", "source": "user-1539173000", "target": "device-654e3f33acf6134e478b6688", "properties": {"start_date": "2023-11-10T14:34:01Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-6851a69ea161ac4c079c8ce9", "label": "uses_device", "source": "user-1539173000", "target": "device-6851a69ea161ac4c079c8ce9", "properties": {"start_date": "2025-06-17T17:41:35Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_guest_card-1539173000-RPXWSDFLHTDONUJVWBEMJARERZVXCJXRINIUBOAA", "label": "uses_guest_card", "source": "user-1539173000", "target": "card-RPXWSDFLHTDONUJVWBEMJARERZVXCJXRINIUBOAA", "properties": {"start_date": "2025-08-03T00:46:23Z"}, "bound_level": "LOW_TRUST"},
            {"id": "uses_card-1539173000-AHXZOHNEYGSCMZIECIJXOOKWEHBGUPNHNZHHSEMM", "label": "uses_card", "source": "user-1539173000", "target": "card-AHXZOHNEYGSCMZIECIJXOOKWEHBGUPNHNZHHSEMM", "properties": {"start_date": "2024-02-07T16:03:02Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "validate_phone-1539173000-c2dc0f2e71691ed95cb84a9830b93b23", "label": "validate_phone", "source": "user-1539173000", "target": "phone-c2dc0f2e71691ed95cb84a9830b93b23", "properties": {"start_date": "2023-11-08T11:26:42Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "declare_phone-1539173000-c2dc0f2e71691ed95cb84a9830b93b23", "label": "declare_phone", "source": "user-1539173000", "target": "phone-c2dc0f2e71691ed95cb84a9830b93b23", "properties": {"start_date": "2023-11-08T11:26:42Z"}, "bound_level": "LOW_TRUST"},
            {"id": "declare_person-1539173000-fbd41a3a5836c9747801dcf4706a53cb", "label": "declare_person", "source": "user-1539173000", "target": "person-fbd41a3a5836c9747801dcf4706a53cb", "properties": {"start_date": "2023-11-08T11:31:31Z"}, "bound_level": "LOW_TRUST"},
            {"id": "validate_person-1539173000-fbd41a3a5836c9747801dcf4706a53cb", "label": "validate_person", "source": "user-1539173000", "target": "person-fbd41a3a5836c9747801dcf4706a53cb", "properties": {"start_date": "2023-11-08T11:31:31Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "withdrawal_bank_account-1539173000-cc8410f1199e1c8e9c480693d7f01415", "label": "withdrawal_bank_account", "source": "user-1539173000", "target": "bank_account-cc8410f1199e1c8e9c480693d7f01415", "properties": {"start_date": "2024-03-02T04:21:56Z"}, "bound_level": "UNKNOWN"},
            {"id": "uses_device-1752460179-6787bea46d3396c62e8a333e", "label": "uses_device", "source": "user-1752460179", "target": "device-6787bea46d3396c62e8a333e", "properties": {"start_date": "2025-08-02T16:50:27Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_guest_device-1488191641-64998566d2ed1bfd33ff8c8f", "label": "uses_guest_device", "source": "user-1488191641", "target": "device-64998566d2ed1bfd33ff8c8f", "properties": {"start_date": "2023-09-29T18:49:10Z"}, "bound_level": "LOW_TRUST"},
            {"id": "uses_device-345557826-64998566d2ed1bfd33ff8c8f", "label": "uses_device", "source": "user-345557826", "target": "device-64998566d2ed1bfd33ff8c8f", "properties": {"start_date": "2023-11-10T14:43:58Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1412215331-675f5a93b55028b5f7fa6d0c", "label": "uses_device", "source": "user-1412215331", "target": "device-675f5a93b55028b5f7fa6d0c", "properties": {"start_date": "2024-12-15T22:41:41Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-345557826-675f5a93b55028b5f7fa6d0c", "label": "uses_device", "source": "user-345557826", "target": "device-675f5a93b55028b5f7fa6d0c", "properties": {"start_date": "2025-04-24T15:03:56Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-810346226-675f5a93b55028b5f7fa6d0c", "label": "uses_device", "source": "user-810346226", "target": "device-675f5a93b55028b5f7fa6d0c", "properties": {"start_date": "2025-06-06T23:12:59Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_card-1412215331-AHXZOHNEYGSCMZIECIJXOOKWEHBGUPNHNZHHSEMM", "label": "uses_card", "source": "user-1412215331", "target": "card-AHXZOHNEYGSCMZIECIJXOOKWEHBGUPNHNZHHSEMM", "properties": {"start_date": "2025-06-09T23:08:07Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "validate_phone-1488191641-c2dc0f2e71691ed95cb84a9830b93b23", "label": "validate_phone", "source": "user-1488191641", "target": "phone-c2dc0f2e71691ed95cb84a9830b93b23", "properties": {"start_date": "2023-09-23T20:58:06Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "declare_phone-1488191641-c2dc0f2e71691ed95cb84a9830b93b23", "label": "declare_phone", "source": "user-1488191641", "target": "phone-c2dc0f2e71691ed95cb84a9830b93b23", "properties": {"start_date": "2023-09-23T20:58:06Z"}, "bound_level": "LOW_TRUST"},
            {"id": "validate_person-345557826-fbd41a3a5836c9747801dcf4706a53cb", "label": "validate_person", "source": "user-345557826", "target": "person-fbd41a3a5836c9747801dcf4706a53cb", "properties": {"start_date": "2023-11-10T15:54:59Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "declare_person-345557826-fbd41a3a5836c9747801dcf4706a53cb", "label": "declare_person", "source": "user-345557826", "target": "person-fbd41a3a5836c9747801dcf4706a53cb", "properties": {"start_date": "2022-05-08T01:40:54Z"}, "bound_level": "LOW_TRUST"}
        ],
        "nodes": [
            {"id": "device-675d72f7040a659c1c22e19f", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-6787bea46d3396c62e8a333e", "label": "device", "adjacent_edges_count": 2, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-685c64135573f902288ae4d5", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-6851a69ea161ac4c079c8ce9", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-685c606142d3887c484bcadd", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-6808418c42b222671423b877", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-6762da2e78a3fc74ac2b15e5", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-66576149432115469e6082ec", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-64998566d2ed1bfd33ff8c8f", "label": "device", "adjacent_edges_count": 3, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-675f5a93b55028b5f7fa6d0c", "label": "device", "adjacent_edges_count": 4, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-6588d83e172f774f2c6a7a76", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-6596aadce08d0096897a75ae", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "user-1539173000", "label": "user", "adjacent_edges_count": 96, "properties": {"start_date": "2023-11-06T22:37:24Z", "last_updated": "2025-08-06T13:07:59Z", "is_driver_user": True}},
            {"id": "device-665f260c1194a96a2fd05dca", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-654b711879036fb9ef8f5aa5", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-68058598eb0163257218268c", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-669e66dc67d98773dfbdd748", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-65b5825289c234e3dc34e556", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-654e3f33acf6134e478b6688", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-67ef39bce19a8b80a1bcf9de", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-678992c9fb423f05aa032594", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-685c66686bd6b1cb9012bfe3", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-66ca3b9fdf580810c64518fb", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "card-AHXZOHNEYGSCMZIECIJXOOKWEHBGUPNHNZHHSEMM", "label": "card", "adjacent_edges_count": 2, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "card-RPXWSDFLHTDONUJVWBEMJARERZVXCJXRINIUBOAA", "label": "card", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "phone-c2dc0f2e71691ed95cb84a9830b93b23", "label": "phone", "adjacent_edges_count": 4, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "person-fbd41a3a5836c9747801dcf4706a53cb", "label": "person", "adjacent_edges_count": 4, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "bank_account-cc8410f1199e1c8e9c480693d7f01415", "label": "bank_account", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "user-1752460179", "label": "user", "adjacent_edges_count": 13, "properties": {"start_date": "2024-04-02T21:40:50Z", "last_updated": "2025-08-05T21:25:08Z", "is_driver_user": False}},
            {"id": "user-1488191641", "label": "user", "adjacent_edges_count": 13, "properties": {"start_date": "2023-09-23T20:50:37Z", "last_updated": "2024-10-12T18:08:49Z", "is_driver_user": False}},
            {"id": "user-345557826", "label": "user", "adjacent_edges_count": 52, "properties": {"start_date": "2021-02-27T16:10:22Z", "last_updated": "2025-08-05T20:31:22Z", "is_driver_user": False}},
            {"id": "user-1412215331", "label": "user", "adjacent_edges_count": 31, "properties": {"start_date": "2023-07-02T08:03:58Z", "last_updated": "2025-08-05T18:07:34Z", "is_driver_user": False}},
            {"id": "user-810346226", "label": "user", "adjacent_edges_count": 56, "properties": {"start_date": "2021-08-19T15:15:39Z", "last_updated": "2025-07-07T16:34:34Z", "is_driver_user": False}}
        ]
    }
    
    fraud_hops_data = {
        "boundLevel": "HIGH_TRUST",
        "hopsLimit": None,
        "userFound": True,
        "userCount": 5,
        "fraudConfirmedUserCount": 0,
        "fraudAlmostUserCount": 0,
        "fraudMaybeUserCount": 0,
        "fraudAtoUserCount": 0
    }
    
    # Ejecutar análisis con datos MCP reales
    resultado = await analizador.analizar_usuario_con_mcp_real(user_id, subgraph_data, fraud_hops_data)
    
    if resultado:
        print("\n🎯 ANÁLISIS COMPLETADO")
        print(f"   Decisión: {resultado['decision_final']}")
        print(f"   Motivo: {resultado['motivo']}")

if __name__ == "__main__":
    asyncio.run(main())