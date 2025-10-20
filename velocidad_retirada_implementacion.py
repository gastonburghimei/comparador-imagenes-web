#!/usr/bin/env python3
"""
🚀 IMPLEMENTACIÓN VELOCIDAD DE RETIRADA
Análisis de tiempo entre ingreso y egreso de dinero
Umbral: 6 horas = RÁPIDA = CUENTA HACKER
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from bigquery_config import create_default_connection

class VelocidadRetiradaAnalyzer:
    """Analizador de velocidad de retirada de dinero"""
    
    def __init__(self):
        self.bq_client = create_default_connection()
    
    def analizar_velocidad_usuario(self, user_id, fecha_limite_infraccion=None):
        """
        Analiza la velocidad de retirada para un usuario específico
        
        Args:
            user_id: ID del usuario a analizar
            fecha_limite_infraccion: Fecha límite para considerar movimientos (antes de infracción)
        
        Returns:
            dict con análisis de velocidad
        """
        print(f"\n💸 ANALIZANDO VELOCIDAD DE RETIRADA - Usuario: {user_id}")
        print("-" * 60)
        
        try:
            # 1. Obtener ingresos de dinero
            ingresos = self._obtener_ingresos_dinero(user_id, fecha_limite_infraccion)
            
            if not ingresos:
                print("❌ No se encontraron ingresos de dinero")
                return {
                    'tiene_datos': False,
                    'es_rapida': False,
                    'velocidad_promedio_horas': None,
                    'detalles': 'Sin ingresos detectados'
                }
            
            print(f"💰 Encontrados {len(ingresos)} ingresos de dinero")
            
            # 2. Para cada ingreso, buscar retiros posteriores
            velocidades = []
            
            for ingreso in ingresos[:5]:  # Limitar a 5 ingresos más recientes
                retiros = self._obtener_retiros_posteriores(user_id, ingreso, fecha_limite_infraccion)
                
                for retiro in retiros:
                    velocidad_horas = self._calcular_velocidad_horas(ingreso, retiro)
                    if velocidad_horas is not None:
                        velocidades.append({
                            'ingreso_fecha': ingreso['fecha'],
                            'ingreso_monto': ingreso['monto'],
                            'retiro_fecha': retiro['fecha'],
                            'retiro_monto': retiro['monto'],
                            'velocidad_horas': velocidad_horas,
                            'es_rapida': velocidad_horas <= 6,
                            'tipo_retiro': retiro['tipo']
                        })
            
            if not velocidades:
                print("❌ No se encontraron retiros posteriores a ingresos")
                return {
                    'tiene_datos': True,
                    'es_rapida': False,
                    'velocidad_promedio_horas': None,
                    'detalles': f'Ingresos sin retiros detectados ({len(ingresos)} ingresos)'
                }
            
            # 3. Analizar resultados
            resultado = self._analizar_velocidades(velocidades)
            
            # 4. Mostrar resumen
            self._mostrar_resumen_velocidades(user_id, velocidades, resultado)
            
            return resultado
            
        except Exception as e:
            print(f"❌ Error analizando velocidad: {e}")
            return {
                'tiene_datos': False,
                'es_rapida': False,
                'velocidad_promedio_horas': None,
                'detalles': f'Error: {str(e)}'
            }
    
    def _obtener_ingresos_dinero(self, user_id, fecha_limite=None):
        """Obtiene ingresos de dinero del usuario desde BT_SCO_ORIGIN_REPORT"""
        
        # Filtro de fecha si se proporciona
        filtro_fecha = ""
        if fecha_limite:
            filtro_fecha = f"AND PAY_CREATED_DT <= '{fecha_limite}'"
        
        # Query para ingresos en BT_SCO_ORIGIN_REPORT (origen de dinero)
        query_ingresos = f"""
        SELECT 
            PAY_CREATED_DT as fecha,
            PAY_TRANSACTION_DOL_AMT as monto,
            PAY_OPERATION_TYPE_ID as tipo_operacion,
            PAY_PAYMENT_ID as transaction_id,
            PAY_STATUS_ID as estado
        FROM `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT`
        WHERE 
            CUS_CUST_ID_SEL = {user_id}
            AND PAY_TRANSACTION_DOL_AMT > 0
            AND PAY_STATUS_ID = 'approved'
            {filtro_fecha}
            AND PAY_CREATED_DT >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        ORDER BY PAY_CREATED_DT DESC
        LIMIT 10
        """
        
        try:
            result = self.bq_client.execute_query(query_ingresos)
            
            if hasattr(result, 'iloc') and len(result) > 0:
                ingresos = []
                for _, row in result.iterrows():
                    ingresos.append({
                        'fecha': row['fecha'],
                        'monto': float(row['monto']),
                        'tipo': row['tipo_operacion'],
                        'id': row['transaction_id']
                    })
                return ingresos
            
            return []
            
        except Exception as e:
            print(f"⚠️ Error obteniendo ingresos: {e}")
            return []
    
    def _obtener_retiros_posteriores(self, user_id, ingreso, fecha_limite=None):
        """Obtiene retiros posteriores a un ingreso específico"""
        
        fecha_ingreso = ingreso['fecha']
        
        # Buscar retiros hasta 7 días después del ingreso
        fecha_fin_busqueda = pd.to_datetime(fecha_ingreso) + timedelta(days=7)
        
        if fecha_limite and pd.to_datetime(fecha_limite) < fecha_fin_busqueda:
            fecha_fin_busqueda = pd.to_datetime(fecha_limite)
        
        retiros = []
        
        # 1. Buscar en BT_MP_WITHDRAWALS (retiros/withdrawals)
        retiros.extend(self._buscar_withdrawals(user_id, fecha_ingreso, fecha_fin_busqueda))
        
        # 2. Buscar en BT_MP_PAYOUTS (retiros a cuentas bancarias)
        retiros.extend(self._buscar_retiros_payouts(user_id, fecha_ingreso, fecha_fin_busqueda))
        
        # 3. Buscar compras en BT_SCO_ORIGIN_REPORT (salidas de dinero)
        retiros.extend(self._buscar_compras_sco(user_id, fecha_ingreso, fecha_fin_busqueda))
        
        return retiros
    
    def _buscar_retiros_payments(self, user_id, fecha_inicio, fecha_fin):
        """Busca retiros en BT_MP_PAY_PAYMENTS"""
        
        query = f"""
        SELECT 
            pay_move_date as fecha,
            ABS(PAY_TRANSACTION_DOL_AMT) as monto,
            PAY_OPERATION_TYPE_ID as tipo,
            PAY_PAYMENT_ID as payment_id
        FROM `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
        WHERE 
            CUS_CUST_ID_SEL = {user_id}
            AND pay_move_date > '{fecha_inicio}'
            AND pay_move_date <= '{fecha_fin}'
            AND pay_status_id = 'approved'
            AND PAY_TRANSACTION_DOL_AMT > 0
            AND tpv_flag = 1
        ORDER BY pay_move_date ASC
        LIMIT 5
        """
        
        try:
            result = self.bq_client.execute_query(query)
            
            retiros = []
            if hasattr(result, 'iloc') and len(result) > 0:
                for _, row in result.iterrows():
                    retiros.append({
                        'fecha': row['fecha'],
                        'monto': float(row['monto']),
                        'tipo': f"PAYMENT_{row['tipo']}",
                        'id': row['payment_id'],
                        'fuente': 'BT_MP_PAY_PAYMENTS'
                    })
            
            return retiros
            
        except Exception as e:
            print(f"⚠️ Error en payments: {e}")
            return []
    
    def _buscar_retiros_payouts(self, user_id, fecha_inicio, fecha_fin):
        """Busca retiros en BT_MP_PAYOUTS"""
        
        query = f"""
        SELECT 
            PAYOUT_DATE_CREATED as fecha,
            PAYOUT_AMT as monto,
            PAYOUT_TYPE as tipo,
            PAYOUT_ID as payout_id
        FROM `meli-bi-data.WHOWNER.BT_MP_PAYOUTS`
        WHERE 
            PAYOUT_CUST_ID = {user_id}
            AND PAYOUT_DATE_CREATED > '{fecha_inicio}'
            AND PAYOUT_DATE_CREATED <= '{fecha_fin}'
            AND PAYOUT_STATUS = 'APPROVED'
            AND PAYOUT_AMT > 0
        ORDER BY PAYOUT_DATE_CREATED ASC
        LIMIT 5
        """
        
        try:
            result = self.bq_client.execute_query(query)
            
            retiros = []
            if hasattr(result, 'iloc') and len(result) > 0:
                for _, row in result.iterrows():
                    retiros.append({
                        'fecha': row['fecha'],
                        'monto': float(row['monto']),
                        'tipo': f"PAYOUT_{row['tipo']}",
                        'id': row['payout_id'],
                        'fuente': 'BT_MP_PAYOUTS'
                    })
            
            return retiros
            
        except Exception as e:
            print(f"⚠️ Error en payouts: {e}")
            return []
    
    def _buscar_withdrawals(self, user_id, fecha_inicio, fecha_fin):
        """Busca retiros en BT_MP_WITHDRAWALS"""
        
        query = f"""
        SELECT 
            WDR_DATE_CREATED as fecha,
            WDR_AMT as monto,
            WDR_TYPE as tipo,
            WDR_ID as withdrawal_id,
            WDR_STATUS as estado
        FROM `meli-bi-data.WHOWNER.BT_MP_WITHDRAWALS`
        WHERE 
            WDR_CUST_ID = {user_id}
            AND WDR_DATE_CREATED > '{fecha_inicio}'
            AND WDR_DATE_CREATED <= '{fecha_fin}'
            AND WDR_AMT > 0
            AND WDR_STATUS = 'approved'
        ORDER BY WDR_DATE_CREATED ASC
        LIMIT 5
        """
        
        try:
            result = self.bq_client.execute_query(query)
            
            retiros = []
            if hasattr(result, 'iloc') and len(result) > 0:
                for _, row in result.iterrows():
                    retiros.append({
                        'fecha': row['fecha'],
                        'monto': float(row['monto']),
                        'tipo': f"WITHDRAWAL_{row['tipo']}",
                        'id': row['withdrawal_id'],
                        'fuente': 'BT_MP_WITHDRAWALS'
                    })
            
            return retiros
            
        except Exception as e:
            print(f"⚠️ Error en withdrawals: {e}")
            return []
    
    def _buscar_compras_sco(self, user_id, fecha_inicio, fecha_fin):
        """Busca compras/salidas en BT_SCO_ORIGIN_REPORT"""
        
        query = f"""
        SELECT 
            PAY_CREATED_DT as fecha,
            ABS(PAY_TRANSACTION_DOL_AMT) as monto,
            PAY_OPERATION_TYPE_ID as tipo,
            PAY_PAYMENT_ID as transaction_id
        FROM `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT`
        WHERE 
            CUS_CUST_ID_SEL = {user_id}
            AND PAY_CREATED_DT > '{fecha_inicio}'
            AND PAY_CREATED_DT <= '{fecha_fin}'
            AND PAY_TRANSACTION_DOL_AMT < 0
            AND PAY_STATUS_ID = 'approved'
        ORDER BY PAY_CREATED_DT ASC
        LIMIT 5
        """
        
        try:
            result = self.bq_client.execute_query(query)
            
            retiros = []
            if hasattr(result, 'iloc') and len(result) > 0:
                for _, row in result.iterrows():
                    retiros.append({
                        'fecha': row['fecha'],
                        'monto': float(row['monto']),
                        'tipo': f"COMPRA_{row['tipo']}",
                        'id': row['transaction_id'],
                        'fuente': 'BT_SCO_ORIGIN_REPORT'
                    })
            
            return retiros
            
        except Exception as e:
            print(f"⚠️ Error en compras SCO: {e}")
            return []
    
    def _calcular_velocidad_horas(self, ingreso, retiro):
        """Calcula velocidad en horas entre ingreso y retiro"""
        
        try:
            fecha_ingreso = pd.to_datetime(ingreso['fecha'])
            fecha_retiro = pd.to_datetime(retiro['fecha'])
            
            if fecha_retiro <= fecha_ingreso:
                return None
            
            diferencia = fecha_retiro - fecha_ingreso
            horas = diferencia.total_seconds() / 3600
            
            return round(horas, 2)
            
        except Exception as e:
            print(f"⚠️ Error calculando velocidad: {e}")
            return None
    
    def _analizar_velocidades(self, velocidades):
        """Analiza conjunto de velocidades y determina resultado"""
        
        if not velocidades:
            return {
                'tiene_datos': False,
                'es_rapida': False,
                'velocidad_promedio_horas': None,
                'detalles': 'Sin datos de velocidad'
            }
        
        # Calcular estadísticas
        horas_velocidades = [v['velocidad_horas'] for v in velocidades]
        velocidad_promedio = sum(horas_velocidades) / len(horas_velocidades)
        velocidad_minima = min(horas_velocidades)
        
        # Contar retiros rápidos (≤ 6 horas)
        retiros_rapidos = [v for v in velocidades if v['es_rapida']]
        porcentaje_rapidos = (len(retiros_rapidos) / len(velocidades)) * 100
        
        # Determinar si es cuenta rápida
        # Criterio: al menos 1 retiro ≤ 6 horas O promedio ≤ 12 horas
        es_rapida = (len(retiros_rapidos) > 0) or (velocidad_promedio <= 12)
        
        return {
            'tiene_datos': True,
            'es_rapida': es_rapida,
            'velocidad_promedio_horas': round(velocidad_promedio, 2),
            'velocidad_minima_horas': round(velocidad_minima, 2),
            'total_operaciones': len(velocidades),
            'retiros_rapidos': len(retiros_rapidos),
            'porcentaje_rapidos': round(porcentaje_rapidos, 1),
            'criterio_aplicado': f"{'Retiros rápidos detectados' if len(retiros_rapidos) > 0 else 'Promedio bajo'}" if es_rapida else "Velocidad normal",
            'detalles': f"{len(velocidades)} operaciones analizadas",
            'operaciones_detalle': velocidades[:3]  # Primeras 3 para debug
        }
    
    def _mostrar_resumen_velocidades(self, user_id, velocidades, resultado):
        """Muestra resumen del análisis de velocidades"""
        
        print(f"\n📊 RESUMEN VELOCIDAD - Usuario {user_id}")
        print("-" * 50)
        print(f"🎯 Resultado: {'⚡ RÁPIDA' if resultado['es_rapida'] else '🐌 NORMAL'}")
        print(f"📈 Promedio: {resultado.get('velocidad_promedio_horas', 'N/A')} horas")
        print(f"🏃 Mínima: {resultado.get('velocidad_minima_horas', 'N/A')} horas")
        print(f"🔢 Operaciones: {resultado.get('total_operaciones', 0)}")
        print(f"⚡ Retiros ≤6h: {resultado.get('retiros_rapidos', 0)} ({resultado.get('porcentaje_rapidos', 0)}%)")
        print(f"📋 Criterio: {resultado.get('criterio_aplicado', 'N/A')}")
        
        if velocidades:
            print(f"\n📝 EJEMPLOS DE VELOCIDADES:")
            for i, v in enumerate(velocidades[:3], 1):
                estado = "⚡ RÁPIDA" if v['es_rapida'] else "🐌 NORMAL"
                print(f"   {i}. {v['velocidad_horas']}h - ${v['ingreso_monto']:,.0f} → ${v['retiro_monto']:,.0f} ({estado})")

# Test function
async def test_velocidad_usuario(user_id):
    """Test de velocidad para un usuario específico"""
    
    print(f"\n🎯 TEST VELOCIDAD DE RETIRADA - Usuario: {user_id}")
    print("=" * 80)
    
    analyzer = VelocidadRetiradaAnalyzer()
    resultado = analyzer.analizar_velocidad_usuario(user_id)
    
    print(f"\n✅ ANÁLISIS COMPLETADO")
    return resultado

if __name__ == "__main__":
    # Test con usuario conocido
    asyncio.run(test_velocidad_usuario("760785507"))
