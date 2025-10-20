#!/usr/bin/env python3
"""
🚀 VELOCIDAD DE RETIRADA SIMPLIFICADA
Solo usando BT_SCO_ORIGIN_REPORT para ingresos y egresos
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from bigquery_config import create_default_connection

class VelocidadSimplificada:
    """Análisis simplificado de velocidad usando solo BT_SCO_ORIGIN_REPORT"""
    
    def __init__(self):
        self.bq_client = create_default_connection()
    
    def analizar_usuario(self, user_id):
        """Analiza velocidad de retirada para un usuario"""
        
        print(f"\n💸 ANÁLISIS VELOCIDAD SIMPLIFICADO - Usuario: {user_id}")
        print("-" * 60)
        
        try:
            # Obtener todas las transacciones del usuario
            transacciones = self._obtener_transacciones(user_id)
            
            if not transacciones:
                return {
                    'tiene_datos': False,
                    'es_rapida': False,
                    'detalles': 'Sin transacciones'
                }
            
            print(f"📊 Encontradas {len(transacciones)} transacciones")
            
            # Analizar velocidades entre transacciones positivas y negativas
            velocidades = self._calcular_velocidades(transacciones)
            
            if not velocidades:
                return {
                    'tiene_datos': True,
                    'es_rapida': False,
                    'detalles': f'{len(transacciones)} transacciones sin velocidades calculables'
                }
            
            # Analizar resultados
            resultado = self._evaluar_velocidades(velocidades)
            self._mostrar_resumen(user_id, velocidades, resultado)
            
            return resultado
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return {
                'tiene_datos': False,
                'es_rapida': False,
                'detalles': f'Error: {str(e)}'
            }
    
    def _obtener_transacciones(self, user_id):
        """Obtiene todas las transacciones del usuario"""
        
        query = f"""
        SELECT 
            PAY_CREATED_DT as fecha,
            PAY_TRANSACTION_DOL_AMT as monto,
            PAY_OPERATION_TYPE_ID as tipo,
            PAY_PAYMENT_ID as payment_id,
            PAY_STATUS_ID as estado,
            CASE 
                WHEN PAY_TRANSACTION_DOL_AMT > 0 THEN 'INGRESO'
                WHEN PAY_TRANSACTION_DOL_AMT < 0 THEN 'EGRESO'
                ELSE 'NEUTRO'
            END as direccion
        FROM `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT`
        WHERE 
            CUS_CUST_ID_SEL = {user_id}
            AND PAY_STATUS_ID = 'approved'
            AND ABS(PAY_TRANSACTION_DOL_AMT) > 0
            AND PAY_CREATED_DT >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        ORDER BY PAY_CREATED_DT ASC
        LIMIT 50
        """
        
        try:
            result = self.bq_client.execute_query(query)
            
            transacciones = []
            if hasattr(result, 'iloc') and len(result) > 0:
                for _, row in result.iterrows():
                    transacciones.append({
                        'fecha': row['fecha'],
                        'monto': float(row['monto']),
                        'tipo': row['tipo'],
                        'payment_id': row['payment_id'],
                        'direccion': row['direccion']
                    })
            
            return transacciones
            
        except Exception as e:
            print(f"⚠️ Error obteniendo transacciones: {e}")
            return []
    
    def _calcular_velocidades(self, transacciones):
        """Calcula velocidades entre ingresos y egresos"""
        
        velocidades = []
        
        # Separar ingresos y egresos
        ingresos = [t for t in transacciones if t['direccion'] == 'INGRESO']
        egresos = [t for t in transacciones if t['direccion'] == 'EGRESO']
        
        print(f"📈 Ingresos: {len(ingresos)}, Egresos: {len(egresos)}")
        
        # Para cada ingreso, buscar egresos posteriores en ventana de 7 días
        for ingreso in ingresos:
            fecha_ingreso = pd.to_datetime(ingreso['fecha'])
            fecha_limite = fecha_ingreso + timedelta(days=7)
            
            for egreso in egresos:
                fecha_egreso = pd.to_datetime(egreso['fecha'])
                
                # Solo considerar egresos posteriores al ingreso y dentro de 7 días
                if fecha_ingreso < fecha_egreso <= fecha_limite:
                    
                    horas_diferencia = (fecha_egreso - fecha_ingreso).total_seconds() / 3600
                    
                    velocidades.append({
                        'ingreso_fecha': ingreso['fecha'],
                        'ingreso_monto': abs(ingreso['monto']),
                        'egreso_fecha': egreso['fecha'],
                        'egreso_monto': abs(egreso['monto']),
                        'horas': round(horas_diferencia, 2),
                        'es_rapida': horas_diferencia <= 6,
                        'ingreso_tipo': ingreso['tipo'],
                        'egreso_tipo': egreso['tipo']
                    })
        
        # Ordenar por velocidad (más rápida primero)
        velocidades.sort(key=lambda x: x['horas'])
        
        return velocidades
    
    def _evaluar_velocidades(self, velocidades):
        """Evalúa si el usuario tiene velocidad rápida"""
        
        if not velocidades:
            return {
                'tiene_datos': False,
                'es_rapida': False,
                'detalles': 'Sin velocidades calculables'
            }
        
        # Estadísticas
        horas_todas = [v['horas'] for v in velocidades]
        velocidad_minima = min(horas_todas)
        velocidad_promedio = sum(horas_todas) / len(horas_todas)
        
        # Contar retiros rápidos (≤ 6 horas)
        retiros_rapidos = [v for v in velocidades if v['es_rapida']]
        porcentaje_rapidos = (len(retiros_rapidos) / len(velocidades)) * 100
        
        # Criterio de decisión
        es_rapida = len(retiros_rapidos) > 0 or velocidad_promedio <= 12
        
        return {
            'tiene_datos': True,
            'es_rapida': es_rapida,
            'velocidad_minima_horas': round(velocidad_minima, 2),
            'velocidad_promedio_horas': round(velocidad_promedio, 2),
            'total_operaciones': len(velocidades),
            'retiros_rapidos': len(retiros_rapidos),
            'porcentaje_rapidos': round(porcentaje_rapidos, 1),
            'criterio': f"{'Retiros ≤6h detectados' if len(retiros_rapidos) > 0 else 'Promedio bajo'}" if es_rapida else "Velocidad normal",
            'detalles': f"{len(velocidades)} pares ingreso→egreso analizados"
        }
    
    def _mostrar_resumen(self, user_id, velocidades, resultado):
        """Muestra resumen del análisis"""
        
        print(f"\n📊 RESUMEN VELOCIDAD - Usuario {user_id}")
        print("-" * 50)
        print(f"🎯 Resultado: {'⚡ RÁPIDA' if resultado['es_rapida'] else '🐌 NORMAL'}")
        print(f"⚡ Mínima: {resultado.get('velocidad_minima_horas', 'N/A')}h")
        print(f"📈 Promedio: {resultado.get('velocidad_promedio_horas', 'N/A')}h")
        print(f"🔢 Pares analizados: {resultado.get('total_operaciones', 0)}")
        print(f"⚡ Retiros ≤6h: {resultado.get('retiros_rapidos', 0)} ({resultado.get('porcentaje_rapidos', 0)}%)")
        print(f"📋 Criterio: {resultado.get('criterio', 'N/A')}")
        
        if velocidades:
            print(f"\n📝 TOP 3 VELOCIDADES MÁS RÁPIDAS:")
            for i, v in enumerate(velocidades[:3], 1):
                estado = "⚡ RÁPIDA" if v['es_rapida'] else "🐌 NORMAL"
                print(f"   {i}. {v['horas']}h - ${v['ingreso_monto']:,.0f} → ${v['egreso_monto']:,.0f} ({estado})")

async def test_velocidad_simplificada(user_id):
    """Test de velocidad simplificada"""
    
    print(f"\n🎯 TEST VELOCIDAD SIMPLIFICADA - Usuario: {user_id}")
    print("=" * 80)
    
    analyzer = VelocidadSimplificada()
    resultado = analyzer.analizar_usuario(user_id)
    
    print(f"\n✅ ANÁLISIS COMPLETADO")
    return resultado

if __name__ == "__main__":
    # Test con usuario 760785507
    asyncio.run(test_velocidad_simplificada("760785507"))




















