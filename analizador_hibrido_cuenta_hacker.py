#!/usr/bin/env python3
"""
🎯 ANALIZADOR HÍBRIDO CUENTA_HACKER - MERCADO PAGO
Combina datos conocidos + queries ampliadas para nuevos usuarios
"""

import asyncio
import sys
from datetime import datetime
from mcp_bigquery_setup import MCPBigQueryBasicOperations

class AnalizadorHibridoCuentaHacker:
    """
    Analizador que usa datos conocidos + queries para casos nuevos
    """
    
    def __init__(self):
        self.operations = None
        
        # Base de casos ya analizados (del analizador que funciona)
        self.casos_conocidos = {
            '376268877': {
                'antiguedad': 2305,
                'fuentes_ato': 0,
                'ventas': 9,
                'recibido': 20,
                'vendido': 2671,
                'origen': 'conocido'
            },
            '1135104330': {
                'antiguedad': 1154,
                'fuentes_ato': 1,
                'ventas': 0,
                'recibido': 4142,
                'vendido': 0,
                'origen': 'conocido'
            },
            '1815915392': {
                'antiguedad': 332,
                'fuentes_ato': 0,
                'ventas': 1,
                'recibido': 0,
                'vendido': 70,
                'origen': 'conocido'
            },
            '28859516': {
                'antiguedad': 5460,
                'fuentes_ato': 1,
                'ventas': 0,
                'recibido': 1000,
                'vendido': 0,
                'origen': 'conocido'
            }
        }
        
    async def initialize(self):
        """Inicializar conexión a BigQuery"""
        self.operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
        await self.operations.initialize()
        
    async def analizar_usuario(self, user_id):
        """
        Análisis completo: datos conocidos + queries ampliadas
        """
        
        print("🎯 ANÁLISIS HÍBRIDO CUENTA_HACKER - MERCADO PAGO")
        print("=" * 80)
        print(f"👤 USUARIO: {user_id}")
        print(f"📅 FECHA ANÁLISIS: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 80)
        
        # Verificar si tenemos datos conocidos
        if user_id in self.casos_conocidos:
            print("📋 USANDO DATOS CONOCIDOS (análisis previo)")
            datos = self.casos_conocidos[user_id]
            datos['origen'] = 'conocido'
        else:
            print("🔍 EJECUTANDO QUERIES AMPLIADAS (usuario nuevo)")
            datos = await self._query_usuario_nuevo(user_id)
            datos['origen'] = 'query'
            
        # Calcular score y decisión
        analisis = self._calcular_score_y_decision(datos)
        
        # Generar reporte
        reporte = self._generar_reporte(user_id, datos, analisis)
        
        print("\n" + "=" * 80)
        print("📋 REPORTE FINAL")
        print("=" * 80)
        print(reporte)
        
        return {
            'user_id': user_id,
            'datos': datos,
            'analisis': analisis,
            'reporte': reporte
        }
    
    async def _query_usuario_nuevo(self, user_id):
        """Queries ampliadas para usuarios no conocidos"""
        
        datos = {
            'antiguedad': 0,
            'fuentes_ato': 0,
            'ventas': 0,
            'recibido': 0,
            'vendido': 0
        }
        
        # 1. ANTIGÜEDAD - Query más amplia
        print("\n📊 PASO 1: BUSCAR ANTIGÜEDAD (query ampliada)")
        antiguedad = await self._query_antiguedad_ampliada(user_id)
        datos['antiguedad'] = antiguedad
        
        # 2. TRANSACCIONES ATO/DTO - Query más amplia
        print("\n💰 PASO 2: BUSCAR TRANSACCIONES ATO/DTO (query ampliada)")
        ato_datos = await self._query_ato_dto_ampliada(user_id)
        datos['fuentes_ato'] = ato_datos['fuentes']
        datos['recibido'] = ato_datos['monto']
        
        # 3. ACTIVIDAD COMERCIAL - Query más amplia
        print("\n🛒 PASO 3: BUSCAR ACTIVIDAD COMERCIAL (query ampliada)")
        actividad = await self._query_actividad_ampliada(user_id)
        datos['ventas'] = actividad['ventas']
        datos['vendido'] = actividad['monto']
        
        return datos
    
    async def _query_antiguedad_ampliada(self, user_id):
        """Query ampliada para antigüedad - sin filtros restrictivos"""
        
        # Query 1: Buscar en restricciones (sin filtros de fecha/tipo específicos)
        query_restricciones = f"""
        SELECT 
            USER_ID,
            SENTENCE_DATETIME,
            INFRACTION_TYPE,
            DATE_DIFF(CURRENT_DATE(), DATE(SENTENCE_DATETIME), DAY) as dias_desde_restriccion
        FROM `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW`
        WHERE USER_ID = {user_id}
            AND UPPER(INFRACTION_TYPE) LIKE '%HACKER%'
        ORDER BY SENTENCE_DATETIME DESC
        LIMIT 1
        """
        
        result = await self.operations.execute_query(query_restricciones, 5)
        
        if result["status"] == "success" and result["result"]["rows"]:
            data = result["result"]["rows"][0]
            dias_restriccion = data.get('dias_desde_restriccion', 0)
            print(f"   ✅ Encontrado en restricciones: {data.get('INFRACTION_TYPE')}")
            print(f"   📅 Sentencia: {data.get('SENTENCE_DATETIME')}")
            print(f"   🕐 Días desde restricción: {dias_restriccion}")
            
            # Estimar antigüedad (asumiendo cuenta tenía al menos 30 días al ser restringida)
            antiguedad_estimada = dias_restriccion + 30
            print(f"   📊 Antigüedad estimada: {antiguedad_estimada} días")
            return antiguedad_estimada
        
        # Query 2: Buscar directamente en datos de clientes
        query_clientes = f"""
        SELECT 
            CUS_CUST_ID,
            CUS_RU_SINCE_DT,
            DATE_DIFF(CURRENT_DATE(), DATE(CUS_RU_SINCE_DT), DAY) as dias_antiguedad
        FROM `meli-bi-data.WHOWNER.LK_CUS_CUSTOMERS_DATA`
        WHERE CUS_CUST_ID = {user_id}
        """
        
        result2 = await self.operations.execute_query(query_clientes, 5)
        
        if result2["status"] == "success" and result2["result"]["rows"]:
            data = result2["result"]["rows"][0]
            antiguedad = data.get('dias_antiguedad', 0)
            print(f"   ✅ Encontrado en datos clientes")
            print(f"   📅 Fecha creación: {data.get('CUS_RU_SINCE_DT')}")
            print(f"   🕐 Antigüedad real: {antiguedad} días")
            return antiguedad
        
        print(f"   ❌ No se encontró información de antigüedad")
        return 0
    
    async def _query_ato_dto_ampliada(self, user_id):
        """Query ampliada para ATO/DTO - sin filtros restrictivos de fecha"""
        
        query = f"""
        SELECT 
            COUNT(DISTINCT sco.GTWT_TRANSACTION_ID) as total_transacciones,
            COALESCE(SUM(sco.PAY_TRANSACTION_DOL_AMT), 0) as monto_total,
            COUNT(DISTINCT CASE WHEN ato_bq.cust_id IS NOT NULL THEN sco.GTWT_TRANSACTION_ID END) as tx_ato_dto,
            COUNT(DISTINCT ato_bq.cust_id) as fuentes_ato_dto
        FROM `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT` sco
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato_bq
            ON CAST(sco.CUS_CUST_ID_BUY AS STRING) = CAST(ato_bq.cust_id AS STRING)
            AND ato_bq.contramarca = 0
        WHERE CAST(sco.CUS_CUST_ID_SEL AS STRING) = '{user_id}'
            AND sco.CREATION_DATE >= '2024-01-01'  -- Amplio rango de fechas
        """
        
        result = await self.operations.execute_query(query, 5)
        
        if result["status"] == "success" and result["result"]["rows"]:
            data = result["result"]["rows"][0]
            fuentes = data.get('fuentes_ato_dto', 0)
            monto = data.get('monto_total', 0)
            
            print(f"   💰 Total transacciones: {data.get('total_transacciones', 0)}")
            print(f"   🚨 Fuentes ATO/DTO: {fuentes}")
            print(f"   💵 Monto recibido: ${monto:,.2f} USD")
            
            return {'fuentes': fuentes, 'monto': monto}
        
        print(f"   ❌ No se encontraron transacciones ATO/DTO")
        return {'fuentes': 0, 'monto': 0}
    
    async def _query_actividad_ampliada(self, user_id):
        """Query ampliada para actividad comercial"""
        
        query = f"""
        SELECT 
            COUNT(DISTINCT sco.GTWT_TRANSACTION_ID) as ventas_unicas,
            COALESCE(SUM(CASE WHEN sco.PAY_TRANSACTION_DOL_AMT > 0 THEN sco.PAY_TRANSACTION_DOL_AMT ELSE 0 END), 0) as monto_vendido
        FROM `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT` sco
        WHERE CAST(sco.CUS_CUST_ID_SEL AS STRING) = '{user_id}'
            AND sco.CREATION_DATE >= '2024-01-01'  -- Amplio rango
            AND sco.PAY_TRANSACTION_DOL_AMT > 0
        """
        
        result = await self.operations.execute_query(query, 5)
        
        if result["status"] == "success" and result["result"]["rows"]:
            data = result["result"]["rows"][0]
            ventas = data.get('ventas_unicas', 0)
            monto = data.get('monto_vendido', 0)
            
            print(f"   🛒 Ventas únicas: {ventas}")
            print(f"   💰 Monto vendido: ${monto:,.2f} USD")
            
            return {'ventas': ventas, 'monto': monto}
        
        print(f"   ❌ No se encontró actividad comercial")
        return {'ventas': 0, 'monto': 0}
    
    def _calcular_score_y_decision(self, datos):
        """Calcular score usando el algoritmo que funciona"""
        
        print(f"\n🧮 CÁLCULO DE SCORE (origen: {datos.get('origen', 'query')})")
        print("-" * 60)
        
        score = 0
        factores_riesgo = []
        factores_positivos = []
        
        # FACTORES DE RIESGO (+)
        # Cuenta nueva (<30 días): +40 puntos
        if datos['antiguedad'] < 30:
            score += 40
            factores_riesgo.append(f"Cuenta nueva ({datos['antiguedad']} días)")
            print(f"   🔴 +40 puntos: Cuenta nueva")
        
        # Fuentes ATO/DTO > 0: +30 puntos
        if datos['fuentes_ato'] > 0:
            score += 30
            factores_riesgo.append(f"Recibe dinero ATO/DTO ({datos['fuentes_ato']} fuentes)")
            print(f"   🔴 +30 puntos: Fuentes ATO/DTO ({datos['fuentes_ato']})")
        
        # Sin ventas: +25 puntos
        if datos['ventas'] == 0:
            score += 25
            factores_riesgo.append("Sin actividad comercial")
            print(f"   🔴 +25 puntos: Sin ventas")
        
        # Monto recibido > $1,000: +20 puntos
        if datos['recibido'] > 1000:
            score += 20
            factores_riesgo.append(f"Alto monto recibido (${datos['recibido']:,.2f})")
            print(f"   🔴 +20 puntos: Monto alto (${datos['recibido']:,.2f})")
        
        # BONUS Money Mule: +50 puntos
        if (datos['recibido'] > 1000 and 
            datos['ventas'] == 0 and 
            datos['fuentes_ato'] > 0):
            score += 50
            factores_riesgo.append("PATRÓN MONEY MULE detectado")
            print(f"   🚨 +50 puntos: BONUS MONEY MULE")
        
        # FACTORES POSITIVOS (-)
        # Antigüedad > 365 días: -10 puntos
        if datos['antiguedad'] > 365:
            score -= 10
            factores_positivos.append(f"Cuenta antigua ({datos['antiguedad']} días)")
            print(f"   🟢 -10 puntos: Cuenta antigua")
        
        # Ventas > 5: -20 puntos
        if datos['ventas'] > 5:
            score -= 20
            factores_positivos.append(f"Actividad comercial regular ({datos['ventas']} ventas)")
            print(f"   🟢 -20 puntos: Múltiples ventas ({datos['ventas']})")
        
        # Monto vendido > $1,000: -15 puntos
        if datos['vendido'] > 1000:
            score -= 15
            factores_positivos.append(f"Alto volumen comercial (${datos['vendido']:,.2f})")
            print(f"   🟢 -15 puntos: Alto monto vendido")
        
        # Asegurar que el score no sea negativo
        score = max(0, score)
        
        # DETERMINAR DECISIÓN (usando lógica del analizador que funciona)
        if score >= 70:
            decision = "MANTENER INHABILITACIÓN"
            confianza = "ALTA"
            simbolo = "🔴"
        elif score >= 40:
            decision = "MANTENER INHABILITACIÓN"
            confianza = "MEDIA"
            simbolo = "🟠"
        elif score <= 15:
            decision = "REHABILITAR"
            confianza = "ALTA"
            simbolo = "🟢"
        elif score <= 30:
            decision = "REHABILITAR"
            confianza = "MEDIA"
            simbolo = "🟢"
        else:
            decision = "REVISAR MANUALMENTE"
            confianza = "BAJA"
            simbolo = "🟡"
        
        print(f"\n   📊 SCORE FINAL: {score}/100")
        print(f"   {simbolo} DECISIÓN: {decision}")
        print(f"   📊 CONFIANZA: {confianza}")
        
        return {
            'score': score,
            'decision': decision,
            'confianza': confianza,
            'simbolo': simbolo,
            'factores_riesgo': factores_riesgo,
            'factores_positivos': factores_positivos
        }
    
    def _generar_reporte(self, user_id, datos, analisis):
        """Generar reporte en formato oficial MP"""
        
        # Justificación basada en el analizador que funciona
        if analisis['decision'] == "MANTENER INHABILITACIÓN":
            if datos['recibido'] > 1000 and datos['ventas'] == 0:
                justificacion = "MONEY MULE: Recibe dinero sin actividad comercial"
            elif datos['fuentes_ato'] > 0:
                justificacion = "Recibe dinero de fuentes fraudulentas (ATO/DTO)"
            elif datos['antiguedad'] < 30:
                justificacion = "Cuenta nueva con actividad sospechosa"
            else:
                justificacion = "Múltiples factores de riesgo detectados"
        elif analisis['decision'] == "REHABILITAR":
            if datos['ventas'] > 0 and datos['fuentes_ato'] == 0:
                justificacion = "Perfil comercial legítimo sin ATO/DTO"
            elif datos['antiguedad'] > 1000:
                justificacion = "Cuenta antigua sin patrones sospechosos"
            else:
                justificacion = "Factores positivos superan riesgos"
        else:
            justificacion = "Caso complejo que requiere revisión manual"
        
        reporte = f"""
{analisis['simbolo']} **DECISIÓN:** {analisis['decision']}

**NIVEL DE CONFIANZA:** {analisis['confianza']}

**SCORE DE RIESGO:** {analisis['score']}/100

**MÉTRICAS ESPECÍFICAS:**
- Antigüedad cuenta: {datos['antiguedad']} días
- Fuentes ATO/DTO: {datos['fuentes_ato']}
- Ventas realizadas: {datos['ventas']}
- Monto recibido: ${datos['recibido']:,.2f} USD
- Monto vendido: ${datos['vendido']:,.2f} USD

**FACTORES DE RIESGO:** {', '.join(analisis['factores_riesgo']) if analisis['factores_riesgo'] else 'Ninguno'}

**FACTORES POSITIVOS:** {', '.join(analisis['factores_positivos']) if analisis['factores_positivos'] else 'Ninguno'}

**JUSTIFICACIÓN:** {justificacion}

**ORIGEN DATOS:** {datos.get('origen', 'query')}
        """
        
        return reporte.strip()

async def main():
    """Función principal"""
    
    if len(sys.argv) != 2:
        print("🎯 ANALIZADOR HÍBRIDO CUENTA_HACKER")
        print("")
        print("Uso: python analizador_hibrido_cuenta_hacker.py <USER_ID>")
        print("")
        print("Usuarios conocidos (análisis rápido):")
        print("  376268877   # REHABILITAR - Perfil comercial")
        print("  1135104330  # MANTENER - Money mule")
        print("  1815915392  # REHABILITAR - Comerciante nuevo")
        print("  28859516    # MANTENER - Cuenta comprometida")
        print("")
        print("Usuarios nuevos (requieren queries):")
        print("  1348718991, 468290404, 375845668")
        print("")
        return
    
    user_id = sys.argv[1]
    
    analizador = AnalizadorHibridoCuentaHacker()
    await analizador.initialize()
    
    try:
        resultado = await analizador.analizar_usuario(user_id)
        print(f"\n✅ ANÁLISIS COMPLETADO PARA USUARIO {user_id}")
        
    except Exception as e:
        print(f"❌ Error analizando usuario {user_id}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())