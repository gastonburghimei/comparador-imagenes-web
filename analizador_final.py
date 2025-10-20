#!/usr/bin/env python3
"""
ANALIZADOR FINAL CUENTA_HACKER - Versión que funciona
⚡ ANÁLISIS INSTANTÁNEO para casos conocidos (< 1 segundo)
"""
import sys

def analizar_usuario(user_id):
    # Base de casos analizados (expandida con más usuarios)
    casos = {
        '376268877': {
            'antiguedad': 2305,
            'fuentes_ato': 0,
            'ventas': 9,
            'recibido': 20,
            'vendido': 2671
        },
        '1135104330': {
            'antiguedad': 1154,
            'fuentes_ato': 1,
            'ventas': 0,
            'recibido': 4142,
            'vendido': 0
        },
        '1815915392': {
            'antiguedad': 332,
            'fuentes_ato': 0,
            'ventas': 1,
            'recibido': 0,
            'vendido': 70
        },
        '28859516': {
            'antiguedad': 5460,
            'fuentes_ato': 1,
            'ventas': 0,
            'recibido': 1000,
            'vendido': 0
        },
        # Casos adicionales que se pueden agregar
        '1348718991': {
            'antiguedad': 205,  # Estimado de restricciones
            'fuentes_ato': 0,   # A determinar por MCP
            'ventas': 0,        # A determinar por MCP
            'recibido': 0,      # A determinar por MCP
            'vendido': 0,       # A determinar por MCP
            'requiere_mcp': True
        },
        '468290404': {
            'antiguedad': 0,    # A determinar por MCP
            'fuentes_ato': 0,   # A determinar por MCP
            'ventas': 0,        # A determinar por MCP
            'recibido': 0,      # A determinar por MCP
            'vendido': 0,       # A determinar por MCP
            'requiere_mcp': True
        },
        '375845668': {
            'antiguedad': 0,    # A determinar por MCP
            'fuentes_ato': 0,   # A determinar por MCP
            'ventas': 0,        # A determinar por MCP
            'recibido': 0,      # A determinar por MCP
            'vendido': 0,       # A determinar por MCP
            'requiere_mcp': True
        }
    }
    
    print("🎯 ANALIZADOR FINAL CUENTA_HACKER - MERCADO PAGO")
    print("=" * 60)
    print(f"👤 USUARIO: {user_id}")
    print(f"📅 ANÁLISIS: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    if user_id not in casos:
        print("")
        print("❌ ERROR: Usuario no encontrado en base de conocimiento")
        print("")
        print("🔧 SOLUCIONES:")
        print("   1. Usar analizador_mcp_integrado.py para usuarios nuevos")
        print("   2. Agregar usuario a la base de casos conocidos")
        print("")
        print("📋 USUARIOS CONOCIDOS:")
        for uid in casos.keys():
            if not casos[uid].get('requiere_mcp', False):
                print(f"   ✅ {uid}")
        print("")
        print("📋 USUARIOS PENDIENTES (requieren MCP):")
        for uid in casos.keys():
            if casos[uid].get('requiere_mcp', False):
                print(f"   🔄 {uid}")
        return
    
    datos = casos[user_id]
    
    # Verificar si requiere análisis MCP
    if datos.get('requiere_mcp', False):
        print("")
        print("🔄 USUARIO REQUIERE ANÁLISIS MCP")
        print("   Este usuario necesita queries de BigQuery para completar el análisis")
        print("")
        print("🚀 EJECUTAR:")
        print(f"   python3 analizador_mcp_integrado.py {user_id}")
        print("")
        return
    
    # Calcular score de riesgo
    score = 0
    factores_riesgo = []
    factores_positivos = []
    
    print("🧮 CÁLCULO DE SCORE DE RIESGO")
    print("-" * 40)
    
    # Factores de riesgo (+)
    if datos['antiguedad'] < 30:
        score += 40
        factores_riesgo.append(f"Cuenta nueva ({datos['antiguedad']} días)")
        print(f"   🔴 +40 pts: Cuenta nueva")
    
    if datos['fuentes_ato'] > 0:
        score += 30
        factores_riesgo.append(f"Recibe dinero ATO/DTO ({datos['fuentes_ato']} fuentes)")
        print(f"   🔴 +30 pts: Fuentes ATO/DTO ({datos['fuentes_ato']})")
    
    if datos['ventas'] == 0:
        score += 25
        factores_riesgo.append("Sin actividad comercial")
        print(f"   🔴 +25 pts: Sin ventas")
    
    if datos['recibido'] > 1000:
        score += 20
        factores_riesgo.append(f"Alto monto recibido (${datos['recibido']:,})")
        print(f"   🔴 +20 pts: Monto alto")
    
    # Patrón money mule (+50 BONUS)
    if datos['recibido'] > 1000 and datos['ventas'] == 0 and datos['fuentes_ato'] > 0:
        score += 50
        factores_riesgo.append("PATRÓN MONEY MULE detectado")
        print(f"   🚨 +50 pts: BONUS MONEY MULE")
    
    # Factores positivos (-)
    if datos['antiguedad'] > 365:
        score -= 10
        factores_positivos.append(f"Cuenta antigua ({datos['antiguedad']} días)")
        print(f"   🟢 -10 pts: Cuenta antigua")
    
    if datos['ventas'] > 5:
        score -= 20
        factores_positivos.append(f"Actividad comercial regular ({datos['ventas']} ventas)")
        print(f"   🟢 -20 pts: Múltiples ventas")
    
    if datos['vendido'] > 1000:
        score -= 15
        factores_positivos.append(f"Alto volumen comercial (${datos['vendido']:,})")
        print(f"   🟢 -15 pts: Alto monto vendido")
    
    # Asegurar score no negativo
    score = max(0, score)
    
    # Tomar decisión según criterios MP
    if score >= 70:
        decision = "MANTENER INHABILITACIÓN"
        simbolo = "🔴"
        confianza = "ALTA"
    elif score >= 40:
        decision = "MANTENER INHABILITACIÓN"
        simbolo = "🟠"
        confianza = "MEDIA"
    elif score <= 15:
        decision = "REHABILITAR"
        simbolo = "🟢"
        confianza = "ALTA"
    elif score <= 30:
        decision = "REHABILITAR"
        simbolo = "🟢"
        confianza = "MEDIA"
    else:
        decision = "REVISAR MANUALMENTE"
        simbolo = "🟡"
        confianza = "BAJA"
    
    # Mostrar resultado
    print("")
    print("📊 RESULTADO FINAL")
    print("=" * 40)
    print(f"{simbolo} **DECISIÓN:** {decision}")
    print(f"📊 **NIVEL DE CONFIANZA:** {confianza}")
    print(f"🧮 **SCORE DE RIESGO:** {score}/100")
    print("")
    print("📋 **MÉTRICAS ESPECÍFICAS:**")
    print(f"   - Antigüedad cuenta: {datos['antiguedad']} días")
    print(f"   - Fuentes ATO/DTO: {datos['fuentes_ato']}")
    print(f"   - Ventas realizadas: {datos['ventas']}")
    print(f"   - Monto recibido: ${datos['recibido']:,} USD")
    print(f"   - Monto vendido: ${datos['vendido']:,} USD")
    print("")
    print("🔴 **FACTORES DE RIESGO:**")
    if factores_riesgo:
        for factor in factores_riesgo:
            print(f"   • {factor}")
    else:
        print("   • Ninguno")
    print("")
    print("🟢 **FACTORES POSITIVOS:**")
    if factores_positivos:
        for factor in factores_positivos:
            print(f"   • {factor}")
    else:
        print("   • Ninguno")
    print("")
    print("💡 **JUSTIFICACIÓN:**")
    
    if decision == "MANTENER INHABILITACIÓN":
        if datos['recibido'] > 1000 and datos['ventas'] == 0:
            justificacion = "MONEY MULE: Recibe dinero sin actividad comercial"
        elif datos['fuentes_ato'] > 0:
            justificacion = "Recibe dinero de fuentes fraudulentas (ATO/DTO)"
        elif datos['antiguedad'] < 30:
            justificacion = "Cuenta nueva con actividad sospechosa"
        else:
            justificacion = "Múltiples factores de riesgo detectados"
    elif decision == "REHABILITAR":
        if datos['ventas'] > 0 and datos['fuentes_ato'] == 0:
            justificacion = "Perfil comercial legítimo sin ATO/DTO"
        elif datos['antiguedad'] > 1000:
            justificacion = "Cuenta antigua sin patrones sospechosos"
        else:
            justificacion = "Factores positivos superan riesgos"
    else:
        justificacion = "Caso complejo que requiere revisión manual"
    
    print(f"   → {justificacion}")
    print("")
    print("=" * 60)

def main():
    if len(sys.argv) != 2:
        print("🎯 ANALIZADOR FINAL CUENTA_HACKER")
        print("")
        print("⚡ ANÁLISIS INSTANTÁNEO para casos conocidos")
        print("")
        print("📝 USO:")
        print("   python3 analizador_final.py <USER_ID>")
        print("")
        print("✅ EJEMPLOS CONOCIDOS:")
        print("   python3 analizador_final.py 376268877   # REHABILITAR - Perfil comercial")
        print("   python3 analizador_final.py 1135104330  # MANTENER - Money mule")
        print("   python3 analizador_final.py 1815915392  # REHABILITAR - Comerciante nuevo")
        print("   python3 analizador_final.py 28859516    # MANTENER - Cuenta comprometida")
        print("")
        print("🔄 CASOS PENDIENTES (requieren MCP):")
        print("   python3 analizador_mcp_integrado.py 1348718991")
        print("   python3 analizador_mcp_integrado.py 468290404")
        print("   python3 analizador_mcp_integrado.py 375845668")
        print("")
        return
    
    user_id = sys.argv[1]
    analizar_usuario(user_id)

if __name__ == "__main__":
    main()