#!/usr/bin/env python3
"""
🎯 ANÁLISIS USUARIO 1348718991 - Usuario con cruces conocidos
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from analizador_con_mcp_integrado import AnalizadorConMCPIntegrado

async def main():
    user_id = "1348718991"
    print(f"\n🎯 ANALIZANDO USUARIO CON CRUCES CONOCIDOS: {user_id}")
    print("="*80)
    print("📋 Este usuario debería tener cruces con:")
    print("   • 1879313474 (conexión device)")
    print("   • 749782133 (conexión identity)")
    print("="*80)
    
    analizador = AnalizadorConMCPIntegrado()
    
    try:
        resultado = analizador.analizar_usuario(user_id)
        
        print(f"\n📊 RESULTADO FINAL:")
        print("="*60)
        print(f"Usuario: {user_id}")
        print(f"Decisión: {resultado.get('decision', 'N/A')}")
        print(f"Puntaje: {resultado.get('puntaje', 'N/A')}")
        print(f"Razón: {resultado.get('razon', 'N/A')}")
        
        # Mostrar detalles específicos de cruces
        if 'detalles' in resultado:
            detalles = resultado['detalles']
            if 'cruces_riesgo' in detalles:
                cruces = detalles['cruces_riesgo']
                print(f"\n🔗 DETALLES DE CRUCES:")
                print(f"   • Tiene cruces: {cruces.get('tiene_cruces', 'N/A')}")
                print(f"   • Tipos: {cruces.get('tipos_cruces', [])}")
                print(f"   • Descripción: {cruces.get('descripcion', 'N/A')}")
                
                if 'detalles_mcp' in cruces:
                    mcp_details = cruces['detalles_mcp']
                    print(f"\n📡 DATOS MCP DETALLADOS:")
                    
                    subgraph = mcp_details.get('subgraph', {})
                    print(f"   🕸️ Subgrafo:")
                    print(f"      • Conexiones: {subgraph.get('total_connections', 0)}")
                    print(f"      • Sospechosas: {subgraph.get('has_suspicious_connections', False)}")
                    
                    fraud = mcp_details.get('fraud_analysis', {})
                    print(f"   🚨 Análisis fraude:")
                    print(f"      • Conexiones fraudulentas: {fraud.get('fraud_connections', 0)}")
                    print(f"      • Saltos a fraude: {fraud.get('hops_to_fraud', 'N/A')}")
                    print(f"      • Nivel riesgo: {fraud.get('risk_level', 'N/A')}")
        
        print(f"\n✅ ANÁLISIS COMPLETADO")
        return resultado
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    asyncio.run(main())




















