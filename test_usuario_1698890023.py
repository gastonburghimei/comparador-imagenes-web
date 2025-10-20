#!/usr/bin/env python3
"""
🎯 ANÁLISIS USUARIO 1698890023
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from analizador_con_mcp_integrado import AnalizadorConMCPIntegrado

async def main():
    user_id = "1698890023"
    print(f"\n🎯 ANALIZANDO USUARIO: {user_id}")
    print("="*60)
    
    analizador = AnalizadorConMCPIntegrado()
    
    try:
        resultado = analizador.analizar_usuario(user_id)
        
        print(f"\n📊 RESULTADO FINAL:")
        print("="*60)
        print(f"Usuario: {user_id}")
        print(f"Decisión: {resultado.get('decision', 'N/A')}")
        print(f"Puntaje: {resultado.get('puntaje', 'N/A')}")
        print(f"Razón: {resultado.get('razon', 'N/A')}")
        
        # Mostrar detalles del flujo ejecutado
        if 'detalles' in resultado:
            detalles = resultado['detalles']
            print(f"\n📋 DETALLES DEL ANÁLISIS:")
            print("-"*40)
            
            # Transacciones
            if 'transacciones_ato' in detalles:
                trans = detalles['transacciones_ato']
                print(f"📊 Transacciones ATO/DTO: {trans.get('cantidad', 'N/A')}")
                if trans.get('cantidad', 0) > 0:
                    print(f"   • Monto marcado: ${trans.get('monto_marcado', 0):,.2f}")
            
            # Antigüedad
            if 'antiguedad_cuenta' in detalles:
                ant = detalles['antiguedad_cuenta']
                print(f"📅 Antigüedad: {ant.get('dias', 'N/A')} días ({'Nueva' if ant.get('es_nueva', False) else 'Antigua'})")
            
            # Marcas relevantes
            if 'marcas_relevantes' in detalles:
                marcas = detalles['marcas_relevantes']
                print(f"🏷️ Marcas relevantes: {'SÍ' if marcas.get('tiene_marcas', False) else 'NO'}")
                if marcas.get('marcas_encontradas'):
                    print(f"   • Marcas: {marcas['marcas_encontradas']}")
            
            # Cruces de riesgo
            if 'cruces_riesgo' in detalles:
                cruces = detalles['cruces_riesgo']
                print(f"🔗 Cruces de riesgo: {'SÍ' if cruces.get('tiene_cruces', False) else 'NO'}")
                if cruces.get('tiene_cruces'):
                    print(f"   • Tipos: {cruces.get('tipos_cruces', [])}")
                    print(f"   • Descripción: {cruces.get('descripcion', '')}")
                    
                    # Mostrar datos MCP si están disponibles
                    if 'detalles_mcp' in cruces:
                        mcp_data = cruces['detalles_mcp']
                        print(f"   📡 Datos MCP:")
                        
                        subgraph = mcp_data.get('subgraph', {})
                        print(f"      • Conexiones totales: {subgraph.get('total_connections', 0)}")
                        
                        fraud = mcp_data.get('fraud_analysis', {})
                        print(f"      • Conexiones fraudulentas: {fraud.get('fraud_connections', 0)}")
                        print(f"      • Nivel de riesgo: {fraud.get('risk_level', 'N/A')}")
        
        # Mostrar flujo ejecutado
        if 'flujo_ejecutado' in resultado:
            print(f"\n📈 FLUJO EJECUTADO:")
            print("-"*40)
            for i, paso in enumerate(resultado['flujo_ejecutado'], 1):
                print(f"{i}. {paso}")
        
        print(f"\n✅ ANÁLISIS COMPLETADO")
        return resultado
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    asyncio.run(main())




















