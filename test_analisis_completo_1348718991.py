#!/usr/bin/env python3
"""
Test completo del análisis de cuenta hacker para usuario 1348718991
usando MCP real para cruces de riesgo
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from analizador_mcp_final import AnalizadorCuentaHacker

async def test_usuario_completo():
    """Test completo con usuario conocido"""
    
    user_id = "1348718991"
    print(f"\n🎯 INICIANDO ANÁLISIS COMPLETO PARA USUARIO: {user_id}")
    print("="*80)
    
    analizador = AnalizadorCuentaHacker()
    
    try:
        # Ejecutar análisis completo
        resultado = await analizador.analizar_usuario(user_id)
        
        print(f"\n📊 RESULTADO FINAL:")
        print("="*50)
        print(f"Usuario: {resultado['user_id']}")
        print(f"Decisión: {resultado['decision']}")
        print(f"Puntaje: {resultado['puntaje']}")
        print(f"Razón: {resultado['razon']}")
        
        print(f"\n📋 DETALLES DEL ANÁLISIS:")
        print("-"*40)
        for paso, detalle in resultado['detalles'].items():
            print(f"{paso}: {detalle}")
            
        print(f"\n📈 FLUJO EJECUTADO:")
        print("-"*40)
        for i, paso in enumerate(resultado['flujo_ejecutado'], 1):
            print(f"{i}. {paso}")
            
        # Si se usó MCP, mostrar detalles
        if 'mcp_data' in resultado:
            print(f"\n🔗 DATOS MCP OBTENIDOS:")
            print("-"*40)
            mcp_data = resultado['mcp_data']
            if 'subgraph' in mcp_data:
                print(f"Subgraph: {len(mcp_data['subgraph'])} conexiones")
            if 'fraud_hops' in mcp_data:
                print(f"Fraud hops: {mcp_data['fraud_hops']}")
                
    except Exception as e:
        print(f"❌ ERROR en análisis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_usuario_completo())




















