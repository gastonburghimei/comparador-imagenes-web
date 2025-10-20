#!/usr/bin/env python3
"""
🔍 VERIFICADOR DE CRUCES REALES
Usando Account Relations MCP para verificar cruces entre usuarios específicos
"""

import asyncio
import sys

async def verificar_cruces_usuario():
    """Verificar cruces usando las herramientas MCP disponibles"""
    
    print("🔍 VERIFICADOR DE CRUCES REALES - ACCOUNT RELATIONS MCP")
    print("=" * 80)
    
    # Usuarios reportados con cruces
    usuario_principal = "1348718991"
    usuarios_relacionados = ["1879313474", "749782133"]
    
    print(f"👤 USUARIO PRINCIPAL: {usuario_principal}")
    print(f"🔗 USUARIOS RELACIONADOS: {', '.join(usuarios_relacionados)}")
    print("-" * 80)
    
    try:
        # Usar las herramientas MCP disponibles
        print("🚀 USANDO HERRAMIENTAS ACCOUNT RELATIONS MCP:")
        print()
        
        # 1. say_hello para obtener contexto
        print("1️⃣ 👋 say_hello - Obtener contexto")
        print("   📋 Descripción: Obtener contexto de la problemática")
        # Nota: Esta herramienta estará disponible en Cursor con el MCP configurado
        print("   ⚠️  Esperando herramienta MCP en Cursor...")
        print()
        
        # 2. get_subgraph para analizar conexiones
        print("2️⃣ 🕸️ get_subgraph - Analizar conexiones")
        print(f"   📋 Usuario: {usuario_principal}")
        print("   📋 Parámetros sugeridos:")
        print("     • node_id: '1348718991'")
        print("     • depth: 2")
        print("     • include_types: ['identity', 'device', 'address', 'bank_account']")
        print("   ⚠️  Esperando herramienta MCP en Cursor...")
        print()
        
        # 3. get_hops_to_fraud para verificar usuarios fraudulentos
        print("3️⃣ 🚨 get_hops_to_fraud - Verificar conexiones fraudulentas")
        print(f"   📋 Usuario: {usuario_principal}")
        print("   📋 Parámetros sugeridos:")
        print("     • user_id: '1348718991'")
        print("     • max_hops: 3")
        print("   ⚠️  Esperando herramienta MCP en Cursor...")
        print()
        
        # 4. get_growth_rate para patrones sospechosos
        print("4️⃣ 📈 get_growth_rate - Analizar patrones de crecimiento")
        print(f"   📋 Usuario: {usuario_principal}")
        print("   📋 Parámetros sugeridos:")
        print("     • user_id: '1348718991'")
        print("     • edge_type: 'device' (o 'identity', 'address', 'bank_account')")
        print("     • time_period_days: 30")
        print("   ⚠️  Esperando herramienta MCP en Cursor...")
        print()
        
        # Verificar cada usuario relacionado
        print("🔗 VERIFICACIÓN INDIVIDUAL DE USUARIOS RELACIONADOS:")
        print("-" * 60)
        
        for i, user_relacionado in enumerate(usuarios_relacionados, 1):
            print(f"{i}️⃣ Usuario: {user_relacionado}")
            print(f"   🕸️ get_subgraph(node_id='{user_relacionado}', depth=2)")
            print(f"   🚨 get_hops_to_fraud(user_id='{user_relacionado}')")
            print()
        
        print("💡 INSTRUCCIONES PARA USAR EN CURSOR:")
        print("-" * 50)
        print("1. Abrir Cursor con las herramientas MCP disponibles")
        print("2. Ejecutar get_subgraph con el usuario 1348718991")
        print("3. Comparar los subgrafos para identificar conexiones compartidas")
        print("4. Usar get_hops_to_fraud para verificar conexiones fraudulentas")
        print("5. Analizar tipos de cruces específicos:")
        print("   • Identidad (DNI, CUIT compartidos)")
        print("   • Device (fingerprints, device_ids)")
        print("   • Dirección (IPs, domicilios)")
        print("   • Cuentas bancarias (CBUs, alias)")
        
        print()
        print("✅ VERIFICACIÓN PREPARADA - LISTO PARA USAR MCP TOOLS")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """Función principal"""
    await verificar_cruces_usuario()

if __name__ == "__main__":
    asyncio.run(main())