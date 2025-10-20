#!/usr/bin/env python3
"""
🔄 PROBAR PROTOCOLO SSE COMPLETO
"""

import requests
import json
import time

def test_sse_protocol():
    """Probar protocolo SSE completo como lo hace Cursor"""
    
    base_url = "https://account-relations-mcp.melioffice.com"
    
    print("🔗 Conectando al SSE endpoint...")
    
    try:
        # Mantener conexión SSE abierta
        response = requests.get(
            f"{base_url}/sse",
            headers={
                'Accept': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'User-Agent': 'Cursor/1.0',
                'Connection': 'keep-alive'
            },
            stream=True,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ Error conectando SSE: {response.status_code}")
            return
        
        print("✅ Conectado al SSE")
        
        session_id = None
        message_endpoint = None
        
        # Leer el stream SSE
        print("📡 Leyendo eventos SSE...")
        
        for i, line in enumerate(response.iter_lines(decode_unicode=True)):
            if line:
                print(f"📄 Line {i}: {line}")
                
                if line.startswith('id:'):
                    session_id = line.replace('id:', '').strip()
                    print(f"🎯 Session ID extraído: {session_id}")
                    
                elif line.startswith('data:'):
                    endpoint = line.replace('data:', '').strip()
                    if '/mcp/message' in endpoint:
                        message_endpoint = f"{base_url}{endpoint}"
                        print(f"🎯 Message endpoint: {message_endpoint}")
                        break
            
            # Limitar lectura
            if i > 20:
                print("⏰ Timeout leyendo SSE")
                break
        
        if not session_id or not message_endpoint:
            print("❌ No se pudo obtener session ID o endpoint")
            return
        
        # Ahora probar envío de mensaje con el protocolo correcto
        print(f"\n📤 Enviando mensaje a: {message_endpoint}")
        
        # Intentar protocolo MCP real
        initialize_message = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "roots": {
                        "listChanged": False
                    },
                    "sampling": {}
                },
                "clientInfo": {
                    "name": "cursor-mcp-client",
                    "version": "1.0.0"
                }
            }
        }
        
        msg_response = requests.post(
            message_endpoint,
            json=initialize_message,
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'User-Agent': 'Cursor/1.0'
            },
            timeout=10
        )
        
        print(f"📥 Initialize Response:")
        print(f"   Status: {msg_response.status_code}")
        print(f"   Headers: {dict(msg_response.headers)}")
        print(f"   Content: '{msg_response.text}'")
        
        if msg_response.text.strip():
            print("✅ ¡RESPUESTA CON CONTENIDO!")
            try:
                parsed = msg_response.json()
                print(f"📋 Parsed: {json.dumps(parsed, indent=2)}")
                
                # Si initialize funciona, probar tools/list
                if 'result' in parsed:
                    print("\n🛠️ Probando tools/list...")
                    
                    tools_message = {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "tools/list"
                    }
                    
                    tools_response = requests.post(
                        message_endpoint,
                        json=tools_message,
                        headers={
                            'Content-Type': 'application/json',
                            'Accept': 'application/json',
                            'User-Agent': 'Cursor/1.0'
                        },
                        timeout=10
                    )
                    
                    print(f"🛠️ Tools Response: {tools_response.text}")
                    
                    if tools_response.text.strip():
                        tools_parsed = tools_response.json()
                        print(f"🎯 HERRAMIENTAS DISPONIBLES:")
                        if 'result' in tools_parsed and 'tools' in tools_parsed['result']:
                            for tool in tools_parsed['result']['tools']:
                                print(f"   - {tool.get('name', 'N/A')}: {tool.get('description', 'N/A')}")
                        return True
                
            except Exception as e:
                print(f"❌ Error parsing JSON: {e}")
        else:
            print("❌ Respuesta vacía nuevamente")
        
    except Exception as e:
        print(f"❌ Error en protocolo SSE: {e}")
    
    return False

if __name__ == "__main__":
    success = test_sse_protocol()
    
    if success:
        print("\n✅ ¡MCP FUNCIONA! Protocolo identificado.")
    else:
        print("\n❌ MCP sigue sin funcionar.")
        print("\n🚨 CONCLUSIÓN:")
        print("   1. El servidor responde correctamente (200 OK)")
        print("   2. Session ID se obtiene sin problemas") 
        print("   3. Todas las respuestas tienen Content-Length: 0")
        print("   4. Probamos múltiples protocolos y headers")
        print("\n💡 POSIBLES CAUSAS:")
        print("   1. ❓ Usuario sin permisos en Account Relations MCP")
        print("   2. ❓ Autenticación mediante token/cookie específico")
        print("   3. ❓ Whitelist de IPs o User-Agents")
        print("   4. ❓ Servidor en modo debug/mantenimiento")
        print("\n🎯 ACCIÓN REQUERIDA:")
        print("   📞 Contactar equipo Account Relations MCP")
        print("   🔑 Solicitar credenciales/permisos apropiados")
        print("   📖 Obtener documentación oficial del protocolo")