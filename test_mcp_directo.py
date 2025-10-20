#!/usr/bin/env python3
"""
🔗 PROBADOR MCP DIRECTO
Probar conexión directa al MCP de Account Relations
"""

import requests
import json
import sys

def test_mcp_connection():
    """Probar conexión al MCP"""
    
    print("🔗 PROBANDO CONEXIÓN MCP DIRECTO")
    print("=" * 50)
    
    # URL del MCP
    url = "https://account-relations-mcp.melioffice.com/sse"
    
    headers = {
        'User-Agent': 'Cursor/1.0',
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache'
    }
    
    try:
        print(f"📡 Conectando a: {url}")
        response = requests.get(url, headers=headers, stream=True, timeout=10)
        
        print(f"🎯 Status Code: {response.status_code}")
        print(f"🎯 Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            print("✅ CONEXIÓN EXITOSA")
            
            # Leer primeras líneas del stream
            lines = []
            for line in response.iter_lines(decode_unicode=True):
                if line:
                    lines.append(line)
                    print(f"📄 {line}")
                    if len(lines) >= 10:  # Solo primeras 10 líneas
                        break
                        
        else:
            print(f"❌ ERROR: {response.status_code}")
            print(f"📄 Response: {response.text}")
            
    except Exception as e:
        print(f"❌ ERROR CONEXIÓN: {e}")

def test_mcp_tools():
    """Probar herramientas MCP disponibles"""
    
    print("\n🛠️ PROBANDO HERRAMIENTAS MCP")
    print("=" * 50)
    
    # Intentar obtener lista de herramientas
    # (esto es experimental, el protocolo real puede ser diferente)
    
    url = "https://account-relations-mcp.melioffice.com/sse"
    
    # Mensaje de prueba para listar herramientas
    test_messages = [
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        {"jsonrpc": "2.0", "id": 2, "method": "initialize", "params": {"protocolVersion": "1.0"}},
    ]
    
    for msg in test_messages:
        try:
            print(f"📤 Enviando: {json.dumps(msg)}")
            
            response = requests.post(
                url + "/message",  # Asumiendo endpoint para mensajes
                json=msg,
                headers={'Content-Type': 'application/json'},
                timeout=5
            )
            
            print(f"📥 Response: {response.status_code}")
            if response.text:
                print(f"📄 Content: {response.text[:200]}...")
                
        except Exception as e:
            print(f"❌ Error enviando mensaje: {e}")

if __name__ == "__main__":
    test_mcp_connection()
    test_mcp_tools()