#!/usr/bin/env python3
"""
🔍 PROBAR DIFERENTES PROTOCOLOS MCP
"""

import requests
import json

def test_different_protocols():
    """Probar diferentes formatos de protocolo MCP"""
    
    base_url = "https://account-relations-mcp.melioffice.com"
    
    # Obtener session ID real
    print("🔗 Obteniendo session ID...")
    sse_response = requests.get(f"{base_url}/sse", stream=True, timeout=10)
    
    session_id = None
    for line in sse_response.iter_lines(decode_unicode=True):
        if line.startswith('id:'):
            session_id = line.replace('id:', '').strip()
            break
    
    if not session_id:
        print("❌ No se pudo obtener session ID")
        return
    
    print(f"✅ Session ID: {session_id}")
    
    # URL del mensaje
    message_url = f"{base_url}/mcp/message?sessionId={session_id}"
    
    # Diferentes formatos de mensaje a probar
    test_cases = [
        {
            "name": "MCP Standard",
            "message": {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list"
            }
        },
        {
            "name": "MCP with params",
            "message": {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "test-client",
                        "version": "1.0.0"
                    }
                }
            }
        },
        {
            "name": "Say Hello Simple",
            "message": {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "say_hello"
            }
        },
        {
            "name": "Say Hello with params",
            "message": {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "say_hello",
                "params": {}
            }
        }
    ]
    
    for test_case in test_cases:
        print(f"\n🧪 PROBANDO: {test_case['name']}")
        print("-" * 40)
        
        try:
            response = requests.post(
                message_url,
                json=test_case['message'],
                headers={
                    'Content-Type': 'application/json',
                    'User-Agent': 'test-client/1.0',
                    'Accept': 'application/json'
                },
                timeout=10
            )
            
            print(f"Status: {response.status_code}")
            print(f"Headers: {dict(response.headers)}")
            print(f"Content-Length: {response.headers.get('Content-Length', 'N/A')}")
            print(f"Response: '{response.text}'")
            
            if response.text.strip():
                try:
                    parsed = response.json()
                    print(f"✅ JSON válido: {json.dumps(parsed, indent=2)}")
                except:
                    print(f"❌ No es JSON válido")
            else:
                print("❌ Respuesta vacía")
                
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_different_protocols()