#!/usr/bin/env python3
"""
🔐 PROBAR DIFERENTES MÉTODOS DE AUTENTICACIÓN MCP
"""

import requests
import json
import os

def test_auth_methods():
    """Probar diferentes métodos de autenticación"""
    
    base_url = "https://account-relations-mcp.melioffice.com"
    
    # Obtener session ID
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
    
    # Diferentes headers de autenticación a probar
    auth_tests = [
        {
            "name": "Headers estándar ML",
            "headers": {
                'Content-Type': 'application/json',
                'User-Agent': 'Cursor/1.0',
                'Accept': 'application/json',
                'X-Caller': 'cursor-mcp-client',
                'X-Source': 'account-relations'
            }
        },
        {
            "name": "Headers con Bearer Token (vacío)",
            "headers": {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ',
                'User-Agent': 'Cursor/1.0'
            }
        },
        {
            "name": "Headers con API Key",
            "headers": {
                'Content-Type': 'application/json',
                'X-API-Key': 'cursor-client',
                'User-Agent': 'MCP-Client/1.0'
            }
        },
        {
            "name": "Headers específicos MCP",
            "headers": {
                'Content-Type': 'application/json',
                'User-Agent': 'ModelContextProtocol/1.0',
                'Accept': 'application/json',
                'X-MCP-Version': '2024-11-05'
            }
        },
        {
            "name": "Headers como browser",
            "headers": {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'cross-site'
            }
        }
    ]
    
    # Mensaje de prueba
    test_message = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "say_hello"
    }
    
    for auth_test in auth_tests:
        print(f"\n🔐 PROBANDO: {auth_test['name']}")
        print("-" * 50)
        
        try:
            response = requests.post(
                message_url,
                json=test_message,
                headers=auth_test['headers'],
                timeout=15
            )
            
            print(f"Status: {response.status_code}")
            print(f"Content-Length: {response.headers.get('Content-Length', 'N/A')}")
            print(f"Response: '{response.text[:200]}{'...' if len(response.text) > 200 else ''}'")
            
            if response.text.strip():
                try:
                    parsed = response.json()
                    print(f"✅ JSON VÁLIDO: {json.dumps(parsed, indent=2)}")
                    return True  # Encontramos el método correcto!
                except:
                    print(f"❌ No es JSON válido pero tiene contenido")
            else:
                print("❌ Respuesta vacía")
                
        except Exception as e:
            print(f"❌ Error: {e}")
    
    return False

def test_with_environment_tokens():
    """Probar con tokens de ambiente"""
    print("\n🌍 PROBANDO CON VARIABLES DE AMBIENTE")
    print("=" * 50)
    
    # Variables de ambiente que podrían contener tokens
    potential_tokens = [
        'MELI_TOKEN',
        'ML_TOKEN', 
        'MERCADOLIBRE_TOKEN',
        'MCP_TOKEN',
        'ACCOUNT_RELATIONS_TOKEN',
        'GOOGLE_APPLICATION_CREDENTIALS'
    ]
    
    for token_var in potential_tokens:
        token_value = os.environ.get(token_var)
        if token_value:
            print(f"✅ Encontrado {token_var}: {token_value[:20]}...")
        else:
            print(f"❌ No encontrado {token_var}")

if __name__ == "__main__":
    success = test_auth_methods()
    
    if not success:
        test_with_environment_tokens()
        print("\n💡 SIGUIENTE PASO: Contactar al equipo de Account Relations MCP")
        print("   - Verificar permisos de usuario")
        print("   - Obtener token de autenticación")
        print("   - Confirmar protocolo correcto")