#!/usr/bin/env python3
"""
🔗 CLIENTE MCP REAL
Cliente para conectar al MCP de Account Relations y obtener cruces reales
"""

import requests
import json
import re
import time

class MCPAccountRelationsClient:
    def __init__(self):
        self.base_url = "https://account-relations-mcp.melioffice.com"
        self.session_id = None
        self.message_endpoint = None
        
    def connect(self):
        """Conectar al MCP y obtener session ID"""
        
        print("🔗 Conectando a Account Relations MCP...")
        
        try:
            # Conectar al SSE endpoint
            response = requests.get(
                f"{self.base_url}/sse",
                headers={
                    'User-Agent': 'Cursor/1.0',
                    'Accept': 'text/event-stream',
                    'Cache-Control': 'no-cache'
                },
                stream=True,
                timeout=10
            )
            
            if response.status_code != 200:
                print(f"❌ Error conectando: {response.status_code}")
                return False
            
            # Leer las primeras líneas para obtener session ID
            lines_read = 0
            for line in response.iter_lines(decode_unicode=True):
                lines_read += 1
                if line.startswith('id:'):
                    self.session_id = line.replace('id:', '').strip()
                elif line.startswith('data:'):
                    endpoint = line.replace('data:', '').strip()
                    if '/mcp/message' in endpoint:
                        self.message_endpoint = f"{self.base_url}{endpoint}"
                        break
                
                # Evitar bucle infinito
                if lines_read > 10:
                    break
            
            if self.session_id and self.message_endpoint:
                print(f"✅ Conectado! Session ID: {self.session_id[:8]}...")
                return True
            else:
                print("❌ No se pudo obtener session ID")
                return False
                
        except Exception as e:
            print(f"❌ Error conectando: {e}")
            return False
    
    def send_message(self, method, params=None):
        """Enviar mensaje al MCP"""
        
        if not self.message_endpoint:
            print("❌ No hay conexión activa")
            return None
        
        message = {
            "jsonrpc": "2.0",
            "id": int(time.time()),
            "method": method
        }
        
        if params:
            message["params"] = params
        
        try:
            print(f"📤 Enviando: {method}")
            
            response = requests.post(
                self.message_endpoint,
                json=message,
                headers={
                    'Content-Type': 'application/json',
                    'User-Agent': 'Cursor/1.0'
                },
                timeout=30
            )
            
            print(f"📥 Status: {response.status_code}")
            
            if response.status_code == 200:
                print(f"📄 Raw response length: {len(response.text)}")
                print(f"📄 Raw response: '{response.text}'")
                print(f"📄 Response headers: {dict(response.headers)}")
                
                if not response.text.strip():
                    print("❌ RESPUESTA COMPLETAMENTE VACÍA")
                    return {"error": "empty_response", "message": "MCP devolvió respuesta vacía"}
                
                try:
                    result = response.json()
                    return result
                except Exception as e:
                    print(f"❌ Response no es JSON: {e}")
                    print(f"❌ Content: '{response.text}'")
                    return {"error": "invalid_json", "raw_response": response.text}
            else:
                print(f"❌ Error: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Error enviando mensaje: {e}")
            return None
    
    def list_tools(self):
        """Listar herramientas disponibles"""
        print("🛠️ Listando herramientas...")
        return self.send_message("tools/list")
    
    def get_subgraph(self, node_id, depth=2, relations=None):
        """Obtener subgrafo de un usuario"""
        
        if relations is None:
            relations = ["uses_device", "uses_card", "validate_phone", "validate_person", "withdrawal_bank_account"]
        
        params = {
            "node_id": str(node_id),
            "depth": depth,
            "relations": relations
        }
        
        print(f"🔍 Obteniendo subgrafo para usuario {node_id}...")
        return self.send_message("get_subgraph", params)
    
    def get_hops_to_fraud(self, node_id, max_hops=3):
        """Obtener conexiones a usuarios fraudulentos"""
        
        params = {
            "node_id": str(node_id),
            "max_hops": max_hops
        }
        
        print(f"🚨 Buscando conexiones a fraude para usuario {node_id}...")
        return self.send_message("get_hops_to_fraud", params)
    
    def say_hello(self):
        """Mensaje de prueba"""
        print("👋 Enviando saludo...")
        return self.send_message("say_hello")

def test_mcp_client():
    """Probar cliente MCP"""
    
    print("🧪 PROBANDO CLIENTE MCP REAL")
    print("=" * 50)
    
    client = MCPAccountRelationsClient()
    
    # Conectar
    if not client.connect():
        print("❌ No se pudo conectar")
        return
    
    time.sleep(1)
    
    # Probar herramientas
    tools = client.list_tools()
    if tools:
        print(f"✅ Herramientas: {json.dumps(tools, indent=2)}")
    
    time.sleep(1)
    
    # Probar saludo
    hello = client.say_hello()
    if hello:
        print(f"✅ Saludo: {json.dumps(hello, indent=2)}")
    
    time.sleep(1)
    
    # Probar subgrafo
    subgraph = client.get_subgraph("1348718991")
    if subgraph:
        print(f"✅ Subgrafo: {json.dumps(subgraph, indent=2)}")

if __name__ == "__main__":
    test_mcp_client()