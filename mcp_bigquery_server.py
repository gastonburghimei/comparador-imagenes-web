#!/usr/bin/env python3
"""
Servidor MCP BigQuery para Cursor
Basado en fury_mcp-pf-bigquery-analizer de MercadoLibre
"""

import asyncio
import json
import sys
import os
from mcp_bigquery_setup import MCPBigQueryServer, MCPConfig

class MCPBigQueryDaemon:
    """Daemon del servidor MCP para BigQuery"""
    
    def __init__(self):
        self.config = MCPConfig()
        self.server = MCPBigQueryServer(self.config)
        
        # Configurar proyecto desde variables de entorno
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'meli-bi-data')
        location = os.getenv('BIGQUERY_LOCATION', 'US')
        
        self.server.setup_operations(project_id, location)
        
    async def handle_request(self, request_data: dict) -> dict:
        """Maneja requests del protocolo MCP"""
        
        method = request_data.get('method')
        params = request_data.get('params', {})
        request_id = request_data.get('id')
        
        try:
            if method == 'initialize':
                result = await self.server.initialize()
                capabilities = self.server.get_capabilities()
                result.update(capabilities)
                
            elif method == 'tools/list':
                capabilities = self.server.get_capabilities()
                result = {"tools": capabilities["tools"]}
                
            elif method == 'tools/call':
                tool_name = params.get('name')
                tool_arguments = params.get('arguments', {})
                result = await self.call_tool(tool_name, tool_arguments)
                
            else:
                result = {"error": f"Unknown method: {method}"}
            
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result
            }
            
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": str(e)
                }
            }
    
    async def call_tool(self, tool_name: str, arguments: dict) -> dict:
        """Ejecuta herramientas del MCP"""
        
        if not self.server.operations:
            return {"error": "Operations not initialized"}
        
        try:
            if tool_name == 'list_datasets':
                return await self.server.operations.list_datasets()
                
            elif tool_name == 'list_tables':
                dataset_id = arguments.get('dataset_id')
                if not dataset_id:
                    return {"error": "dataset_id is required"}
                return await self.server.operations.list_tables(dataset_id)
                
            elif tool_name == 'get_table_schema':
                dataset_id = arguments.get('dataset_id')
                table_id = arguments.get('table_id')
                if not dataset_id or not table_id:
                    return {"error": "dataset_id and table_id are required"}
                return await self.server.operations.get_table_schema(dataset_id, table_id)
                
            elif tool_name == 'execute_query':
                query = arguments.get('query')
                limit = arguments.get('limit', 1000)
                if not query:
                    return {"error": "query is required"}
                return await self.server.operations.execute_query(query, limit)
                
            else:
                return {"error": f"Unknown tool: {tool_name}"}
                
        except Exception as e:
            return {"error": f"Tool execution failed: {str(e)}"}
    
    async def run(self):
        """Ejecuta el servidor MCP"""
        print("🚀 Iniciando MCP BigQuery Server...", file=sys.stderr)
        
        while True:
            try:
                # Leer request de stdin
                line = await asyncio.get_event_loop().run_in_executor(
                    None, sys.stdin.readline
                )
                
                if not line:
                    break
                
                # Parsear JSON request
                try:
                    request = json.loads(line.strip())
                except json.JSONDecodeError:
                    continue
                
                # Procesar request
                response = await self.handle_request(request)
                
                # Enviar response a stdout
                print(json.dumps(response), flush=True)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error en servidor MCP: {e}", file=sys.stderr)
                break
        
        print("🛑 Servidor MCP detenido", file=sys.stderr)

async def main():
    """Función principal del servidor"""
    daemon = MCPBigQueryDaemon()
    await daemon.run()

if __name__ == "__main__":
    # Configurar para modo asíncrono
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Servidor interrumpido por el usuario", file=sys.stderr)
    except Exception as e:
        print(f"❌ Error fatal: {e}", file=sys.stderr)
        sys.exit(1) 