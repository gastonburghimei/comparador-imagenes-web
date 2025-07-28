#!/usr/bin/env python3
"""
MCP BigQuery Analyzer - Recreación basada en fury_mcp-pf-bigquery-analizer
Basado en: 
- https://github.com/melisource/fury_mcp-pf-bigquery-analizer/blob/main/docs/GETTING_STARTED.md
- https://github.com/melisource/fury_mcp-pf-bigquery-analizer/blob/main/src/mcp_bigquery/tools/basic_operations.py
"""

import json
import os
import asyncio
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from pathlib import Path

@dataclass
class MCPConfig:
    """Configuración del MCP BigQuery"""
    name: str = "mcp-bigquery-analyzer"
    version: str = "1.0.0"
    description: str = "MCP for BigQuery analysis and operations"
    author: str = "MercadoLibre"
    
class MCPBigQueryBasicOperations:
    """
    Operaciones básicas del MCP BigQuery - Basado en basic_operations.py
    """
    
    def __init__(self, project_id: str, location: str = "US"):
        self.project_id = project_id
        self.location = location
        self.client = None
        
    async def initialize(self):
        """Inicializar el cliente de BigQuery"""
        try:
            from google.cloud import bigquery
            self.client = bigquery.Client(project=self.project_id, location=self.location)
            return {"status": "success", "message": "MCP BigQuery initialized"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    async def list_datasets(self) -> Dict[str, Any]:
        """Lista datasets disponibles"""
        try:
            datasets = []
            for dataset in self.client.list_datasets():
                dataset_info = {
                    "dataset_id": dataset.dataset_id,
                    "project": dataset.project,
                    "full_name": f"{dataset.project}.{dataset.dataset_id}",
                    "location": getattr(dataset, 'location', 'unknown')
                }
                datasets.append(dataset_info)
            
            return {
                "tool": "list_datasets",
                "status": "success",
                "result": {
                    "total_datasets": len(datasets),
                    "datasets": datasets
                }
            }
        except Exception as e:
            return {"tool": "list_datasets", "status": "error", "error": str(e)}
    
    async def list_tables(self, dataset_id: str) -> Dict[str, Any]:
        """Lista tablas en un dataset"""
        try:
            dataset_ref = self.client.dataset(dataset_id)
            tables = []
            
            for table in self.client.list_tables(dataset_ref):
                table_info = {
                    "table_id": table.table_id,
                    "dataset_id": dataset_id,
                    "project": self.project_id,
                    "full_name": f"{self.project_id}.{dataset_id}.{table.table_id}",
                    "table_type": table.table_type
                }
                tables.append(table_info)
            
            return {
                "tool": "list_tables",
                "status": "success",
                "result": {
                    "dataset_id": dataset_id,
                    "total_tables": len(tables),
                    "tables": tables
                }
            }
        except Exception as e:
            return {"tool": "list_tables", "status": "error", "error": str(e)}
    
    async def get_table_schema(self, dataset_id: str, table_id: str) -> Dict[str, Any]:
        """Obtiene el esquema de una tabla"""
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            table = self.client.get_table(table_ref)
            
            schema = []
            for field in table.schema:
                field_info = {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode,
                    "description": field.description or ""
                }
                schema.append(field_info)
            
            return {
                "tool": "get_table_schema",
                "status": "success",
                "result": {
                    "table_id": table_id,
                    "dataset_id": dataset_id,
                    "schema": schema,
                    "num_fields": len(schema),
                    "table_info": {
                        "created": str(table.created),
                        "modified": str(table.modified),
                        "num_rows": table.num_rows,
                        "size_bytes": table.num_bytes
                    }
                }
            }
        except Exception as e:
            return {"tool": "get_table_schema", "status": "error", "error": str(e)}
    
    async def execute_query(self, query: str, limit: int = 1000) -> Dict[str, Any]:
        """Ejecuta una consulta SQL"""
        try:
            # Agregar LIMIT si no existe
            if "LIMIT" not in query.upper():
                query = f"{query.rstrip(';')} LIMIT {limit}"
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Convertir resultados a formato JSON serializable
            rows = []
            for row in results:
                row_dict = {}
                for key, value in row.items():
                    # Manejar tipos especiales de BigQuery
                    if hasattr(value, 'isoformat'):  # datetime objects
                        row_dict[key] = value.isoformat()
                    elif hasattr(value, '__iter__') and not isinstance(value, str):  # arrays
                        row_dict[key] = list(value)
                    else:
                        row_dict[key] = value
                rows.append(row_dict)
            
            return {
                "tool": "execute_query",
                "status": "success",
                "result": {
                    "query": query,
                    "total_rows": len(rows),
                    "rows": rows,
                    "job_info": {
                        "job_id": query_job.job_id,
                        "bytes_processed": query_job.total_bytes_processed,
                        "bytes_billed": query_job.total_bytes_billed
                    }
                }
            }
        except Exception as e:
            return {"tool": "execute_query", "status": "error", "error": str(e)}

class MCPBigQueryServer:
    """
    Servidor MCP para BigQuery - Basado en la estructura de fury_mcp-pf-bigquery-analizer
    """
    
    def __init__(self, config: MCPConfig):
        self.config = config
        self.operations = None
        
    def setup_operations(self, project_id: str, location: str = "US"):
        """Configurar operaciones básicas"""
        self.operations = MCPBigQueryBasicOperations(project_id, location)
        
    async def initialize(self):
        """Inicializar servidor MCP"""
        if self.operations:
            return await self.operations.initialize()
        return {"status": "error", "message": "Operations not configured"}
    
    def get_capabilities(self) -> Dict[str, Any]:
        """Retorna las capacidades del MCP"""
        return {
            "name": self.config.name,
            "version": self.config.version,
            "description": self.config.description,
            "tools": [
                {
                    "name": "list_datasets",
                    "description": "Lista todos los datasets disponibles en el proyecto",
                    "parameters": {}
                },
                {
                    "name": "list_tables",
                    "description": "Lista todas las tablas en un dataset",
                    "parameters": {
                        "dataset_id": {"type": "string", "required": True}
                    }
                },
                {
                    "name": "get_table_schema",
                    "description": "Obtiene el esquema de una tabla específica",
                    "parameters": {
                        "dataset_id": {"type": "string", "required": True},
                        "table_id": {"type": "string", "required": True}
                    }
                },
                {
                    "name": "execute_query",
                    "description": "Ejecuta una consulta SQL en BigQuery",
                    "parameters": {
                        "query": {"type": "string", "required": True},
                        "limit": {"type": "integer", "default": 1000}
                    }
                }
            ]
        }

def create_cursor_mcp_config():
    """
    Crea la configuración MCP para Cursor
    Basado en las especificaciones de GETTING_STARTED.md
    """
    
    cursor_config_dir = Path.home() / ".cursor"
    cursor_config_dir.mkdir(exist_ok=True)
    
    mcp_config = {
        "mcpServers": {
            "bigquery-analyzer": {
                "command": "python3",
                "args": [
                    str(Path.cwd() / "mcp_bigquery_server.py")
                ],
                "env": {
                    "GOOGLE_CLOUD_PROJECT": "meli-bi-data",
                    "GOOGLE_APPLICATION_CREDENTIALS": str(Path.cwd() / "credentials" / "service-account-key.json")
                }
            }
        }
    }
    
    config_file = cursor_config_dir / "mcp_settings.json"
    with open(config_file, 'w') as f:
        json.dump(mcp_config, f, indent=2)
    
    print(f"✅ Configuración MCP creada en: {config_file}")
    return config_file

def main():
    """Función principal para testing"""
    print("🚀 Inicializando MCP BigQuery Analyzer")
    print("📋 Basado en fury_mcp-pf-bigquery-analizer")
    
    # Crear configuración
    config = MCPConfig()
    server = MCPBigQueryServer(config)
    
    # Configurar para meli-bi-data
    server.setup_operations("meli-bi-data", "US")
    
    print("✅ MCP BigQuery Server configurado")
    print("🔧 Capacidades disponibles:")
    
    capabilities = server.get_capabilities()
    for tool in capabilities["tools"]:
        print(f"   - {tool['name']}: {tool['description']}")
    
    # Crear configuración para Cursor
    create_cursor_mcp_config()
    
    return server

if __name__ == "__main__":
    server = main() 