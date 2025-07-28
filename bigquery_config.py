"""
Configuración y utilidades para conectar Cursor a Google BigQuery
Basado en las mejores prácticas de Google Cloud BigQuery
"""

import os
import json
from typing import Optional, Dict, Any, List
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
from google.auth import default


class BigQueryConfig:
    """Configuración central para BigQuery"""
    
    def __init__(self, 
                 project_id: Optional[str] = None,
                 credentials_path: Optional[str] = None,
                 location: str = "US"):
        """
        Inicializa la configuración de BigQuery
        
        Args:
            project_id: ID del proyecto de Google Cloud
            credentials_path: Ruta al archivo JSON de credenciales
            location: Ubicación del dataset (US, EU, etc.)
        """
        self.project_id = project_id or os.getenv('GOOGLE_CLOUD_PROJECT') or 'meli-bi-data'
        self.credentials_path = credentials_path or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        self.location = location
        self._client = None
        self._credentials = None
        
    def get_credentials(self):
        """Obtiene las credenciales de autenticación"""
        if self._credentials:
            return self._credentials
            
        if self.credentials_path and Path(self.credentials_path).exists():
            # Usar service account desde archivo JSON
            self._credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
        else:
            # Usar credenciales por defecto (ADC)
            try:
                self._credentials, project = default()
                if not self.project_id:
                    self.project_id = project
            except Exception as e:
                raise Exception(f"No se pudieron obtener credenciales: {e}")
                
        return self._credentials
    
    def get_client(self) -> bigquery.Client:
        """Obtiene el cliente de BigQuery configurado"""
        if self._client:
            return self._client
            
        credentials = self.get_credentials()
        self._client = bigquery.Client(
            project=self.project_id,
            credentials=credentials,
            location=self.location
        )
        return self._client


class BigQueryConnection:
    """Clase principal para manejar conexiones y operaciones de BigQuery"""
    
    def __init__(self, config: BigQueryConfig):
        self.config = config
        self.client = config.get_client()
        
    def test_connection(self) -> Dict[str, Any]:
        """Prueba la conexión a BigQuery"""
        try:
            # Realizar una consulta simple para probar la conexión
            query = "SELECT 1 as test_value, CURRENT_TIMESTAMP() as test_time"
            result = self.client.query(query).result()
            
            for row in result:
                return {
                    "status": "success",
                    "message": "Conexión exitosa a BigQuery",
                    "project_id": self.config.project_id,
                    "test_value": row.test_value,
                    "test_time": str(row.test_time)
                }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error al conectar a BigQuery: {str(e)}",
                "project_id": self.config.project_id
            }
    
    def list_datasets(self) -> List[str]:
        """Lista todos los datasets disponibles"""
        try:
            datasets = list(self.client.list_datasets())
            return [dataset.dataset_id for dataset in datasets]
        except Exception as e:
            print(f"Error al listar datasets: {e}")
            return []
    
    def list_tables(self, dataset_id: str) -> List[str]:
        """Lista todas las tablas en un dataset"""
        try:
            dataset_ref = self.client.dataset(dataset_id)
            tables = list(self.client.list_tables(dataset_ref))
            return [table.table_id for table in tables]
        except Exception as e:
            print(f"Error al listar tablas: {e}")
            return []
    
    def execute_query(self, query: str, to_dataframe: bool = True):
        """Ejecuta una consulta SQL en BigQuery"""
        try:
            if to_dataframe:
                # Usar pandas-gbq para obtener directamente un DataFrame
                df = pandas_gbq.read_gbq(
                    query,
                    project_id=self.config.project_id,
                    credentials=self.config.get_credentials(),
                    location=self.config.location
                )
                return df
            else:
                # Usar el cliente de BigQuery directamente
                query_job = self.client.query(query)
                return query_job.result()
        except Exception as e:
            print(f"Error al ejecutar consulta: {e}")
            return None
    
    def get_table_schema(self, dataset_id: str, table_id: str) -> List[Dict]:
        """Obtiene el esquema de una tabla"""
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            table = self.client.get_table(table_ref)
            
            schema = []
            for field in table.schema:
                schema.append({
                    "name": field.name,
                    "field_type": field.field_type,
                    "mode": field.mode,
                    "description": field.description
                })
            return schema
        except Exception as e:
            print(f"Error al obtener esquema: {e}")
            return []
    
    def upload_dataframe(self, df: pd.DataFrame, dataset_id: str, table_id: str, 
                        if_exists: str = "replace") -> bool:
        """Sube un DataFrame a BigQuery"""
        try:
            pandas_gbq.to_gbq(
                df,
                destination_table=f"{dataset_id}.{table_id}",
                project_id=self.config.project_id,
                credentials=self.config.get_credentials(),
                location=self.config.location,
                if_exists=if_exists
            )
            return True
        except Exception as e:
            print(f"Error al subir DataFrame: {e}")
            return False


def create_default_connection() -> BigQueryConnection:
    """Crea una conexión por defecto usando variables de entorno"""
    config = BigQueryConfig()
    return BigQueryConnection(config)


# Funciones de utilidad para uso directo
def quick_query(query: str, project_id: Optional[str] = None) -> pd.DataFrame:
    """Ejecuta una consulta rápida y retorna un DataFrame"""
    connection = create_default_connection()
    if project_id:
        connection.config.project_id = project_id
    return connection.execute_query(query)


def get_sample_data(dataset_id: str, table_id: str, limit: int = 100) -> pd.DataFrame:
    """Obtiene datos de muestra de una tabla"""
    query = f"""
    SELECT *
    FROM `{dataset_id}.{table_id}`
    LIMIT {limit}
    """
    return quick_query(query)


if __name__ == "__main__":
    # Ejemplo de uso
    try:
        connection = create_default_connection()
        result = connection.test_connection()
        print("Resultado de la prueba de conexión:")
        print(json.dumps(result, indent=2, default=str))
        
        if result["status"] == "success":
            print("\n📊 Datasets disponibles:")
            datasets = connection.list_datasets()
            for dataset in datasets[:5]:  # Mostrar solo los primeros 5
                print(f"  - {dataset}")
                
    except Exception as e:
        print(f"❌ Error: {e}") 