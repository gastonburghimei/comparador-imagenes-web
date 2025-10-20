#!/usr/bin/env python3
"""
🔍 VERIFICAR ESQUEMAS DE TABLAS PARA VELOCIDAD DE RETIRADA
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bigquery_config import create_default_connection

def verificar_esquemas():
    """Verificar esquemas de las tablas necesarias"""
    
    bq_client = create_default_connection()
    
    tablas = [
        "meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT",
        "meli-bi-data.WHOWNER.BT_MP_WITHDRAWALS", 
        "meli-bi-data.WHOWNER.BT_MP_PAYOUTS"
    ]
    
    for tabla in tablas:
        print(f"\n🔍 ESQUEMA DE: {tabla}")
        print("="*80)
        
        try:
            # Query para obtener las primeras columnas
            query = f"""
            SELECT *
            FROM `{tabla}`
            LIMIT 1
            """
            
            result = bq_client.execute_query(query)
            
            if hasattr(result, 'columns'):
                columnas = result.columns.tolist()
                print(f"📋 COLUMNAS ENCONTRADAS ({len(columnas)}):")
                for i, col in enumerate(columnas, 1):
                    print(f"   {i:2d}. {col}")
            else:
                print("❌ No se pudieron obtener columnas")
                
        except Exception as e:
            print(f"❌ ERROR: {str(e)}")
            # Intentar con INFORMATION_SCHEMA
            try:
                parts = tabla.split('.')
                dataset = parts[1] 
                table_name = parts[2]
                
                schema_query = f"""
                SELECT column_name, data_type
                FROM `{parts[0]}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
                LIMIT 20
                """
                
                schema_result = bq_client.execute_query(schema_query)
                
                if hasattr(schema_result, 'iloc') and len(schema_result) > 0:
                    print(f"📋 COLUMNAS VIA SCHEMA ({len(schema_result)}):")
                    for _, row in schema_result.iterrows():
                        print(f"   • {row['column_name']} ({row['data_type']})")
                        
            except Exception as e2:
                print(f"❌ ERROR SCHEMA: {str(e2)}")

if __name__ == "__main__":
    verificar_esquemas()




















