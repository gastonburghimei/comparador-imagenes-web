#!/usr/bin/env python3
"""
Debug específico para usuario 760785507
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bigquery_config import create_default_connection

def debug_usuario():
    bq_client = create_default_connection()
    user_id = "760785507"
    
    print(f"🔍 DEBUG Usuario: {user_id}")
    print("="*50)
    
    # 1. Verificar si existe en tabla final
    query1 = f"""
    SELECT *
    FROM `meli-bi-data.SBOX_PFFINTECHATO.cta_hacker_chequeo_final`
    WHERE USER_ID = {user_id}
    """
    
    print("\n1️⃣ Verificando en cta_hacker_chequeo_final:")
    try:
        result1 = bq_client.execute_query(query1)
        print(f"Tipo resultado: {type(result1)}")
        print(f"Contenido: {result1}")
        
        if isinstance(result1, dict) and 'result' in result1:
            rows = result1.get('result', {}).get('rows', [])
            print(f"Filas encontradas: {len(rows)}")
            if rows:
                print(f"Primera fila: {rows[0]}")
        elif isinstance(result1, list):
            print(f"Lista con {len(result1)} elementos")
            if result1:
                print(f"Primer elemento: {result1[0]}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # 2. Verificar en tabla base
    query2 = f"""
    SELECT *
    FROM `meli-bi-data.SBOX_PFFINTECHATO.cta_hacker_chequeo`
    WHERE USER_ID = {user_id}
    """
    
    print("\n2️⃣ Verificando en cta_hacker_chequeo:")
    try:
        result2 = bq_client.execute_query(query2)
        print(f"Tipo resultado: {type(result2)}")
        
        if isinstance(result2, dict) and 'result' in result2:
            rows = result2.get('result', {}).get('rows', [])
            print(f"Filas encontradas: {len(rows)}")
            if rows:
                print(f"Primera fila: {rows[0]}")
        elif isinstance(result2, list):
            print(f"Lista con {len(result2)} elementos")
            if result2:
                print(f"Primer elemento: {result2[0]}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # 3. Buscar el usuario en restricciones
    query3 = f"""
    SELECT USER_ID, INFRACTION_TYPE, SENTENCE_DATE
    FROM `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW`
    WHERE USER_ID = {user_id} AND INFRACTION_TYPE = 'CUENTA_DE_HACKER'
    """
    
    print("\n3️⃣ Verificando en restricciones:")
    try:
        result3 = bq_client.execute_query(query3)
        if isinstance(result3, dict) and 'result' in result3:
            rows = result3.get('result', {}).get('rows', [])
            print(f"Filas encontradas: {len(rows)}")
            if rows:
                print(f"Primera fila: {rows[0]}")
        elif isinstance(result3, list):
            print(f"Lista con {len(result3)} elementos")
            if result3:
                print(f"Primer elemento: {result3[0]}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    debug_usuario()