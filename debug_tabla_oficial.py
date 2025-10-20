#!/usr/bin/env python3
"""
🔍 DEBUG: Verificar tabla oficial cta_hacker_chequeo_final
"""

import asyncio
import sys
from mcp_bigquery_setup import MCPBigQueryBasicOperations

async def debug_tabla_oficial():
    """Debug de la tabla oficial paso a paso"""
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    print("🔍 DEBUG: TABLA OFICIAL CTA_HACKER_CHEQUEO_FINAL")
    print("=" * 80)
    
    # 1. Verificar que la tabla existe
    print("1️⃣ VERIFICANDO EXISTENCIA DE LA TABLA...")
    try:
        query1 = """
        SELECT COUNT(*) as total_registros
        FROM `meli-bi-data.SBOX_PFFINTECHATO.cta_hacker_chequeo_final`
        LIMIT 1
        """
        
        result1 = await operations.execute_query(query1)
        print(f"   ✅ Tabla existe")
        print(f"   📊 Total registros: {result1}")
        
    except Exception as e:
        print(f"   ❌ Error accediendo a tabla: {e}")
        return
    
    # 2. Ver estructura de resultados
    print("\n2️⃣ ANALIZANDO ESTRUCTURA DE RESULTADOS...")
    try:
        query2 = """
        SELECT *
        FROM `meli-bi-data.SBOX_PFFINTECHATO.cta_hacker_chequeo_final`
        LIMIT 1
        """
        
        result2 = await operations.execute_query(query2)
        print(f"   📊 Tipo de resultado: {type(result2)}")
        print(f"   📊 Contenido resultado: {result2}")
        
        if result2 and len(result2) > 0:
            print(f"   📊 Primer elemento: {result2[0]}")
            print(f"   📊 Tipo primer elemento: {type(result2[0])}")
        
    except Exception as e:
        print(f"   ❌ Error analizando estructura: {e}")
    
    # 3. Buscar usuarios específicos
    print("\n3️⃣ BUSCANDO USUARIOS ESPECÍFICOS...")
    usuarios_test = ["760785507", "1105384237", "1539173000"]
    
    for user_id in usuarios_test:
        try:
            query3 = f"""
            SELECT 
                user_id,
                cant_trans_marcadas,
                monto_marcado
            FROM `meli-bi-data.SBOX_PFFINTECHATO.cta_hacker_chequeo_final`
            WHERE user_id = {user_id}
            LIMIT 1
            """
            
            result3 = await operations.execute_query(query3)
            
            if result3 and len(result3) > 0:
                print(f"   ✅ Usuario {user_id}: ENCONTRADO")
                print(f"     📊 Datos: {result3[0]}")
            else:
                print(f"   ❌ Usuario {user_id}: NO ENCONTRADO")
                
        except Exception as e:
            print(f"   ❌ Usuario {user_id}: Error - {e}")
    
    # 4. Buscar cualquier usuario con transacciones marcadas
    print("\n4️⃣ BUSCANDO USUARIOS CON TRANSACCIONES MARCADAS...")
    try:
        query4 = """
        SELECT 
            user_id,
            cant_trans_marcadas,
            monto_marcado,
            fecha_creacion_cuenta,
            dias_cuenta_activa
        FROM `meli-bi-data.SBOX_PFFINTECHATO.cta_hacker_chequeo_final`
        WHERE cant_trans_marcadas > 0
        ORDER BY cant_trans_marcadas DESC
        LIMIT 3
        """
        
        result4 = await operations.execute_query(query4)
        
        if result4 and len(result4) > 0:
            print(f"   ✅ Usuarios con transacciones marcadas encontrados:")
            for row in result4:
                print(f"     • Usuario: {row}")
        else:
            print(f"   ❌ No se encontraron usuarios con transacciones marcadas")
                
    except Exception as e:
        print(f"   ❌ Error buscando usuarios: {e}")

if __name__ == "__main__":
    asyncio.run(debug_tabla_oficial())