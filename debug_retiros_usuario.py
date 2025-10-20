#!/usr/bin/env python3
"""
🔍 DEBUG: ¿Por qué no encontramos retiros?
Investigar si el usuario 684730474 realmente no tiene egresos
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bigquery_config import create_default_connection

def debug_transacciones_usuario(user_id):
    """Debug completo de transacciones del usuario"""
    
    print(f"\n🔍 DEBUG TRANSACCIONES - Usuario: {user_id}")
    print("="*80)
    
    bq_client = create_default_connection()
    
    # 1. TODAS las transacciones (positivas y negativas)
    print("\n1️⃣ TODAS LAS TRANSACCIONES EN BT_SCO_ORIGIN_REPORT:")
    print("-"*60)
    
    query_todas = f"""
    SELECT 
        PAY_CREATED_DT as fecha,
        PAY_TRANSACTION_DOL_AMT as monto,
        PAY_OPERATION_TYPE_ID as tipo,
        PAY_STATUS_ID as estado,
        CASE 
            WHEN PAY_TRANSACTION_DOL_AMT > 0 THEN 'INGRESO'
            WHEN PAY_TRANSACTION_DOL_AMT < 0 THEN 'EGRESO'
            WHEN PAY_TRANSACTION_DOL_AMT = 0 THEN 'NEUTRO'
            ELSE 'UNKNOWN'
        END as direccion
    FROM `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT`
    WHERE 
        CUS_CUST_ID_SEL = {user_id}
        AND PAY_STATUS_ID = 'approved'
        AND PAY_CREATED_DT >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    ORDER BY PAY_CREATED_DT DESC
    LIMIT 20
    """
    
    try:
        result = bq_client.execute_query(query_todas)
        
        if hasattr(result, 'iloc') and len(result) > 0:
            print(f"📊 ENCONTRADAS {len(result)} TRANSACCIONES:")
            
            ingresos = 0
            egresos = 0
            neutros = 0
            
            for i, (_, row) in enumerate(result.iterrows(), 1):
                monto = float(row['monto'])
                direccion = row['direccion']
                
                if direccion == 'INGRESO':
                    ingresos += 1
                elif direccion == 'EGRESO':
                    egresos += 1
                else:
                    neutros += 1
                
                # Mostrar primeras 10
                if i <= 10:
                    signo = "+" if monto >= 0 else ""
                    emoji = "💰" if monto > 0 else "💸" if monto < 0 else "⚪"
                    print(f"   {i:2d}. {emoji} {signo}${monto:,.2f} | {row['fecha']} | {row['tipo']} | {direccion}")
            
            print(f"\n📈 RESUMEN:")
            print(f"   💰 Ingresos: {ingresos}")
            print(f"   💸 Egresos: {egresos}")
            print(f"   ⚪ Neutros: {neutros}")
            print(f"   📊 Total: {len(result)}")
            
            if egresos == 0:
                print(f"\n⚠️  ¡PROBLEMA! Usuario NO tiene egresos en BT_SCO_ORIGIN_REPORT")
                print(f"   🤔 Esto explicaría por qué no encontramos retiros")
                
        else:
            print("❌ No se encontraron transacciones")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # 2. Buscar en otras tablas posibles para egresos
    print(f"\n2️⃣ BUSCANDO EGRESOS EN BT_MP_PAY_PAYMENTS:")
    print("-"*60)
    
    query_payments = f"""
    SELECT 
        pay_move_date as fecha,
        PAY_TRANSACTION_DOL_AMT as monto,
        PAY_OPERATION_TYPE_ID as tipo,
        pay_status_id as estado
    FROM `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
    WHERE 
        CUS_CUST_ID_SEL = {user_id}
        AND pay_status_id = 'approved'
        AND PAY_TRANSACTION_DOL_AMT != 0
        AND pay_move_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    ORDER BY pay_move_date DESC
    LIMIT 10
    """
    
    try:
        result_payments = bq_client.execute_query(query_payments)
        
        if hasattr(result_payments, 'iloc') and len(result_payments) > 0:
            print(f"📊 ENCONTRADAS {len(result_payments)} TRANSACCIONES EN PAYMENTS:")
            
            for i, (_, row) in enumerate(result_payments.iterrows(), 1):
                monto = float(row['monto'])
                signo = "+" if monto >= 0 else ""
                emoji = "💰" if monto > 0 else "💸"
                print(f"   {i:2d}. {emoji} {signo}${monto:,.2f} | {row['fecha']} | {row['tipo']}")
        else:
            print("❌ No se encontraron transacciones en PAYMENTS")
            
    except Exception as e:
        print(f"❌ Error en PAYMENTS: {e}")
    
    # 3. Conclusión
    print(f"\n🎯 CONCLUSIÓN:")
    print("="*50)
    print("Si el usuario solo tiene INGRESOS y NO tiene EGRESOS,")
    print("entonces es lógico que la velocidad de retirada sea 'NORMAL'")
    print("porque literalmente NO RETIRA el dinero que recibe.")
    print()
    print("Esto podría indicar:")
    print("• El usuario acumula dinero sin retirarlo")
    print("• Los retiros están en otras tablas que no consultamos")  
    print("• El usuario usa otros métodos de egreso")

if __name__ == "__main__":
    # Debug con el usuario actual
    debug_transacciones_usuario("684730474")




















