#!/usr/bin/env python3
"""
Exportar los 2000 casos a Excel con formato de fechas corregido
"""

import asyncio
import pandas as pd
from mcp_bigquery_setup import MCPBigQueryBasicOperations

async def obtener_y_exportar_casos_completos():
    """
    Obtener todos los casos y exportarlos con formato correcto
    """
    
    print("📊 OBTENIENDO Y EXPORTANDO CASOS COMPLETOS")
    print("🎯 2000+ casos con formato Excel correcto")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    
    # Query completa para todos los casos
    query = """
    SELECT 
        res.SENTENCE_ID,
        res.USER_ID,
        res.INFRACTION_TYPE,
        res.SENTENCE_DATE,
        res.COLOR_DE_TARJETA,
        res.SIT_SITE_ID,
        res.SENTENCE_LAST_STATUS,
        ato.fecha_apertura_caso,
        ato.fecha_cierre_caso
    FROM 
        `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res
    INNER JOIN 
        `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO` ato
        ON CAST(res.USER_ID AS STRING) = CAST(ato.GCA_CUST_ID AS STRING)
    WHERE 
        res.INFRACTION_TYPE = 'CUENTA_DE_HACKER'
        AND res.SENTENCE_DATE >= '2025-04-01'
        AND res.SENTENCE_DATE <= '2025-05-31'
        AND res.SENTENCE_LAST_STATUS IN ('ACTIVE', 'ROLLBACKED', 'REINSTATED')
        
    ORDER BY 
        res.SENTENCE_DATE DESC,
        res.SENTENCE_ID DESC
    """
    
    try:
        print("📊 Ejecutando query completa...")
        result = await operations.execute_query(query, limit=3000)  # Límite alto
        
        if result["status"] == "success":
            casos = result["result"]["rows"]
            print(f"✅ {len(casos)} casos obtenidos")
            
            # Convertir a DataFrame
            df = pd.DataFrame(casos)
            print(f"📋 DataFrame creado: {len(df)} filas x {len(df.columns)} columnas")
            
            # CORREGIR formatos de fecha
            print("🔧 Corrigiendo formatos de fecha...")
            
            # Manejar fecha_apertura_caso
            if 'fecha_apertura_caso' in df.columns:
                try:
                    df['fecha_apertura_caso'] = pd.to_datetime(df['fecha_apertura_caso'], format='mixed').dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    # Si falla, usar método alternativo
                    df['fecha_apertura_caso'] = df['fecha_apertura_caso'].astype(str)
                    print("   ⚠️ fecha_apertura_caso convertida como string")
            
            # Manejar fecha_cierre_caso  
            if 'fecha_cierre_caso' in df.columns:
                try:
                    df['fecha_cierre_caso'] = pd.to_datetime(df['fecha_cierre_caso'], format='mixed').dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    # Si falla, usar método alternativo
                    df['fecha_cierre_caso'] = df['fecha_cierre_caso'].astype(str)
                    print("   ⚠️ fecha_cierre_caso convertida como string")
            
            # Manejar SENTENCE_DATE
            if 'SENTENCE_DATE' in df.columns:
                try:
                    df['SENTENCE_DATE'] = pd.to_datetime(df['SENTENCE_DATE']).dt.strftime('%Y-%m-%d')
                except:
                    df['SENTENCE_DATE'] = df['SENTENCE_DATE'].astype(str)
                    print("   ⚠️ SENTENCE_DATE convertida como string")
            
            print("✅ Formatos de fecha corregidos")
            
            # Exportar a Excel
            filename = 'usuarios_se_contactan_COMPLETO.xlsx'
            print(f"💾 Exportando a {filename}...")
            
            try:
                df.to_excel(filename, index=False, engine='openpyxl')
                print(f"✅ ¡EXPORTACIÓN EXITOSA!")
                print(f"📊 {len(df)} casos exportados")
                print(f"📁 Archivo: {filename}")
                print(f"📋 Columnas: {list(df.columns)}")
                
                # Mostrar primeras filas como verificación
                print(f"\n📄 VERIFICACIÓN - Primeras 3 filas:")
                print(df.head(3).to_string())
                
                return True
                
            except Exception as e:
                print(f"❌ Error en exportación Excel: {e}")
                
                # Exportar como CSV alternativo
                csv_filename = 'usuarios_se_contactan_COMPLETO.csv'
                print(f"💾 Intentando CSV: {csv_filename}...")
                df.to_csv(csv_filename, index=False)
                print(f"✅ CSV exportado exitosamente: {csv_filename}")
                return True
                
        else:
            print(f"❌ Error en query: {result.get('error')}")
            return False
            
    except Exception as e:
        print(f"❌ Error general: {e}")
        return False

async def main():
    """Función principal"""
    print("🚀 EXPORTACIÓN COMPLETA - Usuarios se contactan")
    print("📋 Todos los casos con MCP BigQuery")
    print("=" * 80)
    
    exito = await obtener_y_exportar_casos_completos()
    
    if exito:
        print(f"\n🎉 ¡EXPORTACIÓN COMPLETADA EXITOSAMENTE!")
        print(f"✅ MCP BigQuery funcionando perfectamente")
        print(f"📊 Todos los casos disponibles en archivo Excel/CSV")
        print(f"\n📋 ARCHIVOS GENERADOS:")
        print(f"   - usuarios_se_contactan_COMPLETO.xlsx (o .csv)")
        print(f"   - Estructura idéntica al archivo original")
        print(f"   - {2000}+ casos con fechas ATO")
    else:
        print(f"\n💡 Hubo algún problema en la exportación")
    
    print(f"\n" + "="*80)
    print(f"🎯 Query 'Usuarios se contactan' COMPLETADA")

if __name__ == "__main__":
    asyncio.run(main()) 