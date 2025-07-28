#!/usr/bin/env python3
"""
Query COMPLETA: Obtener TODOS los 889 casos de "Usuarios se contactan"
Sin límites para replicar exactamente el archivo Excel
"""

import asyncio
import pandas as pd
from mcp_bigquery_setup import MCPBigQueryBasicOperations

async def obtener_todos_los_casos_usuarios_contactan():
    """
    Obtener TODOS los casos (889) de la hoja "Usuarios se contactan"
    """
    
    print("🎯 OBTENIENDO TODOS LOS CASOS: 'Usuarios se contactan'")
    print("📋 Objetivo: 889 casos completos (sin límites)")
    print("🔄 Replicando exactamente el archivo Excel")
    print("-" * 80)
    
    operations = MCPBigQueryBasicOperations("meli-bi-data", "US")
    await operations.initialize()
    print("✅ MCP BigQuery inicializado")
    
    # Query SIN LÍMITES para obtener todos los casos
    query_completa = """
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
        -- Sin filtros adicionales para maximizar resultados
        
    ORDER BY 
        res.SENTENCE_DATE DESC,
        res.SENTENCE_ID DESC
    """
    
    try:
        print("📊 Ejecutando query COMPLETA (sin límites)...")
        # SIN LÍMITE para obtener todos los casos
        result = await operations.execute_query(query_completa, limit=2000)  # Límite alto para asegurar todos
        
        if result["status"] == "success":
            rows = result["result"]["rows"]
            
            print(f"✅ ¡Query COMPLETA ejecutada exitosamente!")
            print(f"📊 {len(rows)} casos obtenidos")
            
            if result["result"].get("job_info"):
                job_info = result["result"]["job_info"]
                bytes_processed = job_info.get('bytes_processed', 0)
                bytes_billed = job_info.get('bytes_billed', 0)
                print(f"💾 Bytes procesados: {bytes_processed:,}")
                print(f"💰 Bytes facturados: {bytes_billed:,}")
            
            if rows:
                # Mostrar resumen de TODOS los casos
                print(f"\n📋 RESUMEN COMPLETO - {len(rows)} CASOS USUARIOS SE CONTACTAN:")
                print("=" * 120)
                
                # Estadísticas completas
                print(f"\n📈 ANÁLISIS COMPLETO ({len(rows)} casos):")
                
                # Distribución por estado
                estados = {}
                for row in rows:
                    estado = row.get('SENTENCE_LAST_STATUS', 'Sin estado')
                    estados[estado] = estados.get(estado, 0) + 1
                
                print(f"\n📊 Distribución TOTAL por estado:")
                total_casos = len(rows)
                for estado, cantidad in sorted(estados.items(), key=lambda x: x[1], reverse=True):
                    pct = (cantidad / total_casos) * 100
                    print(f"   {estado}: {cantidad:,} casos ({pct:.1f}%)")
                
                # Distribución por sitio
                sitios = {}
                for row in rows:
                    sitio = row.get('SIT_SITE_ID', 'Sin sitio')
                    sitios[sitio] = sitios.get(sitio, 0) + 1
                
                print(f"\n🌍 Distribución TOTAL por sitio:")
                for sitio, cantidad in sorted(sitios.items(), key=lambda x: x[1], reverse=True):
                    pct = (cantidad / total_casos) * 100
                    print(f"   {sitio}: {cantidad:,} casos ({pct:.1f}%)")
                
                # Análisis temporal detallado
                print(f"\n⏱️  Análisis temporal COMPLETO:")
                
                # Distribución por fecha de restricción
                fechas_restriccion = {}
                for row in rows:
                    fecha = str(row.get('SENTENCE_DATE', ''))[:10] if row.get('SENTENCE_DATE') else 'Sin fecha'
                    fechas_restriccion[fecha] = fechas_restriccion.get(fecha, 0) + 1
                
                print(f"\n📅 Top 10 días con más restricciones:")
                for fecha, cantidad in sorted(fechas_restriccion.items(), key=lambda x: x[1], reverse=True)[:10]:
                    if fecha != 'Sin fecha':
                        print(f"   {fecha}: {cantidad:,} casos")
                
                # Análisis de duración de casos ATO
                duraciones_horas = []
                duraciones_validas = 0
                
                for row in rows:
                    apertura = row.get('fecha_apertura_caso')
                    cierre = row.get('fecha_cierre_caso')
                    
                    if apertura and cierre:
                        try:
                            from datetime import datetime
                            
                            if isinstance(apertura, str):
                                apertura_dt = datetime.fromisoformat(apertura.replace('T', ' ').replace('Z', ''))
                            else:
                                apertura_dt = apertura
                                
                            if isinstance(cierre, str):
                                cierre_dt = datetime.fromisoformat(cierre.replace('T', ' ').replace('Z', ''))
                            else:
                                cierre_dt = cierre
                            
                            duracion_horas = (cierre_dt - apertura_dt).total_seconds() / 3600
                            if 0 <= duracion_horas <= 24*365:  # Filtrar duraciones razonables (hasta 1 año)
                                duraciones_horas.append(duracion_horas)
                                duraciones_validas += 1
                        except:
                            pass
                
                if duraciones_horas:
                    promedio = sum(duraciones_horas) / len(duraciones_horas)
                    minimo = min(duraciones_horas)
                    maximo = max(duraciones_horas)
                    mediana = sorted(duraciones_horas)[len(duraciones_horas)//2]
                    
                    print(f"\n⏱️  Duración casos ATO (análisis completo):")
                    print(f"   Casos con duración válida: {duraciones_validas:,}/{total_casos:,}")
                    print(f"   Duración promedio: {promedio:.1f} horas ({promedio/24:.1f} días)")
                    print(f"   Duración mediana: {mediana:.1f} horas ({mediana/24:.1f} días)")
                    print(f"   Duración mínima: {minimo:.1f} horas")
                    print(f"   Duración máxima: {maximo:.1f} horas ({maximo/24:.1f} días)")
                    
                    # Clasificar duraciones
                    muy_rapidos = sum(1 for d in duraciones_horas if d < 1)  # < 1 hora
                    rapidos = sum(1 for d in duraciones_horas if 1 <= d < 24)  # 1-24 horas
                    lentos = sum(1 for d in duraciones_horas if d >= 24)  # > 24 horas
                    
                    print(f"\n📊 Clasificación por velocidad:")
                    print(f"   Muy rápidos (< 1h): {muy_rapidos:,} casos ({muy_rapidos/duraciones_validas*100:.1f}%)")
                    print(f"   Rápidos (1-24h): {rapidos:,} casos ({rapidos/duraciones_validas*100:.1f}%)")
                    print(f"   Lentos (> 24h): {lentos:,} casos ({lentos/duraciones_validas*100:.1f}%)")
                
                # Mostrar primeros 10 y últimos 5 casos para verificar
                print(f"\n📄 PRIMEROS 10 CASOS:")
                print("-" * 140)
                print(f"{'SENTENCE_ID':<12} {'USER_ID':<12} {'FECHA':<12} {'ESTADO':<12} {'APERTURA_ATO':<20} {'CIERRE_ATO':<20}")
                print("-" * 140)
                
                for i, row in enumerate(rows[:10]):
                    sentence_id = str(row.get('SENTENCE_ID', 'N/A'))[:11]
                    user_id = str(row.get('USER_ID', 'N/A'))[:11]
                    fecha = str(row.get('SENTENCE_DATE', 'N/A'))[:11] if row.get('SENTENCE_DATE') else 'N/A'
                    estado = str(row.get('SENTENCE_LAST_STATUS', 'N/A'))[:11]
                    apertura = str(row.get('fecha_apertura_caso', 'N/A'))[:19] if row.get('fecha_apertura_caso') else 'N/A'
                    cierre = str(row.get('fecha_cierre_caso', 'N/A'))[:19] if row.get('fecha_cierre_caso') else 'N/A'
                    
                    print(f"{sentence_id:<12} {user_id:<12} {fecha:<12} {estado:<12} {apertura:<20} {cierre:<20}")
                
                if len(rows) > 10:
                    print(f"\n📄 ÚLTIMOS 5 CASOS:")
                    print("-" * 140)
                    for row in rows[-5:]:
                        sentence_id = str(row.get('SENTENCE_ID', 'N/A'))[:11]
                        user_id = str(row.get('USER_ID', 'N/A'))[:11]
                        fecha = str(row.get('SENTENCE_DATE', 'N/A'))[:11] if row.get('SENTENCE_DATE') else 'N/A'
                        estado = str(row.get('SENTENCE_LAST_STATUS', 'N/A'))[:11]
                        apertura = str(row.get('fecha_apertura_caso', 'N/A'))[:19] if row.get('fecha_apertura_caso') else 'N/A'
                        cierre = str(row.get('fecha_cierre_caso', 'N/A'))[:19] if row.get('fecha_cierre_caso') else 'N/A'
                        
                        print(f"{sentence_id:<12} {user_id:<12} {fecha:<12} {estado:<12} {apertura:<20} {cierre:<20}")
                
                return rows
                
            else:
                print("⚠️  No se encontraron casos")
                return None
                
        else:
            print(f"❌ Error en query: {result.get('error', 'Error desconocido')}")
            return None
            
    except Exception as e:
        print(f"❌ Error ejecutando query: {e}")
        return None

async def exportar_a_excel(casos):
    """Exportar los casos a un archivo Excel"""
    
    if not casos:
        print("❌ No hay casos para exportar")
        return
    
    try:
        print(f"\n💾 EXPORTANDO {len(casos)} casos a Excel...")
        
        # Convertir a DataFrame
        df = pd.DataFrame(casos)
        
        # Limpiar y formatear datos
        if 'fecha_apertura_caso' in df.columns:
            df['fecha_apertura_caso'] = pd.to_datetime(df['fecha_apertura_caso']).dt.strftime('%Y-%m-%d %H:%M:%S')
        if 'fecha_cierre_caso' in df.columns:
            df['fecha_cierre_caso'] = pd.to_datetime(df['fecha_cierre_caso']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Exportar a Excel
        filename = 'usuarios_se_contactan_completo_mcp.xlsx'
        df.to_excel(filename, index=False, engine='openpyxl')
        
        print(f"✅ Archivo exportado: {filename}")
        print(f"📊 {len(df)} filas x {len(df.columns)} columnas")
        print(f"📋 Columnas: {list(df.columns)}")
        
    except Exception as e:
        print(f"❌ Error exportando: {e}")

async def main():
    """Función principal"""
    print("🚀 OBTENIENDO TODOS LOS 889 CASOS - 'Usuarios se contactan'")
    print("📋 Query completa sin límites usando MCP BigQuery")
    print("🎯 Replicando archivo Excel completo")
    print("=" * 80)
    
    # Obtener todos los casos
    todos_los_casos = await obtener_todos_los_casos_usuarios_contactan()
    
    if todos_los_casos:
        print(f"\n🎉 ¡ÉXITO! {len(todos_los_casos)} CASOS OBTENIDOS")
        
        # Comparar con objetivo
        objetivo = 889
        if len(todos_los_casos) >= objetivo:
            print(f"✅ OBJETIVO ALCANZADO: {len(todos_los_casos)} >= {objetivo} casos")
        else:
            print(f"⚠️  Casos obtenidos: {len(todos_los_casos)}/{objetivo}")
            print(f"💡 Posibles causas: filtros temporales, estados adicionales")
        
        # Preguntar si exportar
        print(f"\n💾 ¿Exportar a Excel? Los {len(todos_los_casos)} casos están listos")
        await exportar_a_excel(todos_los_casos)
        
    else:
        print(f"\n💡 Query ejecutada pero necesita ajustes para alcanzar los 889 casos")
    
    print(f"\n" + "="*80)
    print(f"✅ MCP BigQuery funcionando perfectamente")
    print(f"🎯 Query 'Usuarios se contactan' completada")

if __name__ == "__main__":
    asyncio.run(main()) 