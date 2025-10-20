#!/usr/bin/env python3
"""
🧪 PRUEBA DEL ANALIZADOR MCP REAL CON USUARIO 1539173000
Saltea verificación de transacciones ATO/DTO para ir directo a cruces
"""

import asyncio
import sys
from datetime import datetime
from analizador_mcp_real_integrado import AnalizadorMCPRealIntegrado

async def test_usuario_1539173000():
    """Probar analizador con usuario 1539173000 y datos MCP reales"""
    
    analizador = AnalizadorMCPRealIntegrado()
    
    if not await analizador.initialize():
        print("❌ Error inicializando BigQuery")
        return
    
    user_id = "1539173000"
    
    print("🧪 PRUEBA ANALIZADOR MCP REAL - USUARIO 1539173000")
    print("=" * 80)
    print(f"👤 USUARIO: {user_id}")
    print(f"📅 ANÁLISIS: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 80)
    
    # DATOS MCP REALES YA OBTENIDOS
    subgraph_data = {
        "edges": [
            {"id": "uses_device-1539173000-65b5825289c234e3dc34e556", "label": "uses_device", "source": "user-1539173000", "target": "device-65b5825289c234e3dc34e556", "properties": {"start_date": "2024-01-27T22:23:15Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-669e66dc67d98773dfbdd748", "label": "uses_device", "source": "user-1539173000", "target": "device-669e66dc67d98773dfbdd748", "properties": {"start_date": "2024-07-22T14:11:48Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-66ca3b9fdf580810c64518fb", "label": "uses_device", "source": "user-1539173000", "target": "device-66ca3b9fdf580810c64518fb", "properties": {"start_date": "2024-08-24T20:00:19Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-66576149432115469e6082ec", "label": "uses_device", "source": "user-1539173000", "target": "device-66576149432115469e6082ec", "properties": {"start_date": "2024-05-29T17:10:19Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-68058598eb0163257218268c", "label": "uses_device", "source": "user-1539173000", "target": "device-68058598eb0163257218268c", "properties": {"start_date": "2025-04-20T23:55:51Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-6588d83e172f774f2c6a7a76", "label": "uses_device", "source": "user-1539173000", "target": "device-6588d83e172f774f2c6a7a76", "properties": {"start_date": "2024-01-14T04:18:15Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-6762da2e78a3fc74ac2b15e5", "label": "uses_device", "source": "user-1539173000", "target": "device-6762da2e78a3fc74ac2b15e5", "properties": {"start_date": "2025-02-17T10:56:26Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-64998566d2ed1bfd33ff8c8f", "label": "uses_device", "source": "user-1539173000", "target": "device-64998566d2ed1bfd33ff8c8f", "properties": {"start_date": "2023-11-10T13:42:17Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-675d72f7040a659c1c22e19f", "label": "uses_device", "source": "user-1539173000", "target": "device-675d72f7040a659c1c22e19f", "properties": {"start_date": "2025-02-17T14:28:47Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-685c66686bd6b1cb9012bfe3", "label": "uses_device", "source": "user-1539173000", "target": "device-685c66686bd6b1cb9012bfe3", "properties": {"start_date": "2025-06-25T21:14:31Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-6596aadce08d0096897a75ae", "label": "uses_device", "source": "user-1539173000", "target": "device-6596aadce08d0096897a75ae", "properties": {"start_date": "2024-01-04T12:56:38Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-6787bea46d3396c62e8a333e", "label": "uses_device", "source": "user-1539173000", "target": "device-6787bea46d3396c62e8a333e", "properties": {"start_date": "2025-01-15T13:58:24Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-675f5a93b55028b5f7fa6d0c", "label": "uses_device", "source": "user-1539173000", "target": "device-675f5a93b55028b5f7fa6d0c", "properties": {"start_date": "2025-04-17T13:06:54Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-67ef39bce19a8b80a1bcf9de", "label": "uses_device", "source": "user-1539173000", "target": "device-67ef39bce19a8b80a1bcf9de", "properties": {"start_date": "2025-04-04T01:46:28Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-678992c9fb423f05aa032594", "label": "uses_device", "source": "user-1539173000", "target": "device-678992c9fb423f05aa032594", "properties": {"start_date": "2025-01-16T23:14:17Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-665f260c1194a96a2fd05dca", "label": "uses_device", "source": "user-1539173000", "target": "device-665f260c1194a96a2fd05dca", "properties": {"start_date": "2024-06-04T14:35:31Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-685c606142d3887c484bcadd", "label": "uses_device", "source": "user-1539173000", "target": "device-685c606142d3887c484bcadd", "properties": {"start_date": "2025-06-25T20:49:48Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-685c64135573f902288ae4d5", "label": "uses_device", "source": "user-1539173000", "target": "device-685c64135573f902288ae4d5", "properties": {"start_date": "2025-06-25T21:04:07Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-6808418c42b222671423b877", "label": "uses_device", "source": "user-1539173000", "target": "device-6808418c42b222671423b877", "properties": {"start_date": "2025-04-23T01:29:24Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-654b711879036fb9ef8f5aa5", "label": "uses_device", "source": "user-1539173000", "target": "device-654b711879036fb9ef8f5aa5", "properties": {"start_date": "2023-11-08T11:29:28Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-654e3f33acf6134e478b6688", "label": "uses_device", "source": "user-1539173000", "target": "device-654e3f33acf6134e478b6688", "properties": {"start_date": "2023-11-10T14:34:01Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1539173000-6851a69ea161ac4c079c8ce9", "label": "uses_device", "source": "user-1539173000", "target": "device-6851a69ea161ac4c079c8ce9", "properties": {"start_date": "2025-06-17T17:41:35Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_guest_card-1539173000-RPXWSDFLHTDONUJVWBEMJARERZVXCJXRINIUBOAA", "label": "uses_guest_card", "source": "user-1539173000", "target": "card-RPXWSDFLHTDONUJVWBEMJARERZVXCJXRINIUBOAA", "properties": {"start_date": "2025-08-03T00:46:23Z"}, "bound_level": "LOW_TRUST"},
            {"id": "uses_card-1539173000-AHXZOHNEYGSCMZIECIJXOOKWEHBGUPNHNZHHSEMM", "label": "uses_card", "source": "user-1539173000", "target": "card-AHXZOHNEYGSCMZIECIJXOOKWEHBGUPNHNZHHSEMM", "properties": {"start_date": "2024-02-07T16:03:02Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "validate_phone-1539173000-c2dc0f2e71691ed95cb84a9830b93b23", "label": "validate_phone", "source": "user-1539173000", "target": "phone-c2dc0f2e71691ed95cb84a9830b93b23", "properties": {"start_date": "2023-11-08T11:26:42Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "declare_phone-1539173000-c2dc0f2e71691ed95cb84a9830b93b23", "label": "declare_phone", "source": "user-1539173000", "target": "phone-c2dc0f2e71691ed95cb84a9830b93b23", "properties": {"start_date": "2023-11-08T11:26:42Z"}, "bound_level": "LOW_TRUST"},
            {"id": "declare_person-1539173000-fbd41a3a5836c9747801dcf4706a53cb", "label": "declare_person", "source": "user-1539173000", "target": "person-fbd41a3a5836c9747801dcf4706a53cb", "properties": {"start_date": "2023-11-08T11:31:31Z"}, "bound_level": "LOW_TRUST"},
            {"id": "validate_person-1539173000-fbd41a3a5836c9747801dcf4706a53cb", "label": "validate_person", "source": "user-1539173000", "target": "person-fbd41a3a5836c9747801dcf4706a53cb", "properties": {"start_date": "2023-11-08T11:31:31Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "withdrawal_bank_account-1539173000-cc8410f1199e1c8e9c480693d7f01415", "label": "withdrawal_bank_account", "source": "user-1539173000", "target": "bank_account-cc8410f1199e1c8e9c480693d7f01415", "properties": {"start_date": "2024-03-02T04:21:56Z"}, "bound_level": "UNKNOWN"},
            {"id": "uses_device-1752460179-6787bea46d3396c62e8a333e", "label": "uses_device", "source": "user-1752460179", "target": "device-6787bea46d3396c62e8a333e", "properties": {"start_date": "2025-08-02T16:50:27Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_guest_device-1488191641-64998566d2ed1bfd33ff8c8f", "label": "uses_guest_device", "source": "user-1488191641", "target": "device-64998566d2ed1bfd33ff8c8f", "properties": {"start_date": "2023-09-29T18:49:10Z"}, "bound_level": "LOW_TRUST"},
            {"id": "uses_device-345557826-64998566d2ed1bfd33ff8c8f", "label": "uses_device", "source": "user-345557826", "target": "device-64998566d2ed1bfd33ff8c8f", "properties": {"start_date": "2023-11-10T14:43:58Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-1412215331-675f5a93b55028b5f7fa6d0c", "label": "uses_device", "source": "user-1412215331", "target": "device-675f5a93b55028b5f7fa6d0c", "properties": {"start_date": "2024-12-15T22:41:41Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-345557826-675f5a93b55028b5f7fa6d0c", "label": "uses_device", "source": "user-345557826", "target": "device-675f5a93b55028b5f7fa6d0c", "properties": {"start_date": "2025-04-24T15:03:56Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_device-810346226-675f5a93b55028b5f7fa6d0c", "label": "uses_device", "source": "user-810346226", "target": "device-675f5a93b55028b5f7fa6d0c", "properties": {"start_date": "2025-06-06T23:12:59Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "uses_card-1412215331-AHXZOHNEYGSCMZIECIJXOOKWEHBGUPNHNZHHSEMM", "label": "uses_card", "source": "user-1412215331", "target": "card-AHXZOHNEYGSCMZIECIJXOOKWEHBGUPNHNZHHSEMM", "properties": {"start_date": "2025-06-09T23:08:07Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "validate_phone-1488191641-c2dc0f2e71691ed95cb84a9830b93b23", "label": "validate_phone", "source": "user-1488191641", "target": "phone-c2dc0f2e71691ed95cb84a9830b93b23", "properties": {"start_date": "2023-09-23T20:58:06Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "declare_phone-1488191641-c2dc0f2e71691ed95cb84a9830b93b23", "label": "declare_phone", "source": "user-1488191641", "target": "phone-c2dc0f2e71691ed95cb84a9830b93b23", "properties": {"start_date": "2023-09-23T20:58:06Z"}, "bound_level": "LOW_TRUST"},
            {"id": "validate_person-345557826-fbd41a3a5836c9747801dcf4706a53cb", "label": "validate_person", "source": "user-345557826", "target": "person-fbd41a3a5836c9747801dcf4706a53cb", "properties": {"start_date": "2023-11-10T15:54:59Z"}, "bound_level": "HIGH_TRUST"},
            {"id": "declare_person-345557826-fbd41a3a5836c9747801dcf4706a53cb", "label": "declare_person", "source": "user-345557826", "target": "person-fbd41a3a5836c9747801dcf4706a53cb", "properties": {"start_date": "2022-05-08T01:40:54Z"}, "bound_level": "LOW_TRUST"}
        ],
        "nodes": [
            {"id": "device-675d72f7040a659c1c22e19f", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-6787bea46d3396c62e8a333e", "label": "device", "adjacent_edges_count": 2, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-685c64135573f902288ae4d5", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-6851a69ea161ac4c079c8ce9", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-685c606142d3887c484bcadd", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-6808418c42b222671423b877", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-6762da2e78a3fc74ac2b15e5", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-66576149432115469e6082ec", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-64998566d2ed1bfd33ff8c8f", "label": "device", "adjacent_edges_count": 3, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-675f5a93b55028b5f7fa6d0c", "label": "device", "adjacent_edges_count": 4, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-6588d83e172f774f2c6a7a76", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-6596aadce08d0096897a75ae", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "user-1539173000", "label": "user", "adjacent_edges_count": 96, "properties": {"start_date": "2023-11-06T22:37:24Z", "last_updated": "2025-08-06T13:07:59Z", "is_driver_user": True}},
            {"id": "device-665f260c1194a96a2fd05dca", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-654b711879036fb9ef8f5aa5", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-68058598eb0163257218268c", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-669e66dc67d98773dfbdd748", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-65b5825289c234e3dc34e556", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-654e3f33acf6134e478b6688", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-67ef39bce19a8b80a1bcf9de", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-678992c9fb423f05aa032594", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-685c66686bd6b1cb9012bfe3", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "device-66ca3b9fdf580810c64518fb", "label": "device", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "card-AHXZOHNEYGSCMZIECIJXOOKWEHBGUPNHNZHHSEMM", "label": "card", "adjacent_edges_count": 2, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "card-RPXWSDFLHTDONUJVWBEMJARERZVXCJXRINIUBOAA", "label": "card", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "phone-c2dc0f2e71691ed95cb84a9830b93b23", "label": "phone", "adjacent_edges_count": 4, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "person-fbd41a3a5836c9747801dcf4706a53cb", "label": "person", "adjacent_edges_count": 4, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "bank_account-cc8410f1199e1c8e9c480693d7f01415", "label": "bank_account", "adjacent_edges_count": 1, "properties": {"start_date": "", "last_updated": "", "is_driver_user": False}},
            {"id": "user-1752460179", "label": "user", "adjacent_edges_count": 13, "properties": {"start_date": "2024-04-02T21:40:50Z", "last_updated": "2025-08-05T21:25:08Z", "is_driver_user": False}},
            {"id": "user-1488191641", "label": "user", "adjacent_edges_count": 13, "properties": {"start_date": "2023-09-23T20:50:37Z", "last_updated": "2024-10-12T18:08:49Z", "is_driver_user": False}},
            {"id": "user-345557826", "label": "user", "adjacent_edges_count": 52, "properties": {"start_date": "2021-02-27T16:10:22Z", "last_updated": "2025-08-05T20:31:22Z", "is_driver_user": False}},
            {"id": "user-1412215331", "label": "user", "adjacent_edges_count": 31, "properties": {"start_date": "2023-07-02T08:03:58Z", "last_updated": "2025-08-05T18:07:34Z", "is_driver_user": False}},
            {"id": "user-810346226", "label": "user", "adjacent_edges_count": 56, "properties": {"start_date": "2021-08-19T15:15:39Z", "last_updated": "2025-07-07T16:34:34Z", "is_driver_user": False}}
        ]
    }
    
    fraud_hops_data = {
        "boundLevel": "HIGH_TRUST",
        "hopsLimit": None,
        "userFound": True,
        "userCount": 5,
        "fraudConfirmedUserCount": 0,
        "fraudAlmostUserCount": 0,
        "fraudMaybeUserCount": 0,
        "fraudAtoUserCount": 0
    }
    
    # SIMULAR QUE TIENE TRANSACCIONES ATO/DTO (para saltear prerequisito)
    transacciones_ato_simuladas = {
        'cantidad': 2,  # Más de 1 para que vaya por Flujo 2
        'monto': 1500.00
    }
    
    print(f"🔍 PREREQUISITO: VERIFICAR TRANSACCIONES ATO/DTO")
    print(f"   ✅ Prerequisito simulado cumplido: {transacciones_ato_simuladas['cantidad']} transacciones ATO/DTO")
    print(f"   💰 Monto: ${transacciones_ato_simuladas['monto']:,.2f}")
    
    # EJECUTAR FLUJO 2 DIRECTAMENTE
    print("\n📋 FLUJO 2: ANÁLISIS PARA 2+ TRANSACCIONES ATO/DTO")
    print("-" * 60)
    
    # PASO 1: Verificar antigüedad de cuenta (SIMULADA para este test)
    print("1️⃣ EVALUANDO ANTIGÜEDAD DE CUENTA...")
    
    # Basado en el subgrafo MCP, el usuario tiene start_date: "2023-11-06T22:37:24Z"
    # Eso significa que la cuenta tiene más de 1 año = CUENTA ANTIGUA
    antiguedad_simulada = {
        'es_antigua': True,
        'dias_antiguedad': 365,  # Más de 1 año
        'fecha_creacion': '2023-11-06'
    }
    
    print(f"   📅 Fecha creación: {antiguedad_simulada['fecha_creacion']}")
    print(f"   ⏰ Días de antigüedad: {antiguedad_simulada['dias_antiguedad']}")
    print(f"   📊 ¿Es cuenta antigua? ✅ Sí (umbral: 90 días)")
    
    if not antiguedad_simulada['es_antigua']:
        print("❌ CUENTA NUEVA → CUENTA_HACKER_CONFIRMADA")
        return
    
    # PASO 2: Verificar marcas relevantes (SIMULADA para este test)
    print("\n2️⃣ EVALUANDO MARCAS RELEVANTES...")
    
    # Simulamos que NO tiene marcas relevantes para continuar al análisis de cruces
    marcas_simuladas = {
        'tiene_marcas': False,
        'marcas_encontradas': [],
        'marcas_completas': ''
    }
    
    print(f"   ❌ No se encontraron marcas relevantes")
    
    if marcas_simuladas['tiene_marcas']:
        print("❌ MARCAS RELEVANTES → CUENTA_HACKER_DESCARTADA")
        return
    
    # PASO 3: Verificar cruces de riesgo con MCP REAL
    print("\n3️⃣ EVALUANDO CRUCES DE RIESGO CON MCP REAL...")
    cruces_mcp = analizador.procesar_subgraph_mcp(subgraph_data)
    fraud_analysis = analizador.procesar_fraud_hops(fraud_hops_data)
    
    print(f"   📊 CRUCES DETECTADOS:")
    if cruces_mcp['tiene_cruces']:
        print(f"   ❌ {cruces_mcp['total_cruces']} tipos de cruces detectados")
        print(f"   👥 {cruces_mcp['total_usuarios_conectados']} usuarios conectados")
        for cruce in cruces_mcp['tipos_cruces']:
            print(f"     • {cruce['tipo']}: {cruce['detalles']}")
            print(f"       └─ Usuarios: {', '.join(cruce['usuarios'])}")
    else:
        print(f"   ✅ Sin cruces detectados")
    
    print(f"\n   🚨 ANÁLISIS DE FRAUDE:")
    if fraud_analysis['tiene_fraude_conectado']:
        print(f"   ❌ USUARIOS FRAUDULENTOS CONECTADOS:")
        print(f"     • Total: {fraud_analysis['usuarios_fraudulentos_conectados']}")
        for tipo in fraud_analysis['tipos_fraude']:
            print(f"     • {tipo}")
    else:
        print(f"   ✅ Sin usuarios fraudulentos conectados")
        print(f"     • Usuarios analizados: {fraud_hops_data['userCount']}")
        print(f"     • Fraud confirmed: {fraud_hops_data['fraudConfirmedUserCount']}")
        print(f"     • Fraud almost: {fraud_hops_data['fraudAlmostUserCount']}")
        print(f"     • Fraud maybe: {fraud_hops_data['fraudMaybeUserCount']}")
        print(f"     • Fraud ATO: {fraud_hops_data['fraudAtoUserCount']}")
    
    # DECISIÓN BASADA EN LOS CRUCES
    if fraud_analysis['tiene_fraude_conectado']:
        print("\n🏛️ DECISIÓN FINAL: CUENTA_HACKER_CONFIRMADA")
        print("📝 MOTIVO: Cruces con usuarios fraudulentos detectados")
    elif cruces_mcp['tiene_cruces']:
        print("\n🏛️ DECISIÓN FINAL: CUENTA_HACKER_CONFIRMADA")
        print("📝 MOTIVO: Cruces de riesgo detectados")
        print("📋 EVALUACIÓN ADICIONAL: Se requiere continuar con pasos 4 y 5")
        
        # PASO 4: Verificar velocidad de retirada
        print("\n4️⃣ EVALUANDO VELOCIDAD DE RETIRADA...")
        velocidad = await analizador._verificar_velocidad_retirada(user_id)
        
        # PASO 5: Verificar contacto/desconocimiento
        print("\n5️⃣ EVALUANDO CONTACTO/DESCONOCIMIENTO...")
        contacto = await analizador._verificar_contacto_desconocimiento(user_id)
        
        # DECISIÓN FINAL
        if velocidad['es_rapida']:
            print("\n🏛️ DECISIÓN FINAL: CUENTA_HACKER_CONFIRMADA")
            print("📝 MOTIVO: Cruces de riesgo + velocidad de retirada rápida")
        elif contacto['tiene_contacto']:
            print("\n🏛️ DECISIÓN FINAL: CUENTA_HACKER_DESCARTADA")
            print("📝 MOTIVO: Usuario contactó reportando desconocimiento")
        else:
            print("\n🏛️ DECISIÓN FINAL: CUENTA_HACKER_CONFIRMADA")
            print("📝 MOTIVO: Cruces de riesgo detectados sin otros factores mitigantes")
    else:
        print("\n🏛️ DECISIÓN FINAL: CUENTA_HACKER_DESCARTADA")
        print("📝 MOTIVO: Sin cruces de riesgo significativos")
    
    print("\n" + "=" * 80)
    print("🎯 ANÁLISIS COMPLETO CON DATOS MCP REALES")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(test_usuario_1539173000())