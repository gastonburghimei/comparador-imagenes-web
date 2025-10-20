#!/usr/bin/env python3
"""
🔗 BASE DE CRUCES CONOCIDOS
Información manual de cruces de riesgo para usuarios específicos
"""

# Cruces de riesgo conocidos basados en información proporcionada
CRUCES_CONOCIDOS = {
    "1348718991": {
        "tiene_cruces": True,
        "usuarios_relacionados": ["1879313474", "749782133"],
        "tipos_cruces": [
            {
                "tipo": "Usuarios fraudulentos",
                "cantidad": 2,
                "detalles": "Conectado a usuarios con infracciones CUENTA_HACKER"
            }
        ],
        "descripcion": "Usuario con cruces confirmados con otros casos CUENTA_HACKER"
    },
    
    "1879313474": {
        "tiene_cruces": True,
        "usuarios_relacionados": ["1348718991", "749782133"],
        "tipos_cruces": [
            {
                "tipo": "Usuarios fraudulentos", 
                "cantidad": 2,
                "detalles": "Parte de red de usuarios con infracciones CUENTA_HACKER"
            }
        ],
        "descripcion": "Usuario relacionado con red de fraude"
    },
    
    "749782133": {
        "tiene_cruces": True,
        "usuarios_relacionados": ["1348718991", "1879313474"],
        "tipos_cruces": [
            {
                "tipo": "Usuarios fraudulentos",
                "cantidad": 2, 
                "detalles": "Parte de red de usuarios con infracciones CUENTA_HACKER"
            }
        ],
        "descripcion": "Usuario relacionado con red de fraude"
    },
    
    "1539173000": {
        "tiene_cruces": True,
        "usuarios_relacionados": ["1348718991", "1879313474", "749782133"],
        "tipos_cruces": [
            {
                "tipo": "Usuarios fraudulentos",
                "cantidad": 3,
                "detalles": "Conectado a red extendida de usuarios con infracciones CUENTA_HACKER"
            },
            {
                "tipo": "Device/Fingerprint",
                "cantidad": 2,
                "detalles": "Dispositivos compartidos con usuarios fraudulentos"
            }
        ],
        "descripcion": "Usuario con marcas relevantes y cruces múltiples detectados"
    }
}

def verificar_cruces_usuario(user_id):
    """
    Verificar cruces de riesgo para un usuario específico
    
    Args:
        user_id (str): ID del usuario a verificar
        
    Returns:
        dict: Información de cruces o None si no hay cruces conocidos
    """
    
    user_id_str = str(user_id)
    
    if user_id_str in CRUCES_CONOCIDOS:
        return CRUCES_CONOCIDOS[user_id_str]
    
    # Si no está en la base de cruces conocidos, asumir sin cruces
    return {
        "tiene_cruces": False,
        "usuarios_relacionados": [],
        "tipos_cruces": [],
        "descripcion": "Sin cruces de riesgo detectados en base conocida"
    }

def agregar_cruce_usuario(user_id, usuarios_relacionados, tipos_cruces, descripcion=""):
    """
    Agregar nuevo cruce de usuario a la base
    
    Args:
        user_id (str): ID del usuario
        usuarios_relacionados (list): Lista de usuarios relacionados
        tipos_cruces (list): Lista de tipos de cruces detectados
        descripcion (str): Descripción del cruce
    """
    
    CRUCES_CONOCIDOS[str(user_id)] = {
        "tiene_cruces": True,
        "usuarios_relacionados": usuarios_relacionados,
        "tipos_cruces": tipos_cruces,
        "descripcion": descripcion
    }

def listar_todos_cruces():
    """Listar todos los cruces conocidos"""
    return CRUCES_CONOCIDOS

if __name__ == "__main__":
    # Prueba de la base de cruces
    print("🔗 BASE DE CRUCES CONOCIDOS")
    print("=" * 50)
    
    for user_id, info in CRUCES_CONOCIDOS.items():
        print(f"\n👤 Usuario: {user_id}")
        print(f"   🔗 Tiene cruces: {info['tiene_cruces']}")
        print(f"   👥 Relacionados: {', '.join(info['usuarios_relacionados'])}")
        print(f"   📋 Tipos: {len(info['tipos_cruces'])} tipos detectados")
        for cruce in info['tipos_cruces']:
            print(f"     • {cruce['tipo']}: {cruce['detalles']}")
        print(f"   📝 Descripción: {info['descripcion']}")
    
    print(f"\n✅ Total usuarios con cruces: {len(CRUCES_CONOCIDOS)}")

