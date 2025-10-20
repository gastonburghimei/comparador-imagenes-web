/**
 * ARCHIVO DE CONFIGURACIÓN
 * Personaliza aquí todos los mapeos y configuraciones del sistema
 */

/**
 * MAPEO DE USER_ID A NOMBRES REALES
 * Actualiza esta función con los user_id y nombres reales de tu organización
 */
function obtenerMapeoUsuarios() {
  return {
    // Formato: 'user_id': 'Nombre Completo'
    '12345': 'Juan Pérez',
    '12346': 'María González', 
    '12347': 'Carlos Rodríguez',
    '12348': 'Ana López',
    '12349': 'Luis Martínez',
    '12350': 'Pedro Sánchez',
    '12351': 'Laura Torres',
    '12352': 'Miguel Fernández',
    '12353': 'Carmen Ruiz',
    '12354': 'Antonio Morales',
    
    // AGREGAR MÁS USUARIOS AQUÍ:
    // 'tu_user_id': 'Tu Nombre',
    // 'otro_user_id': 'Otro Nombre',
  };
}

/**
 * MAPEO DE NOMBRES A EMAILS
 * Para envío de mensajes por email como respaldo
 */
function obtenerMapeoEmails() {
  return {
    // Formato: 'Nombre Completo': 'email@empresa.com'
    'Juan Pérez': 'juan.perez@empresa.com',
    'María González': 'maria.gonzalez@empresa.com',
    'Carlos Rodríguez': 'carlos.rodriguez@empresa.com',
    'Ana López': 'ana.lopez@empresa.com',
    'Luis Martínez': 'luis.martinez@empresa.com',
    'Pedro Sánchez': 'pedro.sanchez@empresa.com',
    'Laura Torres': 'laura.torres@empresa.com',
    'Miguel Fernández': 'miguel.fernandez@empresa.com',
    'Carmen Ruiz': 'carmen.ruiz@empresa.com',
    'Antonio Morales': 'antonio.morales@empresa.com',
    
    // AGREGAR MÁS EMAILS AQUÍ:
    // 'Tu Nombre': 'tu.email@empresa.com',
  };
}

/**
 * CONFIGURACIÓN DE FILTROS DE EMAIL
 * Personaliza cómo buscar los emails de alarma
 */
function obtenerConfiguracionFiltros() {
  return {
    // Palabras clave en el asunto del email
    PALABRAS_CLAVE_ASUNTO: ['alarma', 'reporte', 'casos'],
    
    // Remitente específico (opcional, dejar vacío para cualquier remitente)
    REMITENTE_ESPECIFICO: '', // ej: 'sistema@empresa.com'
    
    // Horario de procesamiento
    HORA_INICIO: 9,  // 9 AM
    HORA_FIN: 11,    // 11 AM
    
    // Días de la semana para procesar (1=Lunes, 7=Domingo)
    DIAS_ACTIVOS: [1, 2, 3, 4, 5], // Lunes a Viernes
  };
}

/**
 * CONFIGURACIÓN DE MENSAJES
 * Personaliza el formato de los mensajes
 */
function obtenerConfiguracionMensajes() {
  return {
    // Incluir emojis en los mensajes
    USAR_EMOJIS: true,
    
    // Máximo número de casos a mostrar en detalle
    MAX_CASOS_DETALLE: 5,
    
    // Incluir comentarios en el mensaje
    INCLUIR_COMENTARIOS: true,
    
    // Plantilla de saludo personalizada
    SALUDO_PERSONALIZADO: true,
  };
}

/**
 * MAPEO DE TIPOS DE INFRACCIÓN A DESCRIPCIONES AMIGABLES
 * Para hacer los mensajes más claros
 */
function obtenerMapeoTiposInfraccion() {
  return {
    'VIOLATION_TYPE_1': 'Acceso no autorizado',
    'VIOLATION_TYPE_2': 'Intento de login fallido',
    'VIOLATION_TYPE_3': 'Comportamiento sospechoso',
    'VIOLATION_TYPE_4': 'Uso indebido de recursos',    
    'VIOLATION_TYPE_5': 'Política de seguridad violada',
    
    // AGREGAR MÁS TIPOS SEGÚN TUS DATOS:
    // 'TU_TIPO': 'Descripción clara',
  };
}

/**
 * CONFIGURACIÓN DE GOOGLE CHAT
 * Para integración con Google Chat (Meet)
 */
function obtenerConfiguracionChat() {
  return {
    // ID del espacio de Google Chat (obtener de la URL del espacio)
    SPACE_ID: '', // ej: 'spaces/AAAAMtF5i3s'
    
    // Webhook URL para Google Chat (RECOMENDADO - más fácil de configurar)
    WEBHOOK_URL: '', // ej: 'https://chat.googleapis.com/v1/spaces/AAAAMtF5i3s/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=...'
    
    // Crear mensajes directos (DM) individuales
    CREAR_DM_DIRECTO: false,
    
    // Usar email como respaldo si Chat falla (cambiar a false para solo Chat)
    EMAIL_RESPALDO: false, // ← Cambiado a false para solo usar Chat
    
    // Formato de mensaje
    USAR_CARDS: true, // Usar cards de Chat para mejor formato
    INCLUIR_IMAGENES: true, // Incluir avatares en los mensajes
  };
}

/**
 * CONFIGURACIÓN DE ADMINISTRADORES
 * Emails que reciben notificaciones de errores
 */
function obtenerAdministradores() {
  return [
    'admin@empresa.com',
    'it.support@empresa.com',
    // Agregar más admins aquí
  ];
}

/**
 * FUNCIÓN AUXILIAR: Obtener todos los user_ids válidos
 */
function obtenerTodosLosUserIds() {
  return Object.keys(obtenerMapeoUsuarios());
}

/**
 * FUNCIÓN AUXILIAR: Validar si un user_id es conocido
 */
function esUserIdValido(userId) {
  return obtenerMapeoUsuarios().hasOwnProperty(userId);
}

/**
 * FUNCIÓN AUXILIAR: Obtener estadísticas de configuración
 */
function obtenerEstadisticasConfiguracion() {
  const usuarios = obtenerMapeoUsuarios();
  const emails = obtenerMapeoEmails();
  
  return {
    totalUsuarios: Object.keys(usuarios).length,
    totalEmails: Object.keys(emails).length,
    usuariosSinEmail: Object.keys(usuarios).filter(nombre => !emails[usuarios[nombre]]).length,
    fechaActualizacion: new Date().toISOString()
  };
} 