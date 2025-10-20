/**
 * SISTEMA AUTOMATIZADO DE PROCESAMIENTO DE ALARMAS
 * Procesa emails diarios y envía mensajes personalizados por Meet
 */

// Configuración principal
const CONFIG = {
  EMAIL_SUBJECT_FILTER: 'alarma', // Ajusta según el asunto de tus emails
  EMAIL_SENDER: '', // Opcional: filtrar por remitente específico
  PROCESS_TIME_START: 9, // Hora de inicio para procesar (9 AM)
  PROCESS_TIME_END: 11,  // Hora límite para procesar (11 AM)
  CHAT_SPACE_ID: '', // ID del espacio de Google Chat (se configura después)
};

/**
 * Función principal que se ejecuta automáticamente
 */
function procesarAlarmasDiarias() {
  try {
    console.log('🚀 Iniciando procesamiento de alarmas...');
    
    // 1. Buscar emails de alarma del día
    const emailsAlarma = buscarEmailsAlarma();
    
    if (emailsAlarma.length === 0) {
      console.log('📧 No se encontraron emails de alarma para procesar');
      return;
    }
    
    // 2. Procesar cada email
    for (const email of emailsAlarma) {
      const datosAlarma = extraerDatosDelEmail(email);
      
      if (datosAlarma.length > 0) {
        // 3. Agrupar casos por usuario
        const casosPorUsuario = agruparCasosPorUsuario(datosAlarma);
        
        // 4. Enviar mensajes personalizados
        enviarMensajesPorMeet(casosPorUsuario);
        
        // 5. Marcar email como procesado (agregar etiqueta)
        marcarEmailComoProcesado(email);
      }
    }
    
    console.log('✅ Procesamiento completado exitosamente');
    
  } catch (error) {
    console.error('❌ Error en procesamiento:', error);
    enviarNotificacionError(error);
  }
}

/**
 * Buscar emails de alarma en Gmail
 */
function buscarEmailsAlarma() {
  const hoy = new Date();
  const fechaFiltro = Utilities.formatDate(hoy, Session.getScriptTimeZone(), 'yyyy/MM/dd');
  
  // Query para buscar emails de alarma del día
  let query = `is:unread after:${fechaFiltro}`;
  
  if (CONFIG.EMAIL_SUBJECT_FILTER) {
    query += ` subject:${CONFIG.EMAIL_SUBJECT_FILTER}`;
  }
  
  if (CONFIG.EMAIL_SENDER) {
    query += ` from:${CONFIG.EMAIL_SENDER}`;
  }
  
  const threads = GmailApp.search(query, 0, 10);
  const emails = [];
  
  for (const thread of threads) {
    const messages = thread.getMessages();
    for (const message of messages) {
      if (!message.isUnread()) continue;
      
      const horaEmail = message.getDate().getHours();
      if (horaEmail >= CONFIG.PROCESS_TIME_START && horaEmail <= CONFIG.PROCESS_TIME_END) {
        emails.push(message);
      }
    }
  }
  
  console.log(`📬 Encontrados ${emails.length} emails de alarma para procesar`);
  return emails;
}

/**
 * Extraer datos estructurados del email
 */
function extraerDatosDelEmail(email) {
  const cuerpoEmail = email.getPlainBody();
  const lineas = cuerpoEmail.split('\n').map(linea => linea.trim()).filter(linea => linea);
  
  const datos = [];
  let encabezadosEncontrados = false;
  let indiceColumnas = {};
  
  for (const linea of lineas) {
    // Buscar la línea de encabezados
    if (linea.includes('user_id') && linea.includes('case_id')) {
      const columnas = linea.split('\t');
      columnas.forEach((columna, index) => {
        indiceColumnas[columna.trim()] = index;
      });
      encabezadosEncontrados = true;
      continue;
    }
    
    // Procesar líneas de datos después de encontrar encabezados
    if (encabezadosEncontrados && linea.includes('\t')) {
      const valores = linea.split('\t');
      
      if (valores.length >= Object.keys(indiceColumnas).length) {
        const registro = {};
        Object.keys(indiceColumnas).forEach(columna => {
          registro[columna] = valores[indiceColumnas[columna]] || '';
        });
        datos.push(registro);
      }
    }
  }
  
  console.log(`📊 Extraídos ${datos.length} registros del email`);
  return datos;
}

/**
 * Agrupar casos por user_id
 */
function agruparCasosPorUsuario(datos) {
  const grupos = {};
  
  for (const registro of datos) {
    const userId = registro.user_id;
    if (!userId) continue;
    
    if (!grupos[userId]) {
      grupos[userId] = {
        user_id: userId,
        nombre: obtenerNombreUsuario(userId),
        casos: []
      };
    }
    
    grupos[userId].casos.push(registro);
  }
  
  console.log(`👥 Agrupados casos para ${Object.keys(grupos).length} usuarios`);
  return grupos;
}

/**
 * Obtener nombre real del usuario basado en user_id
 * IMPORTANTE: Debes personalizar esta función con tu mapeo real
 */
function obtenerNombreUsuario(userId) {
  const mapeoUsuarios = obtenerMapeoUsuarios();
  return mapeoUsuarios[userId] || `Usuario ${userId}`;
}

/**
 * Enviar mensajes personalizados por Google Meet (Chat)
 */
function enviarMensajesPorMeet(casosPorUsuario) {
  for (const userId in casosPorUsuario) {
    const usuario = casosPorUsuario[userId];
    const mensaje = generarMensajePersonalizado(usuario);
    
    try {
      // Enviar mensaje por Google Chat
      enviarMensajeChat(usuario.nombre, mensaje);
      console.log(`✅ Mensaje enviado a ${usuario.nombre}`);
      
    } catch (error) {
      console.error(`❌ Error enviando mensaje a ${usuario.nombre}:`, error);
    }
  }
}

/**
 * Generar mensaje personalizado basado en los casos del usuario
 */
function generarMensajePersonalizado(usuario) {
  const nombre = usuario.nombre;
  const totalCasos = usuario.casos.length;
  
  // Analizar tipos de infracciones
  const tiposInfraccion = {};
  const casosDetalle = [];
  
  for (const caso of usuario.casos) {
    const tipo = caso.infraction_type || 'No especificado';
    tiposInfraccion[tipo] = (tiposInfraccion[tipo] || 0) + 1;
    
    casosDetalle.push({
      case_id: caso.case_id,
      tipo: tipo,
      fecha: caso.fecha_accion,
      comentario: caso.detail_comment || 'Sin comentarios'
    });
  }
  
  // Construir mensaje
  let mensaje = `🚨 **Reporte Diario de Alarmas - ${nombre}**\n\n`;
  mensaje += `📊 **Total de casos:** ${totalCasos}\n\n`;
  
  // Resumen por tipo de infracción
  mensaje += `📋 **Resumen por tipo:**\n`;
  for (const tipo in tiposInfraccion) {
    mensaje += `• ${tipo}: ${tiposInfraccion[tipo]} caso(s)\n`;
  }
  
  mensaje += `\n📝 **Detalle de casos:**\n`;
  
  // Detalles de cada caso (limitar a 5 para no hacer el mensaje muy largo)
  const casosAMostrar = casosDetalle.slice(0, 5);
  for (let i = 0; i < casosAMostrar.length; i++) {
    const caso = casosAMostrar[i];
    mensaje += `\n${i + 1}. **Caso ID:** ${caso.case_id}\n`;
    mensaje += `   📅 Fecha: ${caso.fecha}\n`;
    mensaje += `   🏷️ Tipo: ${caso.tipo}\n`;
    if (caso.comentario !== 'Sin comentarios') {
      mensaje += `   💬 Comentario: ${caso.comentario}\n`;
    }
  }
  
  if (totalCasos > 5) {
    mensaje += `\n... y ${totalCasos - 5} caso(s) adicional(es)\n`;
  }
  
  mensaje += `\n⚡ *Mensaje generado automáticamente - ${new Date().toLocaleString('es-ES')}*`;
  
  return mensaje;
}

/**
 * Enviar mensaje por Google Chat
 */
function enviarMensajeChat(nombreUsuario, mensaje) {
  try {
    const configChat = obtenerConfiguracionChat();
    
    // Opción 1: Usando Webhook URL (más fácil de configurar)
    if (configChat.WEBHOOK_URL) {
      enviarPorWebhook(configChat.WEBHOOK_URL, mensaje, nombreUsuario);
      console.log(`✅ Mensaje enviado por webhook a ${nombreUsuario}`);
      return;
    }
    
    // Opción 2: Usando Google Chat API directamente
    if (configChat.SPACE_ID) {
      enviarPorChatAPI(configChat.SPACE_ID, mensaje, nombreUsuario);
      console.log(`✅ Mensaje enviado por Chat API a ${nombreUsuario}`);
      return;
    }
    
    // Opción 3: Crear DM directo (si está configurado)
    const emailUsuario = obtenerEmailUsuario(nombreUsuario);
    if (emailUsuario && configChat.CREAR_DM_DIRECTO) {
      enviarDMDirecto(emailUsuario, mensaje);
      console.log(`✅ DM directo enviado a ${nombreUsuario}`);
      return;
    }
    
    throw new Error('No hay configuración de Google Chat disponible');
    
  } catch (error) {
    console.error(`❌ Error enviando mensaje por Chat a ${nombreUsuario}:`, error);
    
    // Solo usar email como respaldo si está habilitado
    if (obtenerConfiguracionChat().EMAIL_RESPALDO) {
      console.log('🔄 Usando email como respaldo...');
      enviarMensajePorEmail(nombreUsuario, mensaje);
    } else {
      throw error;
    }
  }
}

/**
 * Enviar mensaje usando Webhook de Google Chat
 */
function enviarPorWebhook(webhookUrl, mensaje, nombreUsuario) {
  const payload = {
    text: mensaje,
    cards: [{
      header: {
        title: `🚨 Reporte de Alarmas`,
        subtitle: `Para: ${nombreUsuario}`,
        imageUrl: 'https://developers.google.com/chat/images/quickstart-app-avatar.png'
      },
      sections: [{
        widgets: [{
          textParagraph: {
            text: mensaje
          }
        }]
      }]
    }]
  };
  
  const options = {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    payload: JSON.stringify(payload)
  };
  
  const response = UrlFetchApp.fetch(webhookUrl, options);
  
  if (response.getResponseCode() !== 200) {
    throw new Error(`Error en webhook: ${response.getResponseCode()} - ${response.getContentText()}`);
  }
}

/**
 * Enviar mensaje usando Google Chat API directamente
 */
function enviarPorChatAPI(spaceId, mensaje, nombreUsuario) {
  // Habilitar Google Chat API en Google Cloud Console primero
  try {
    const chatMessage = {
      text: mensaje,
      cards: [{
        header: {
          title: `🚨 Reporte de Alarmas - ${nombreUsuario}`,
          imageUrl: 'https://developers.google.com/chat/images/quickstart-app-avatar.png'
        }
      }]
    };
    
    // Usar la API avanzada de Google Chat
    const response = Chat.Spaces.Messages.create(chatMessage, spaceId);
    console.log('Respuesta de Chat API:', response);
    
  } catch (error) {
    console.error('Error usando Chat API:', error);
    // Fallback a método webhook si está disponible
    const configChat = obtenerConfiguracionChat();
    if (configChat.WEBHOOK_URL) {
      enviarPorWebhook(configChat.WEBHOOK_URL, mensaje, nombreUsuario);
    } else {
      throw error;
    }
  }
}

/**
 * Crear y enviar mensaje directo (DM) a usuario específico
 */
function enviarDMDirecto(emailUsuario, mensaje) {
  try {
    // Crear espacio DM con el usuario
    const space = Chat.Spaces.create({
      type: 'DM',
      singleUserBotDm: true,
      spaceDetails: {
        description: 'Reporte automático de alarmas'
      }
    });
    
    // Enviar mensaje al espacio DM
    const chatMessage = {
      text: mensaje
    };
    
    Chat.Spaces.Messages.create(chatMessage, space.name);
    console.log(`DM creado y mensaje enviado a ${emailUsuario}`);
    
  } catch (error) {
    console.error('Error creando DM:', error);
    throw error;
  }
}

/**
 * Obtener email del usuario por nombre
 */
function obtenerEmailUsuario(nombreUsuario) {
  const mapeoEmails = obtenerMapeoEmails();
  return mapeoEmails[nombreUsuario] || null;
}

/**
 * Enviar mensaje por email como método de respaldo
 */
function enviarMensajePorEmail(nombreUsuario, mensaje) {
  const emailsUsuarios = obtenerMapeoEmails();
  const emailDestino = emailsUsuarios[nombreUsuario];
  
  if (emailDestino) {
    GmailApp.sendEmail(
      emailDestino,
      `🚨 Reporte Diario de Alarmas - ${nombreUsuario}`,
      mensaje
    );
    console.log(`📧 Email enviado a ${emailDestino}`);
  } else {
    console.log(`⚠️ No se encontró email para ${nombreUsuario}`);
  }
}

/**
 * Marcar email como procesado
 */
function marcarEmailComoProcesado(email) {
  // Crear etiqueta si no existe
  let etiqueta;
  try {
    etiqueta = GmailApp.getUserLabelByName('Alarmas/Procesado');
  } catch (e) {
    etiqueta = GmailApp.createLabel('Alarmas/Procesado');
  }
  
  // Aplicar etiqueta al email
  const thread = email.getThread();
  thread.addLabel(etiqueta);
  thread.markAsRead();
}

/**
 * Enviar notificación de error
 */
function enviarNotificacionError(error) {
  const mensaje = `❌ Error en procesamiento de alarmas:\n\n${error.toString()}\n\nFecha: ${new Date()}`;
  
  // Enviar notificación al administrador
  GmailApp.sendEmail(
    Session.getActiveUser().getEmail(),
    '🚨 Error en Sistema de Alarmas',
    mensaje
  );
}

/**
 * Función para configurar trigger automático
 * Ejecutar una vez para configurar la ejecución diaria
 */
function configurarTriggerDiario() {
  // Eliminar triggers existentes
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === 'procesarAlarmasDiarias') {
      ScriptApp.deleteTrigger(trigger);
    }
  });
  
  // Crear nuevo trigger para ejecutar diariamente a las 9:30 AM
  ScriptApp.newTrigger('procesarAlarmasDiarias')
    .timeBased()
    .everyDays(1)
    .atHour(9)
    .create();
    
  console.log('✅ Trigger diario configurado correctamente');
} 