# 💬 Configuración de Google Chat

Guía paso a paso para configurar el envío de mensajes por Google Chat.

## 🎯 **Opción 1: Webhook (RECOMENDADO - Más Fácil)**

### **Paso 1: Crear Webhook en Google Chat**

1. **Abre Google Chat** en tu navegador
2. **Ve al espacio** donde quieres recibir los mensajes
3. **Haz clic en el nombre del espacio** (arriba)
4. **Selecciona "Configurar webhooks"**
5. **Haz clic en "+ AGREGAR WEBHOOK"**
6. **Configura el webhook:**
   - **Nombre**: `Sistema Alarmas`
   - **URL de avatar**: `https://developers.google.com/chat/images/quickstart-app-avatar.png`
   - **Descripción**: `Bot para reportes automáticos de alarmas`
7. **Haz clic en "GUARDAR"**
8. **Copia la URL del webhook** que se genera

### **Paso 2: Configurar en Apps Script**

En tu archivo `Configuracion.gs`, actualiza:

```javascript
function obtenerConfiguracionChat() {
  return {
    WEBHOOK_URL: 'TU_WEBHOOK_URL_AQUI', // ← Pegar la URL copiada
    EMAIL_RESPALDO: false, // Solo usar Chat
  };
}
```

### **Paso 3: ¡Listo para Probar!**

Ejecuta tu script y los mensajes aparecerán en el espacio de Chat.

---

## 🎯 **Opción 2: Google Chat API (Avanzado)**

### **Paso 1: Habilitar Google Chat API**

1. **Ve a [Google Cloud Console](https://console.cloud.google.com)**
2. **Selecciona tu proyecto** (o crea uno nuevo)
3. **Ve a "APIs y servicios" → "Biblioteca"**
4. **Busca "Google Chat API"**
5. **Haz clic en "HABILITAR"**

### **Paso 2: Crear Credenciales**

1. **Ve a "APIs y servicios" → "Credenciales"**
2. **Haz clic en "+ CREAR CREDENCIALES"**
3. **Selecciona "Clave de API"**
4. **Copia la clave generada**

### **Paso 3: Obtener Space ID**

1. **Abre Google Chat** en navegador
2. **Ve al espacio** donde quieres enviar mensajes
3. **Mira la URL**, debe ser algo como:**
   ```
   https://chat.google.com/room/AAAAMtF5i3s
   ```
4. **El Space ID es:** `spaces/AAAAMtF5i3s`

### **Paso 4: Configurar Apps Script**

1. **En Apps Script, ve a "Servicios"**
2. **Haz clic en "+" y agrega "Google Chat API"**
3. **En `Configuracion.gs`:**

```javascript
function obtenerConfiguracionChat() {
  return {
    SPACE_ID: 'spaces/TU_SPACE_ID_AQUI', // ← Space ID del paso 3
    EMAIL_RESPALDO: false,
  };
}
```

---

## 🎯 **Opción 3: Mensajes Directos (DM)**

Para enviar mensajes directos a cada usuario individualmente:

### **Configuración:**

```javascript
function obtenerConfiguracionChat() {
  return {
    CREAR_DM_DIRECTO: true,
    EMAIL_RESPALDO: false,
  };
}
```

**Nota:** Requiere que el bot tenga permisos para crear DMs.

---

## 🧪 **Pruebas de Configuración**

### **Función de Prueba**

Agrega esta función a `AlarmaProcessor.gs` para probar:

```javascript
function probarGoogleChat() {
  const mensaje = `🧪 **Mensaje de Prueba**\n\n✅ Google Chat configurado correctamente!\n\nFecha: ${new Date().toLocaleString('es-ES')}`;
  
  try {
    enviarMensajeChat('Usuario Prueba', mensaje);
    console.log('✅ Prueba exitosa!');
  } catch (error) {
    console.error('❌ Error en prueba:', error);
  }
}
```

### **Ejecutar Prueba:**

1. **En Apps Script:** Selecciona `probarGoogleChat`
2. **Haz clic en "Ejecutar"**
3. **Verifica que aparezca el mensaje en Chat**

---

## 🎨 **Formato de Mensajes en Chat**

El sistema envía mensajes con cards bonitas que incluyen:

- **Header** con título y avatar
- **Contenido** del reporte formateado
- **Colores** y emojis para mejor visualización

### **Ejemplo de mensaje:**

```
🚨 Reporte de Alarmas
Para: Juan Pérez

📊 Total de casos: 3

📋 Resumen por tipo:
• Acceso no autorizado: 2 caso(s)
• Intento de login fallido: 1 caso(s)

📝 Detalle de casos:

1. Caso ID: C001
   📅 Fecha: 2024-01-15
   🏷️ Tipo: Acceso no autorizado
```

---

## 🚨 **Solución de Problemas**

### **"Webhook not found"**
- Verifica que la URL del webhook sea correcta
- Asegúrate que el webhook esté activo en Google Chat

### **"API not enabled"**
- Habilita Google Chat API en Google Cloud Console
- Verifica que el proyecto sea correcto

### **"Permission denied"**
- Agrega el servicio Google Chat API en Apps Script
- Verifica permisos del bot en el espacio

### **"Space not found"**
- Verifica que el Space ID sea correcto
- Asegúrate que el bot esté agregado al espacio

---

## ✅ **Verificación Final**

Para confirmar que todo funciona:

1. **Ejecuta** `probarGoogleChat()`
2. **Revisa** que aparezca el mensaje en Chat
3. **Ejecuta** `procesarAlarmasDiarias()` con un email real
4. **Verifica** que los reportes lleguen a Chat

**¡Tu sistema ya está enviando por Google Chat!** 🎉 