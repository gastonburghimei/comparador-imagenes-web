# 🚨 Sistema Automatizado de Procesamiento de Alarmas

Sistema que procesa emails diarios de alarmas y envía mensajes personalizados por Google Meet/Chat.

## 📋 **Requisitos Previos**

- Cuenta de Gmail/Google Workspace
- Acceso a Google Apps Script
- Permisos para crear triggers automáticos

## 🛠️ **Instalación Paso a Paso**

### **Paso 1: Crear el Proyecto en Google Apps Script**

1. Ve a [script.google.com](https://script.google.com)
2. Haz clic en **"Nuevo proyecto"**
3. Cambia el nombre a **"Sistema Alarmas Automatizado"**

### **Paso 2: Agregar los Archivos**

1. **Elimina el archivo** `Code.gs` por defecto
2. **Crea los siguientes archivos:**
   - `AlarmaProcessor.gs` (archivo principal)
   - `Configuracion.gs` (configuración personalizada)

3. **Copia el código** de cada archivo a su respectivo archivo en Apps Script

### **Paso 3: Personalizar la Configuración**

En el archivo `Configuracion.gs`, actualiza:

#### **3.1 Mapeo de Usuarios**
```javascript
function obtenerMapeoUsuarios() {
  return {
    '12345': 'Juan Pérez',        // ← Cambia por tus user_id reales
    '12346': 'María González',
    // Agregar todos tus usuarios aquí
  };
}
```

#### **3.2 Mapeo de Emails**
```javascript
function obtenerMapeoEmails() {
  return {
    'Juan Pérez': 'juan.perez@tuempresa.com',  // ← Cambia por emails reales
    'María González': 'maria.gonzalez@tuempresa.com',
    // Agregar todos los emails aquí
  };
}
```

#### **3.3 Configurar Filtros de Email**
```javascript
function obtenerConfiguracionFiltros() {
  return {
    PALABRAS_CLAVE_ASUNTO: ['alarma'],  // ← Palabras en el asunto de tus emails
    REMITENTE_ESPECIFICO: 'sistema@tuempresa.com',  // ← Email remitente de alarmas
    HORA_INICIO: 9,   // ← Hora cuando empezar a buscar
    HORA_FIN: 11,     // ← Hora límite para buscar
  };
}
```

### **Paso 4: Configurar Permisos**

1. En Apps Script, haz clic en **"Ejecutar"** la función `procesarAlarmasDiarias`
2. **Autoriza los permisos** cuando se soliciten:
   - Acceso a Gmail
   - Envío de emails
   - Creación de triggers

### **Paso 5: Configurar Ejecución Automática**

1. **Ejecuta la función** `configurarTriggerDiario()` una sola vez
2. Esto creará un trigger que ejecute el script diariamente a las 9:30 AM

### **Paso 6: Configurar Google Chat (Opcional)**

Para enviar mensajes por Google Chat en lugar de email:

1. Ve a [Google Chat API Console](https://console.developers.google.com)
2. Habilita la **Google Chat API**
3. Obtén el **Space ID** de tu espacio de chat
4. Actualiza la configuración en `Configuracion.gs`

## 🧪 **Pruebas**

### **Prueba Manual**
1. Ejecuta `procesarAlarmasDiarias()` manualmente
2. Revisa los logs en **"Executions"**
3. Verifica que se envíen los mensajes

### **Prueba con Email de Ejemplo**
1. Envíate un email con el formato de alarma
2. Asegúrate que contenga los headers: `user_id`, `case_id`, etc.
3. Ejecuta el script manualmente

## 📊 **Monitoreo**

### **Ver Logs**
- En Apps Script: **"Executions"** → Seleccionar ejecución → Ver logs

### **Verificar Trigger**
- En Apps Script: **"Triggers"** → Verificar que esté activo

### **Notificaciones de Error**
- Si hay errores, recibirás un email automáticamente

## 🔧 **Personalización Avanzada**

### **Modificar Formato de Mensajes**
En `AlarmaProcessor.gs`, función `generarMensajePersonalizado()`:
```javascript
let mensaje = `🚨 **Tu formato personalizado aquí**\n\n`;
```

### **Agregar Más Filtros**
En `buscarEmailsAlarma()`, modificar la query de búsqueda:
```javascript
let query = `is:unread after:${fechaFiltro} subject:tu-filtro`;
```

### **Cambiar Horario de Ejecución**
En `configurarTriggerDiario()`:
```javascript
.atHour(10)  // Cambiar hora
```

## 🚨 **Solución de Problemas**

### **No se Procesan Emails**
- Verificar filtros de búsqueda en `buscarEmailsAlarma()`
- Comprobar que los emails no estén marcados como leídos
- Revisar horario de procesamiento

### **No se Envían Mensajes**
- Verificar mapeo de emails en `Configuracion.gs`
- Comprobar permisos de Gmail
- Revisar logs de errores

### **Trigger No Funciona**
- Volver a ejecutar `configurarTriggerDiario()`
- Verificar en **"Triggers"** que esté activo
- Comprobar límites de ejecución de Google Apps Script

## 📧 **Formato de Email Esperado**

El email debe contener una tabla con estas columnas:
```
user_id	case_id	admin_id	script_rule_id	fecha_accion	infraction_type	color_de_tarjeta	sentence_datetime	sit_site_id	detection_type	detail_comment	fecha_baja_string	event_id
12345	C001	A001	R001	2024-01-15	VIOLATION_TYPE_1	red	2024-01-15 10:00:00	S001	AUTO	Acceso no autorizado	2024-01-15	E001
```

## 🔄 **Mantenimiento**

### **Actualizar Usuarios**
- Modificar `obtenerMapeoUsuarios()` en `Configuracion.gs`
- No requiere reiniciar el trigger

### **Cambiar Configuración**
- Editar funciones en `Configuracion.gs`
- Los cambios se aplican en la siguiente ejecución

### **Backup**
- Descargar los archivos `.gs` periódicamente
- Exportar configuración de usuarios

## 🎯 **Ejemplo de Mensaje Generado**

```
🚨 **Reporte Diario de Alarmas - Juan Pérez**

📊 **Total de casos:** 3

📋 **Resumen por tipo:**
• Acceso no autorizado: 2 caso(s)
• Intento de login fallido: 1 caso(s)

📝 **Detalle de casos:**

1. **Caso ID:** C001
   📅 Fecha: 2024-01-15
   🏷️ Tipo: Acceso no autorizado
   💬 Comentario: Login desde IP no reconocida

⚡ *Mensaje generado automáticamente - 15/1/2024 9:30:00*
```

## 📞 **Soporte**

Si necesitas ayuda:
1. Revisa los logs en Apps Script
2. Verifica la configuración en `Configuracion.gs`
3. Comprueba que los emails tengan el formato correcto

---

**¡Listo! Tu sistema automatizado ya está funcionando** 🎉 