# 🚀 CUANDO TENGAS LOS PERMISOS - Pasos Finales

## ⚡ **UNA VEZ QUE EL ADMIN TE DÉ PERMISOS:**

### **🔑 PASO 1: Crear Service Account (2 minutos)**

1. **Ir a:**
   ```
   https://console.cloud.google.com/iam-admin/serviceaccounts?project=warehouse-cross-pf
   ```

2. **Clic en "CREATE SERVICE ACCOUNT"**

3. **Configurar:**
   - **Nombre:** `bigquery-cursor-dev`
   - **ID:** Se genera automáticamente
   - **Descripción:** `Credenciales para Cursor/VS Code - acceso BigQuery`

4. **Clic "CREATE AND CONTINUE"**

### **🛡️ PASO 2: Asignar Roles (1 minuto)**

**Agregar estos 2 roles exactos:**
1. `BigQuery Data Editor`
2. `BigQuery User`

**Clic "CONTINUE" → "DONE"**

### **📥 PASO 3: Descargar Credenciales (30 segundos)**

1. **Clic** en el Service Account recién creado
2. **Ir a pestaña "KEYS"**
3. **"ADD KEY" → "Create new key" → "JSON"**
4. **Guardar el archivo como:**
   ```
   /Users/gasburghi/Documents/SQL/credentials/service-account-key.json
   ```

### **⚙️ PASO 4: Actualizar Configuración (30 segundos)**

**Editar el archivo `.env`:**
```bash
# Cambiar esta línea:
GOOGLE_APPLICATION_CREDENTIALS=ruta/a/tu/service-account-key.json

# Por esta:
GOOGLE_APPLICATION_CREDENTIALS=./credentials/service-account-key.json
```

### **🧪 PASO 5: ¡PROBAR! (10 segundos)**

**Ejecutar:**
```bash
cd /Users/gasburghi/Documents/SQL
python3 test_warehouse_connection.py
```

**Si sale esto = ÉXITO:**
```
✅ ¡CONEXIÓN EXITOSA!
📊 Proyecto: warehouse-cross-pf
📁 Explorando datasets disponibles...
🎉 ¡INTEGRACIÓN COMPLETADA!
```

---

## 🎯 **DESPUÉS PODRÁS:**

- 🔗 **Conectar Cursor directamente a BigQuery**
- 📊 **Ejecutar consultas SQL desde tu editor**
- 📁 **Explorar datasets y tablas**
- 🧪 **Analizar datos con pandas**
- 📈 **Crear visualizaciones**

---

## 📞 **¿PROBLEMAS?**

**Si algo no funciona:**
1. Verifica que el archivo JSON esté en `credentials/service-account-key.json`
2. Verifica que `.env` tenga la ruta correcta
3. Ejecuta: `python3 test_warehouse_connection.py`

**¡Estaremos listos en menos de 5 minutos una vez que tengas permisos!** 