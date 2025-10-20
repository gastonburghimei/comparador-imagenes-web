# 🚨 PASOS FINALES - Completar Integración BigQuery + Cursor

## ⚡ Solo faltan 3 pasos (5 minutos)

### **PASO 1: 🔑 Configurar Credenciales**

#### **Opción A: Service Account (Recomendado)**
1. **Ir a Google Cloud Console:**
   ```
   https://console.cloud.google.com/iam-admin/serviceaccounts
   ```

2. **Crear Service Account:**
   - Nombre: `bigquery-cursor`
   - Roles: `BigQuery Data Editor` + `BigQuery User`

3. **Descargar clave JSON:**
   - Crear clave → JSON
   - Guardar como: `./credentials/service-account-key.json`

#### **Opción B: Credenciales por Defecto (Más rápido)**
```bash
# Solo si tienes gcloud CLI instalado
gcloud auth application-default login
```

### **PASO 2: ✏️ Editar archivo .env**

```bash
# Abrir en Cursor/VS Code
cursor .env
```

**Cambiar estas líneas:**
```bash
# Antes:
GOOGLE_CLOUD_PROJECT=tu-proyecto-bigquery

# Después (con tu proyecto real):
GOOGLE_CLOUD_PROJECT=mi-proyecto-real-123
```

**Si usas Service Account, también cambiar:**
```bash
# Antes:
GOOGLE_APPLICATION_CREDENTIALS=ruta/a/tu/service-account-key.json

# Después:
GOOGLE_APPLICATION_CREDENTIALS=./credentials/service-account-key.json
```

### **PASO 3: 🧪 Probar Conexión**

```bash
python3 bigquery_config.py
```

**✅ Si sale esto = ÉXITO:**
```
✅ Conexión a BigQuery exitosa
📊 Proyecto: mi-proyecto-real-123
⏰ Tiempo de respuesta: 2024-07-25 15:42:00+00:00
```

**❌ Si sale error = revisar credenciales**

### **PASO 4: 🎉 Probar con Datos Reales**

```bash
python3 bigquery_examples.py
```

---

## 🔧 **Uso en Cursor:**

### **1. Explorar tu BigQuery:**
```python
from bigquery_config import create_default_connection

connection = create_default_connection()
datasets = connection.list_datasets()
print("Mis datasets:", datasets)
```

### **2. Ejecutar consultas:**
```python
from bigquery_config import quick_query

df = quick_query("""
    SELECT 
        COUNT(*) as total_records,
        CURRENT_DATE() as fecha
    FROM `tu-proyecto.tu-dataset.tu-tabla`
    LIMIT 10
""")
print(df)
```

### **3. Trabajar con archivos SQL:**
```python
# Cursor reconocerá automáticamente tus archivos .sql
# Con syntax highlighting y autocompletado

with open('mi_consulta.sql', 'r') as f:
    query = f.read()
    
df = quick_query(query)
```

---

## 🆘 **Solución de Problemas:**

### **Error: "No module named 'google'"**
```bash
pip3 install -r requirements.txt
```

### **Error: "Permission denied"**
- Verificar roles del service account
- Verificar que BigQuery esté habilitado en el proyecto

### **Error: "Project not found"**
- Verificar el ID del proyecto en .env
- Verificar permisos en Google Cloud

---

## ✅ **Cuando funcione tendrás:**

- 🔗 **Cursor conectado a BigQuery**
- 📊 **Autocompletado SQL inteligente**
- 🧪 **Ejecución de consultas desde Cursor**
- 📁 **Gestión de archivos SQL**
- 🎯 **Análisis de datos integrado**
- 📈 **Visualización con pandas/matplotlib**

**Total de tiempo:** ⏱️ **5-10 minutos máximo** 