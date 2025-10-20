# 🚀 Configuración de BigQuery para Cursor

Esta guía te ayudará a conectar Cursor (o VS Code) con Google BigQuery para trabajar con tus datos de manera eficiente.

## 📋 Prerequisitos

- **Python 3.8+** instalado
- **Cursor** o VS Code instalado
- **Cuenta de Google Cloud** con BigQuery habilitado
- **Proyecto de Google Cloud** creado

## 🛠️ Configuración Automática

### Paso 1: Ejecutar el Script de Configuración

```bash
python setup_bigquery.py
```

Este script automáticamente:
- ✅ Verifica prerequisitos del sistema
- ✅ Instala dependencias de BigQuery
- ✅ Crea estructura de directorios
- ✅ Configura archivos de entorno
- ✅ Crea configuración de VS Code/Cursor

### Paso 2: Configurar Credenciales

#### Opción A: Service Account (Recomendado para desarrollo)

1. **Crear Service Account en Google Cloud:**
   - Ir a [Google Cloud Console](https://console.cloud.google.com/)
   - Navegar a "IAM & Admin" > "Service Accounts"
   - Hacer clic en "Create Service Account"
   - Asignar roles: `BigQuery Data Editor`, `BigQuery User`

2. **Descargar credenciales:**
   - Crear una nueva clave JSON
   - Descargar el archivo `service-account-key.json`
   - Guardar en `./credentials/service-account-key.json`

3. **Configurar variables de entorno:**
   ```bash
   # Editar archivo .env
   GOOGLE_CLOUD_PROJECT=tu-proyecto-bigquery
   GOOGLE_APPLICATION_CREDENTIALS=./credentials/service-account-key.json
   BIGQUERY_LOCATION=US
   ```

#### Opción B: Credenciales por Defecto (ADC)

1. **Instalar Google Cloud CLI:**
   ```bash
   # macOS
   brew install google-cloud-sdk
   
   # Linux/Windows
   # Seguir: https://cloud.google.com/sdk/docs/install
   ```

2. **Autenticarse:**
   ```bash
   gcloud auth login
   gcloud config set project tu-proyecto-bigquery
   gcloud auth application-default login
   ```

## 🧪 Probar la Conexión

### Prueba Rápida
```bash
python bigquery_config.py
```

### Ejecutar Ejemplos Completos
```bash
python bigquery_examples.py
```

## 📊 Uso Básico en tu Código

### Conexión Simple
```python
from bigquery_config import create_default_connection, quick_query

# Conexión rápida
df = quick_query("SELECT 1 as test")
print(df)
```

### Conexión Avanzada
```python
from bigquery_config import BigQueryConfig, BigQueryConnection

# Configuración personalizada
config = BigQueryConfig(
    project_id="mi-proyecto",
    location="EU"
)

connection = BigQueryConnection(config)

# Consultar datos
df = connection.execute_query("""
    SELECT 
        name, 
        COUNT(*) as count
    FROM `bigquery-public-data.usa_names.usa_1910_current`
    WHERE year = 2023
    GROUP BY name
    ORDER BY count DESC
    LIMIT 10
""")

print(df)
```

## 🔧 Configuración de Cursor/VS Code

### Extensiones Recomendadas

El script de configuración instala automáticamente estas recomendaciones:

- **Python** - Soporte completo para Python
- **Pylint** - Linting de código Python
- **Jupyter** - Soporte para notebooks
- **Cloud Code** - Herramientas de Google Cloud
- **SQLTools** - Editor SQL avanzado

### Configuración de SQL

Cursor/VS Code estará configurado para:
- 📄 Reconocer archivos `.sql` automáticamente
- 🎨 Syntax highlighting para SQL
- 📝 Formateo automático de consultas
- 🔍 IntelliSense para SQL

## 📁 Estructura del Proyecto

```
tu-proyecto/
├── bigquery_config.py         # Configuración principal
├── bigquery_examples.py       # Ejemplos prácticos
├── setup_bigquery.py         # Script de configuración
├── requirements.txt          # Dependencias Python
├── env.template             # Template de variables
├── .env                    # Variables de entorno (crear)
├── credentials/           # Directorio de credenciales
│   ├── .gitignore        # Protección de credenciales
│   └── service-account-key.json  # Tus credenciales
└── .vscode/             # Configuración de Cursor/VS Code
    ├── settings.json    # Configuración del editor
    └── extensions.json  # Extensiones recomendadas
```

## 🎯 Casos de Uso Comunes

### 1. Explorar Datasets
```python
connection = create_default_connection()

# Listar datasets
datasets = connection.list_datasets()
print("Datasets disponibles:", datasets)

# Listar tablas en un dataset
tables = connection.list_tables("mi_dataset")
print("Tablas:", tables)
```

### 2. Ejecutar Consultas SQL desde Archivos
```python
# Leer y ejecutar archivo SQL
with open('mi_consulta.sql', 'r') as f:
    query = f.read()

df = quick_query(query)
print(df.head())
```

### 3. Subir DataFrames a BigQuery
```python
import pandas as pd

# Crear datos de ejemplo
df = pd.DataFrame({
    'fecha': pd.date_range('2024-01-01', periods=100),
    'ventas': range(100)
})

# Subir a BigQuery
connection.upload_dataframe(
    df, 
    dataset_id="mi_dataset", 
    table_id="ventas_2024"
)
```

### 4. Análisis con Datos Públicos
```python
# Analizar tendencias de nombres
query = """
SELECT 
    year,
    name,
    SUM(number) as total
FROM `bigquery-public-data.usa_names.usa_1910_current`
WHERE name IN ('Emma', 'Liam', 'Olivia', 'Noah')
    AND year >= 2000
GROUP BY year, name
ORDER BY year, total DESC
"""

df = quick_query(query)

# Visualizar con matplotlib
import matplotlib.pyplot as plt
df.pivot(index='year', columns='name', values='total').plot()
plt.show()
```

## 🔒 Seguridad y Mejores Prácticas

### Protección de Credenciales
- ✅ **Nunca** commits credenciales al repositorio
- ✅ Usar archivos `.gitignore` apropiados
- ✅ Rotar credenciales regularmente
- ✅ Usar roles mínimos necesarios

### Gestión de Costos
- 💰 Configurar alertas de billing en Google Cloud
- 📊 Usar `LIMIT` en consultas exploratorias
- 🎯 Filtrar datos tempranamente
- 📈 Monitorear uso con Cloud Monitoring

### Optimización de Consultas
- 📊 Usar particiones cuando sea posible
- 🔍 Evitar `SELECT *` en tablas grandes
- 📦 Cachear resultados frecuentes
- ⚡ Usar clustering para mejores performances

## 🐛 Solución de Problemas

### Error: "No module named 'google'"
```bash
pip install -r requirements.txt
```

### Error: "Credentials not found"
1. Verificar que `GOOGLE_APPLICATION_CREDENTIALS` apunte al archivo correcto
2. Verificar permisos del archivo JSON
3. Intentar con `gcloud auth application-default login`

### Error: "Permission denied"
1. Verificar roles del service account
2. Verificar que el proyecto tenga BigQuery habilitado
3. Verificar billing account configurado

### Error: "Query timeout"
```python
# Aumentar timeout en la configuración
config = BigQueryConfig()
client = config.get_client()
job_config = bigquery.QueryJobConfig()
job_config.job_timeout_ms = 600000  # 10 minutos
```

## 📚 Recursos Adicionales

- 📖 [Documentación oficial de BigQuery](https://cloud.google.com/bigquery/docs)
- 🎓 [BigQuery SQL Reference](https://cloud.google.com/bigquery/docs/reference/standard-sql)
- 💡 [Datos públicos de BigQuery](https://cloud.google.com/bigquery/public-data)
- 💰 [Calculadora de precios](https://cloud.google.com/products/calculator)
- 🛠️ [Herramientas de línea de comandos](https://cloud.google.com/bigquery/docs/bq-command-line-tool)

## 🤝 Contribuir

Si encuentras problemas o mejoras:
1. Crear un issue describiendo el problema
2. Proponer soluciones o mejoras
3. Compartir casos de uso adicionales

---

**¡Listo para empezar!** 🎉

Ejecuta `python setup_bigquery.py` y sigue los pasos para tener BigQuery funcionando en Cursor en menos de 10 minutos. 