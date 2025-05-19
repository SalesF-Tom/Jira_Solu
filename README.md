# Jira Solu ETL

Proyecto de extracción, transformación y carga (ETL) de datos desde la API de Jira a BigQuery para análisis de productividad y seguimiento de tickets, sprints y proyectos.

---

## 📉 Objetivo

Automatizar la recolección de datos de Jira para centralizar la información en BigQuery y consumirla desde Google Sheets, dashboards o herramientas de BI.

---

## 🚀 Tecnologías utilizadas

* Python 3.10+
* BigQuery (Google Cloud)
* Pandas
* Google Cloud SDK
* Jira REST API
* dotenv
* schedule (para ejecuciones programadas)
* Discord Webhooks (para notificaciones)

---

## 📦 Estructura del proyecto

```plaintext
Jira_Solu/
├── etl/
│   ├── extractor.py        # Obtención de datos desde la API
│   ├── transformer.py      # Limpieza y normalización de datos
│   └── loader.py           # Inserción de datos en BigQuery
├── bigquery/
│   ├── bigquery_func.py
│   └── querys.py
├── funciones/              # Módulos reutilizables por entidad
├── schema/
│   └── schemas.py         # Definición de los esquemas BQ
├── utils/
│   ├── logger.py           # Configuración de logging
│   └── discord_notify.py  # Notificación por Discord
├── credenciales/
│   └── [clave-gcp].json   # Credenciales de servicio (gitignored)
├── .env                    # Variables de entorno sensibles
├── requirements.txt
├── .gitignore
└── main.py              # Orquestador principal del pipeline
```

---

## 🔧 Instalación y configuración

### 1. Crear entorno virtual:

```bash
python -m venv myenv
myenv\Scripts\activate
```

### 2. Instalar dependencias:

```bash
pip install -r requirements.txt
```

### 3. Variables de entorno:

Crear un archivo `.env` con:

```env
mail = 
API_TOKEN = 
AUTHORIZATION = "Basic... "

PROJECT_ID = 
DATASET_FINAL = 
DATASET_TEMP = 

DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

```

### 4. Configurar credenciales GCP:

Ubicar el archivo de clave JSON en `./credenciales/` y referenciarlo en `main.py`:

```python
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./credenciales/clave.json"
```

---

## ⏱️ Ejecución

### Manual:

```bash
python main.py
```

### Automática:

El script usa `schedule` para mantener la ejecución corriendo en loop cada cierto tiempo. Se puede customizar por tipo de ejecución:

* Diaria
* Semanal
* Mensual
* Histórica

---

## 🚨 Notificaciones

Si configurás `DISCORD_WEBHOOK_URL`, recibirás un resumen como este en tu canal:

```
**Resumen ETL Jira**
✅ Projects: 45 filas procesadas
✅ Sprints: 49 filas procesadas
✅ Tickets: 184 filas procesadas
⏱️ Duración total: 37.79 segundos
```

---

## 🔒 Seguridad

* `.env` y `credenciales/` están ignorados por Git
* Nunca se suben datos sensibles

---

## 🌟 Mejores prácticas

* Logging persistente en `etl.log`
* Validación de datos antes de cargar
* Actualización condicional en BigQuery (`IS DISTINCT FROM`)
* Paginación de tickets y ejecución paralela

---

## 📄 License

MIT (o lo que decida el equipo)

---
