# 🚀 ETL Jira → BigQuery → Looker

Este proyecto implementa un pipeline **ETL** para extraer información de **Jira**, transformarla y cargarla en **BigQuery**, con el objetivo de habilitar reporting en **Looker / Power BI**.  

---

## 📂 Estructura del proyecto
Jira_Solu/
│── bigquery/           # Funciones de conexión y queries (MERGE)
│── etl/                # Extractor, Transformer, Loader (ETL)
│── funciones/          # Conexiones específicas a APIs (Jira)
│── schema/             # Definición de esquemas BigQuery
│── utils/              # Logger y notificaciones
│── credenciales/       # (Ignorado en git) JSON de servicio GCP
│── main.py             # Script principal de orquestación
│── .env.example        # Variables de entorno (plantilla)
│── requirements.txt    # Dependencias Python
│── README.md           # Este archivo

---

## ⚙️ Configuración del entorno

1. **Clonar el repo**  
git clone <repo-url>
cd Jira_Solu

2. **Crear entorno virtual**
python -m venv venv
source venv/bin/activate   # Linux/Mac
venv\Scripts\activate      # Windows

3. **Instalar dependencias**
pip install -r requirements.txt

4.	**Configurar variables de entorno**
Copiar el archivo de ejemplo y completarlo con tus credenciales:

cp .env.example .env

**🔑 Variables de entorno** (.env)

PROJECT_ID=data-warehouse-311917
DATASET_FINAL=Jira
DATASET_TEMP=zt_Jira_temp
BQ_LOCATION=US

JIRA_EMAIL=tu_email@dominio.com
JIRA_API_TOKEN=xxxxxxxxxxxxxxxxxx

DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/xxx/yyy

GOOGLE_APPLICATION_CREDENTIALS=./credenciales/data-warehouse-311917.json

**▶️ Ejecución**

Correr ETL manualmente

python main.py

Ejecución histórica

python main.py --historico

**🛠️ Componentes ETL**
	•	Extractor: descarga proyectos, sprints y tickets desde Jira.
	•	Transformer: limpia y normaliza los datos (ej. estimates a horas).
	•	Loader: carga los datos en BigQuery (dataset temporal + MERGE).

