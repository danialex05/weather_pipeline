# Pipeline de Datos Meteorológicos — Colombia

**Prueba Técnica Senior Data Engineering**

Pipeline ETL reproducible que extrae, transforma y publica datos meteorológicos
históricos para 5 ciudades colombianas, con orquestación en Airflow y almacenamiento
en formato Apache Parquet.

---

## Arquitectura

```
Open-Meteo API
      │
      ▼
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌───────────────┐
│  EXTRACTOR  │────▶│  VALIDATOR   │────▶│ TRANSFORMER  │────▶│    LOADER     │
│  (Bronze)   │     │  (Bronze)    │     │  (Silver)    │     │   (Gold)      │
│ JSON raw    │     │ Integridad   │     │ Limpieza +   │     │ Parquet part. │
│ por ciudad  │     │ + esquema    │     │ Indicadores  │     │ + Dashboard   │
└─────────────┘     └──────────────┘     └──────────────┘     └───────────────┘
      │                                                               │
      ▼                                                               ▼
data/raw/<ciudad>/           data/processed/parquet/     data/dashboard/
<ciudad>_<ts>.json           city_slug=*/year=*/month=*  weather_dashboard.parquet
                                                          weather_dashboard.csv
                                                          quality_report.json
```

**Orquestación (Airflow DAG):**

```
extract_weather → validate_raw_data → transform_weather → load_parquet → generate_dashboard_dataset
```

---

## Inicio rápido

### Prerequisitos

- Docker ≥ 24.0
- Docker Compose ≥ 2.20

### 1. Clonar y configurar

```bash
git clone https://github.com/danialex05/weather_pipeline.git
cd weather_pipeline
```

### 2. Levantar el entorno (primera vez)

```bash
# Inicializar Airflow (una sola vez)
docker-compose up airflow-init

# Levantar todos los servicios
docker-compose up -d
```

### 3. Acceder a Airflow

- URL: http://localhost:8080
- Usuario: `admin` / Contraseña: `admin`

### 4. Ejecutar el DAG

En la UI de Airflow:

1. Buscar el DAG `weather_pipeline_colombia`
2. Activarlo (toggle ON)
3. Clic en **Trigger DAG** (ícono ▶)

O desde línea de comandos:

```bash
docker-compose exec scheduler airflow dags trigger weather_pipeline_colombia
```

### 5. Verificar resultados

```bash
# Listar archivos Parquet generados
ls data/processed/parquet/

# Leer con pandas
python3 -c "
import pandas as pd
df = pd.read_parquet('data/processed/parquet/', engine='pyarrow')
print(df.shape, df['city_name'].unique())
"

# Leer con DuckDB
python3 -c "
import duckdb
r = duckdb.query(\"SELECT city_name, COUNT(*) as dias FROM read_parquet('data/processed/parquet/**/*.parquet') GROUP BY city_name\")
print(r.df())
"
```

---

## Estructura del proyecto

```
weather_pipeline/
├── config/
│   └── cities.yaml              # Ciudades, periodo, umbrales, rutas
├── dags/
│   └── weather_pipeline_dag.py  # DAG de Airflow (TaskFlow API)
├── src/
│   ├── extractor/
│   │   └── weather_extractor.py # Extracción Open-Meteo + validación raw
│   ├── transformer/
│   │   └── weather_transformer.py # Limpieza, tipado, indicadores derivados
│   ├── loader/
│   │   └── parquet_loader.py    # Carga Parquet + dataset dashboard
│   └── utils/
│       ├── config.py            # Cargador de configuración YAML
│       └── logger.py            # Logger unificado
├── tests/
│   └── test_transformer.py      # Pruebas unitarias (pytest)
├── data/
│   ├── raw/                     # Capa Bronze: JSON originales por ciudad
│   ├── processed/parquet/       # Capa Silver/Gold: Parquet particionado
│   └── dashboard/               # Dataset resumido para BI
├── docs/
│   └── data_dictionary.md       # Diccionario de campos
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── .gitignore
└── README.md
```

## Decisiones técnicas

| Decisión                       | Alternativa considerada | Justificación                                                           |
| ------------------------------- | ----------------------- | ------------------------------------------------------------------------ |
| TaskFlow API (Airflow)          | PythonOperator clásico | Código más limpio, XCom implícito, menos boilerplate                  |
| LocalExecutor                   | CeleryExecutor          | Suficiente para 5 ciudades; menor complejidad operativa                  |
| Parquet + Snappy                | CSV / ORC               | Balance compresión/velocidad; nativo en pandas, DuckDB, Spark, Power BI |
| Partición city_slug/year/month | Solo por ciudad         | Permite partition pruning eficiente en consultas por periodo             |
| SQLite para metadatos Airflow   | MySQL                   | Más simple para demo; en producción cambiar a PostgreSQL               |
| PyYAML para configuración      | .env / argparse         | Un solo archivo con todas las variables; versionable en Git              |

---

## Umbrales meteorológicos documentados

| Indicador           | Umbral                                           | Fuente                             |
| ------------------- | ------------------------------------------------ | ---------------------------------- |
| Día lluvioso       | precipitation_mm > 1.0 mm                        | Estándar OMM                      |
| Lluvia intensa      | precipitation_mm > 20.0 mm                       | Estándar OMM                      |
| Viento fuerte       | windgusts_max_kmh > 50.0 km/h                    | Escala Beaufort (fuerza 7)         |
| Temperatura fría   | temp_max_c < 10.0 °C                            | Referencia climatológica tropical |
| Temperatura cálida | temp_max_c > 35.0 °C                            | Referencia climatológica tropical |
| Día adverso        | heavy_rain OR strong_wind OR cold_day OR hot_day | Combinación de anteriores         |

Todos los umbrales son configurables en `config/cities.yaml`.

---

---

## Limitaciones conocidas y mejoras productivas

| Limitación actual                  | Mejora productiva                            |
| ----------------------------------- | -------------------------------------------- |
| Datos hasta 2026-09-15 (histórico) | Agregar DAG incremental diario con`@daily` |
| Sin autenticación en Airflow       | Configurar LDAP/OAuth                        |
| Sin alertas de fallo                | Integrar con PagerDuty o email SMTP          |
| Sin versionado de datos             | Agregar Delta Lake o Iceberg                 |
| Tests solo en transformer           | Ampliar cobertura a extractor y loader       |

---

## Tiempo registrado por sección

| Sección                           | Tiempo estimado      |
| ---------------------------------- | -------------------- |
| Contenerización (Docker)          | 45 min               |
| Extracción + validación          | 60 min               |
| Transformación + indicadores      | 60 min               |
| Almacenamiento Parquet             | 45 min               |
| Orquestación Airflow              | 60 min               |
| Dashboard dataset + documentación | 30 min               |
| **Total**                    | **~5.5 horas** |
