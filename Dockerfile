# ── Imagen base ───────────────────────────────────────────────────────────────
# apache/airflow:2.9.1-python3.11
# - Airflow 2.9.1: versión LTS estable con TaskFlow API y soporte completo
# - Python 3.11: mejoras de rendimiento vs 3.10, compatible con todas las libs
FROM apache/airflow:2.9.1-python3.11

# ── Metadatos ─────────────────────────────────────────────────────────────────
LABEL maintainer="daniel.bohorquez"
LABEL description="Pipeline ETL meteorológico — INETUM/BBVA"
LABEL version="1.0.0"

# ── Usuario root para instalación de dependencias del sistema ─────────────────
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── Usuario airflow para instalación de paquetes Python ──────────────────────
USER airflow

# ── Copiar requerimientos e instalar ─────────────────────────────────────────
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# ── Copiar código fuente ──────────────────────────────────────────────────────
COPY --chown=airflow:root src/      /opt/airflow/src/
COPY --chown=airflow:root config/   /opt/airflow/config/
COPY --chown=airflow:root dags/     /opt/airflow/dags/

# ── Directorios de datos (montados como volúmenes en docker-compose) ──────────
RUN mkdir -p /opt/airflow/data/raw \
             /opt/airflow/data/processed/parquet \
             /opt/airflow/data/dashboard \
             /opt/airflow/logs
