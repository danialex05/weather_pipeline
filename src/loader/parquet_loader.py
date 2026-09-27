"""
Módulo de carga: persiste datos en formato Apache Parquet y genera dataset para BI.

Estrategia de particionamiento: city_slug / year / month
  - Permite a herramientas como Power BI, DuckDB y pandas filtrar por ciudad
    o periodo sin leer todos los archivos (partition pruning).
  - Granularidad: diaria en parquet, mensual en dataset de dashboard.

Lectura verificada con pandas (pyarrow engine) — compatible también con
DuckDB, Polars y Spark sin cambios en los archivos.
"""

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__, log_file="logs/loader.log")

# Columnas que van al parquet diario (ordenadas para legibilidad)
DAILY_COLUMNS = [
    "date", "city_name", "city_slug", "department",
    "latitude", "longitude", "year", "month", "month_name", "day_of_week",
    "temp_max_c", "temp_min_c", "temp_mean_c", "temp_range_c",
    "precipitation_mm", "rain_mm", "precipitation_hours",
    "windspeed_max_kmh", "windgusts_max_kmh", "evapotranspiration_mm",
    "rain_day", "heavy_rain", "strong_wind", "cold_day", "hot_day", "adverse_day",
]

# Columnas del dataset resumido para dashboard
DASHBOARD_COLUMNS = [
    "city_name", "city_slug", "department", "latitude", "longitude",
    "year", "month", "month_name",
    "temp_mean_avg", "temp_max_avg", "temp_min_avg",
    "temp_max_abs", "temp_min_abs",
    "precipitation_total_mm", "rain_days", "heavy_rain_days",
    "strong_wind_days", "adverse_days",
    "avg_windspeed_max_kmh", "avg_windgusts_max_kmh",
]


def save_parquet(df: pd.DataFrame, parquet_path: str,
                 partition_cols: list[str] = None) -> Path:
    """
    Guarda el DataFrame en formato Parquet particionado.

    Particionamiento: city_slug / year / month
    Resultado: data/processed/parquet/city_slug=bogota/year=2026/month=1/part-0.parquet

    Se usa pyarrow directamente para control fino sobre el esquema y la compresión.
    """
    if partition_cols is None:
        partition_cols = ["city_slug", "year", "month"]

    out_path = Path(parquet_path)
    out_path.mkdir(parents=True, exist_ok=True)

    # Seleccionar y ordenar columnas disponibles
    cols_available = [c for c in DAILY_COLUMNS if c in df.columns]
    df_out = df[cols_available].copy()

    # Convertir date a string para compatibilidad máxima en Parquet
    df_out["date"] = df_out["date"].dt.strftime("%Y-%m-%d")

    table = pa.Table.from_pandas(df_out, preserve_index=False)

    pq.write_to_dataset(
        table,
        root_path=str(out_path),
        partition_cols=partition_cols,
        compression="snappy",           # Snappy: balance velocidad/tamaño
        existing_data_behavior="delete_matching",  # Idempotente: sobreescribe partición
    )

    # Contar archivos generados
    n_files = len(list(out_path.rglob("*.parquet")))
    logger.info(f"✅ Parquet guardado: {out_path} | "
                f"{len(df_out)} registros | {n_files} archivos de partición")
    return out_path


def generate_dashboard_dataset(df: pd.DataFrame, dashboard_path: str) -> Path:
    """
    Genera dataset mensual resumido para consumo de BI.

    Agrega por ciudad + año + mes:
    - Temperatura: promedio, máxima absoluta, mínima absoluta
    - Precipitación: total acumulada, días lluviosos, días lluvia intensa
    - Viento: promedio de máximas, días viento fuerte
    - Días adversos
    """
    out_path = Path(dashboard_path)
    out_path.mkdir(parents=True, exist_ok=True)

    group_keys = ["city_name", "city_slug", "department",
                  "latitude", "longitude", "year", "month"]

    dashboard = df.groupby(group_keys, as_index=False).agg(
        temp_mean_avg        = ("temp_mean_c",        "mean"),
        temp_max_avg         = ("temp_max_c",         "mean"),
        temp_min_avg         = ("temp_min_c",         "mean"),
        temp_max_abs         = ("temp_max_c",         "max"),
        temp_min_abs         = ("temp_min_c",         "min"),
        precipitation_total_mm = ("precipitation_mm", "sum"),
        rain_days            = ("rain_day",            "sum"),
        heavy_rain_days      = ("heavy_rain",          "sum"),
        strong_wind_days     = ("strong_wind",         "sum"),
        adverse_days         = ("adverse_day",         "sum"),
        avg_windspeed_max_kmh  = ("windspeed_max_kmh", "mean"),
        avg_windgusts_max_kmh  = ("windgusts_max_kmh", "mean"),
    )

    # Redondear métricas continuas
    round_cols = [c for c in dashboard.columns
                  if dashboard[c].dtype in ["float64", "float32"]]
    dashboard[round_cols] = dashboard[round_cols].round(2)

    # Añadir nombre del mes
    dashboard["month_name"] = pd.to_datetime(
        dashboard[["year", "month"]].assign(day=1)
    ).dt.strftime("%B")

    # Ordenar
    dashboard = dashboard.sort_values(["city_name", "year", "month"]).reset_index(drop=True)

    # Guardar como Parquet y CSV (el CSV facilita conectar Power BI directamente)
    parquet_file = out_path / "weather_dashboard.parquet"
    csv_file     = out_path / "weather_dashboard.csv"

    dashboard.to_parquet(parquet_file, index=False, compression="snappy")
    dashboard.to_csv(csv_file, index=False, encoding="utf-8-sig")  # utf-8-sig: compatible Excel/Power BI

    logger.info(f"✅ Dashboard dataset: {len(dashboard)} registros mensuales | "
                f"{parquet_file.name} + {csv_file.name}")
    return out_path


def save_quality_report(reports: list[dict], dashboard_path: str) -> Path:
    """Guarda el reporte de calidad de datos en JSON legible."""
    out_path = Path(dashboard_path)
    out_path.mkdir(parents=True, exist_ok=True)
    report_file = out_path / "quality_report.json"

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(reports, f, ensure_ascii=False, indent=2, default=str)

    logger.info(f"✅ Reporte de calidad guardado: {report_file}")
    return report_file


def verify_parquet(parquet_path: str) -> pd.DataFrame:
    """
    Verifica que los archivos Parquet son legibles con pandas + pyarrow.
    Retorna muestra de 5 registros para comprobación visual.

    Compatible también con:
      import duckdb; duckdb.query("SELECT * FROM read_parquet('data/processed/parquet/**/*.parquet')")
      import polars as pl; pl.read_parquet("data/processed/parquet/**/*.parquet")
    """
    sample = pd.read_parquet(parquet_path, engine="pyarrow")
    logger.info(f"✅ Verificación Parquet: {len(sample)} registros leídos correctamente")
    logger.info(f"   Columnas: {list(sample.columns)}")
    logger.info(f"   Ciudades: {sample['city_name'].unique().tolist()}")
    return sample


def load_all(df: pd.DataFrame, reports: list[dict],
             config_path: str = None) -> dict:
    """
    Punto de entrada: guarda parquet diario, genera dataset dashboard,
    guarda reporte de calidad y verifica la carga.
    """
    cfg = load_config(config_path)

    parquet_path   = cfg["storage"]["parquet_path"]
    dashboard_path = cfg["storage"]["dashboard_path"]
    partition_cols = cfg["storage"]["partition_cols"]

    logger.info("Iniciando carga de datos procesados...")

    parquet_out   = save_parquet(df, parquet_path, partition_cols)
    dashboard_out = generate_dashboard_dataset(df, dashboard_path)
    report_file   = save_quality_report(reports, dashboard_path)
    sample        = verify_parquet(parquet_path)

    return {
        "parquet_path":    str(parquet_out),
        "dashboard_path":  str(dashboard_out),
        "quality_report":  str(report_file),
        "sample_records":  len(sample),
    }


if __name__ == "__main__":
    from src.transformer.weather_transformer import transform_all
    df, reports = transform_all()
    result = load_all(df, reports)
    print(result)
