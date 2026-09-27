"""
Módulo de transformación de datos meteorológicos.

Responsabilidades:
1. Leer archivos raw JSON
2. Convertir a estructura tabular (DataFrame)
3. Estandarizar tipos y nombres de columnas
4. Enriquecer con metadatos geográficos y temporales
5. Detectar valores faltantes y duplicados
6. Validar rango de fechas
7. Calcular indicadores meteorológicos derivados

UMBRALES DOCUMENTADOS (fuente: config/cities.yaml + estándares OMM):
  - Día lluvioso:        precipitación_sum > 1.0 mm
  - Lluvia intensa:      precipitación_sum > 20.0 mm  (umbral OMM)
  - Viento fuerte:       windgusts_10m_max > 50.0 km/h
  - Temperatura fría:    temperature_2m_max < 10.0 °C
  - Temperatura cálida:  temperature_2m_max > 35.0 °C
  - Día adverso:         lluvia intensa OR viento fuerte OR temperatura extrema
"""

import json
from pathlib import Path

import pandas as pd

from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__, log_file="logs/transformer.log")


# ── Mapeo de nombres de columnas API → nombres estandarizados ────────────────
COLUMN_MAP = {
    "time":                        "date",
    "temperature_2m_max":          "temp_max_c",
    "temperature_2m_min":          "temp_min_c",
    "temperature_2m_mean":         "temp_mean_c",
    "precipitation_sum":           "precipitation_mm",
    "rain_sum":                    "rain_mm",
    "precipitation_hours":         "precipitation_hours",
    "windspeed_10m_max":           "windspeed_max_kmh",
    "windgusts_10m_max":           "windgusts_max_kmh",
    "et0_fao_evapotranspiration":  "evapotranspiration_mm",
}


def raw_to_dataframe(raw_file: Path) -> pd.DataFrame:
    """
    Convierte un archivo raw JSON en un DataFrame tabular limpio.
    """
    with open(raw_file, "r", encoding="utf-8") as f:
        envelope = json.load(f)

    meta    = envelope["metadata"]
    payload = envelope["payload"]
    daily   = payload["daily"]

    # Construir DataFrame base desde variables diarias
    df = pd.DataFrame(daily)

    # Renombrar columnas al estándar del proyecto
    df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})

    # ── Tipos de datos ────────────────────────────────────────────────────────
    df["date"] = pd.to_datetime(df["date"])

    # Columnas numéricas: forzar tipo float (pueden venir como None/null)
    numeric_cols = [c for c in df.columns if c != "date"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── Metadatos geográficos y temporales ───────────────────────────────────
    df["city_name"]   = meta["city_name"]
    df["city_slug"]   = meta["city_slug"]
    df["department"]  = meta["department"]
    df["latitude"]    = meta["latitude"]
    df["longitude"]   = meta["longitude"]
    df["extracted_at"] = meta["extracted_at"]
    df["year"]        = df["date"].dt.year
    df["month"]       = df["date"].dt.month
    df["month_name"]  = df["date"].dt.strftime("%B")
    df["day_of_week"] = df["date"].dt.day_name()

    return df


def check_quality(df: pd.DataFrame, cfg: dict) -> dict:
    """
    Genera reporte de calidad de datos:
    - Valores faltantes por columna
    - Duplicados
    - Validación de rango de fechas
    """
    city  = df["city_name"].iloc[0]
    start = pd.to_datetime(cfg["pipeline"]["start_date"])
    end   = pd.to_datetime(cfg["pipeline"]["end_date"])

    # Valores faltantes
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0].to_dict()

    # Duplicados
    n_dups = df.duplicated(subset=["date", "city_slug"]).sum()

    # Rango de fechas
    actual_start = df["date"].min()
    actual_end   = df["date"].max()
    expected_days = (end - start).days + 1
    actual_days   = len(df)
    missing_days  = expected_days - actual_days

    report = {
        "city":           city,
        "total_records":  actual_days,
        "expected_days":  expected_days,
        "missing_days":   missing_days,
        "duplicates":     int(n_dups),
        "null_fields":    nulls,
        "date_min":       str(actual_start.date()),
        "date_max":       str(actual_end.date()),
        "date_range_ok":  (actual_start.date() == start.date() and
                           actual_end.date() <= end.date()),
    }

    # Log del reporte
    status = "✅" if (missing_days == 0 and n_dups == 0) else "⚠️"
    logger.info(f"  {status} Calidad {city}: "
                f"{actual_days}/{expected_days} días | "
                f"Faltantes: {missing_days} | Duplicados: {n_dups} | "
                f"Nulls: {nulls if nulls else 'ninguno'}")

    return report


def add_indicators(df: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    """
    Calcula indicadores meteorológicos derivados.

    Umbrales aplicados (documentados en config/cities.yaml):
      - rain_day:      precipitation_mm > 1.0 mm
      - heavy_rain:    precipitation_mm > 20.0 mm
      - strong_wind:   windgusts_max_kmh > 50.0 km/h
      - cold_day:      temp_max_c < 10.0 °C
      - hot_day:       temp_max_c > 35.0 °C
      - adverse_day:   heavy_rain OR strong_wind OR cold_day OR hot_day
      - temp_range_c:  temp_max_c - temp_min_c (amplitud térmica diaria)
    """
    t = thresholds

    df["rain_day"]     = (df["precipitation_mm"] > t["rain_day_mm"]).astype(int)
    df["heavy_rain"]   = (df["precipitation_mm"] > t["rain_heavy_mm"]).astype(int)
    df["strong_wind"]  = (df["windgusts_max_kmh"] > t["wind_strong_kmh"]).astype(int)
    df["cold_day"]     = (df["temp_max_c"] < t["temp_cold_c"]).astype(int)
    df["hot_day"]      = (df["temp_max_c"] > t["temp_hot_c"]).astype(int)

    # Día adverso: cumple al menos una condición extrema
    df["adverse_day"]  = (
        (df["heavy_rain"] == 1) |
        (df["strong_wind"] == 1) |
        (df["cold_day"] == 1) |
        (df["hot_day"] == 1)
    ).astype(int)

    # Amplitud térmica diaria
    df["temp_range_c"] = (df["temp_max_c"] - df["temp_min_c"]).round(2)

    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina duplicados por (date, city_slug). Conserva el primer registro.
    Registra cuántos se eliminaron.
    """
    before = len(df)
    df = df.drop_duplicates(subset=["date", "city_slug"], keep="first")
    removed = before - len(df)
    if removed > 0:
        logger.warning(f"  Se eliminaron {removed} registros duplicados")
    return df


def transform_city(raw_file: Path, cfg: dict) -> tuple[pd.DataFrame, dict]:
    """
    Transforma un archivo raw en un DataFrame limpio con indicadores.
    Retorna (DataFrame, reporte_calidad).
    """
    city_slug = raw_file.parent.name
    logger.info(f"Transformando: {city_slug} — {raw_file.name}")

    df      = raw_to_dataframe(raw_file)
    df      = remove_duplicates(df)
    report  = check_quality(df, cfg)
    df      = add_indicators(df, cfg["thresholds"])

    logger.info(f"  ✅ {city_slug}: {len(df)} registros procesados, "
                f"{len(df.columns)} columnas")
    return df, report


def transform_all(raw_files: list[Path] = None, config_path: str = None
                  ) -> tuple[pd.DataFrame, list[dict]]:
    """
    Punto de entrada: transforma todos los archivos raw disponibles.
    Si raw_files es None, lee todos los JSON de data/raw/.

    Retorna (DataFrame consolidado de todas las ciudades, lista de reportes de calidad).
    """
    cfg = load_config(config_path)

    if raw_files is None:
        raw_path  = Path(cfg["storage"]["raw_path"])
        raw_files = sorted(raw_path.rglob("*.json"))
        logger.info(f"Encontrados {len(raw_files)} archivos raw en {raw_path}")

    if not raw_files:
        raise FileNotFoundError(
            f"No hay archivos raw. Ejecuta primero el extractor.")

    # Para cada ciudad tomar solo el archivo más reciente (último por nombre)
    latest: dict[str, Path] = {}
    for f in raw_files:
        slug = f.parent.name
        if slug not in latest or f.name > latest[slug].name:
            latest[slug] = f

    dfs, reports = [], []
    for slug, raw_file in sorted(latest.items()):
        try:
            df, report = transform_city(raw_file, cfg)
            dfs.append(df)
            reports.append(report)
        except Exception as e:
            logger.error(f"❌ Error transformando {slug}: {e}")

    if not dfs:
        raise RuntimeError("Ninguna ciudad pudo ser transformada.")

    consolidated = pd.concat(dfs, ignore_index=True)
    logger.info(f"Transformación completada: {len(consolidated)} registros totales "
                f"de {len(dfs)} ciudades")

    return consolidated, reports


if __name__ == "__main__":
    df, reports = transform_all()
    print(df.info())
    print(df.head())
