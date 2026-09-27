"""
Módulo de extracción de datos meteorológicos desde Open-Meteo Archive API.

Diseño:
- Parametrizable: ciudades y periodo vienen del archivo de configuración.
- Sin duplicación: una sola función construye y ejecuta la query para
  cualquier ciudad.
- Persiste la respuesta original (JSON) en capa raw con metadata de extracción.
- Logs detallados en cada paso.
- Reintentos con backoff para errores de conexión transitorios.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__, log_file="logs/extractor.log")


def build_api_params(city: dict, cfg: dict) -> dict:
    """
    Construye los parámetros de la query para una ciudad.
    Se centraliza aquí para no duplicar lógica por ciudad.
    """
    return {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": cfg["pipeline"]["start_date"],
        "end_date": cfg["pipeline"]["end_date"],
        "daily": ",".join(cfg["api"]["daily_variables"]),
        "timezone": cfg["pipeline"]["timezone"],
    }


def fetch_city(city: dict, cfg: dict) -> dict:
    """
    Consulta la API para una ciudad con reintentos y validación HTTP.
    Retorna el JSON de respuesta o lanza excepción si todos los 
    intentos fallan.
    """
    url = cfg["api"]["base_url"]
    params = build_api_params(city, cfg)
    retries = cfg["api"]["max_retries"]
    delay = cfg["api"]["retry_delay_seconds"]
    timeout = cfg["api"]["timeout_seconds"]

    logger.info(f"Extrayendo: {city['name']} ({city['slug']}) | "
                f"{cfg['pipeline']['start_date']} → "
                f"{cfg['pipeline']['end_date']}")

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            if "daily" not in data:
                raise ValueError(
                    f"Respuesta inesperada de API: 'daily' no encontrado. "
                    f"Respuesta: {str(data)[:200]}")

            logger.info(f"{city['name']}: {len(
                data['daily'].get('time', []))} días recibidos")
            return data

        except requests.exceptions.ConnectionError as e:
            logger.warning(
                f"  Intento {attempt}/{retries} — Error de conexión: {e}")
        except requests.exceptions.Timeout as e:
            logger.warning(f"  Intento {attempt}/{retries} — Timeout: {e}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"  Error HTTP {response.status_code}: {e}")
            raise
        except ValueError as e:
            logger.error(f"  Error de validación: {e}")
            raise

        if attempt < retries:
            logger.info(f"Esperando {delay}s antes de reintentar")
            time.sleep(delay)

    raise RuntimeError(f"Todos los intentos fallaron para {city['name']}")


def save_raw(city: dict, data: dict, raw_path: str) -> Path:
    """
    Persiste la respuesta original en capa raw.
    Estructura: raw/<slug>/<slug>_<fecha_extraccion>.json

    Se guarda el JSON original + metadata de cuándo y cómo se extrajo,
    para garantizar reproducibilidad y trazabilidad.
    """
    extracted_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(raw_path) / city["slug"]
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{city['slug']}_{extracted_at}.json"

    envelope = {
        "metadata": {
            "city_name":      city["name"],
            "city_slug":      city["slug"],
            "department":     city["department"],
            "latitude":       city["latitude"],
            "longitude":      city["longitude"],
            "extracted_at":   extracted_at,
            "source_url":     "https://archive-api.open-meteo.com/v1/archive",
        },
        "payload": data,
    }

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=2)

    logger.info(f"Raw guardado: {out_file}")
    return out_file


def validate_raw(raw_file: Path) -> bool:
    """
    Validación básica del archivo raw antes de continuar el pipeline.
    Verifica integridad del JSON y presencia de campos requeridos.
    """
    try:
        with open(raw_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        required_meta = {"city_name", "city_slug", "extracted_at"}
        missing_meta = required_meta - set(data.get("metadata", {}).keys())
        if missing_meta:
            logger.error(f"Raw inválido: faltan campos en metadata: "
                         f"{missing_meta}")
            return False

        daily = data.get("payload", {}).get("daily", {})
        if not daily or "time" not in daily:
            logger.error("Raw inválido: 'daily.time' no encontrado en payload")
            return False

        n_days = len(daily["time"])
        logger.info(
            f"Raw válido: {data['metadata']['city_name']} — {n_days} días")
        return True

    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"  Raw inválido — error de lectura: {e}")
        return False


def extract_all(config_path: str = None) -> list[Path]:
    """
    Punto de entrada principal: extrae datos para todas las ciudades 
    configuradas.
    Retorna lista de archivos raw generados.

    Diseño: una función, sin duplicación por ciudad — el loop maneja el resto.
    """
    cfg = load_config(config_path)
    raw_path = cfg["storage"]["raw_path"]
    cities = cfg["cities"]
    raw_files = []
    errors = []

    logger.info(f"Iniciando extracción: {len(cities)} ciudades | "
                f"{cfg['pipeline']['start_date']} → "
                f"{cfg['pipeline']['end_date']}")

    for city in cities:
        try:
            data = fetch_city(city, cfg)
            raw_file = save_raw(city, data, raw_path)
            raw_files.append(raw_file)
        except Exception as e:
            logger.error(f"Falló extracción para {city['name']}: {e}")
            errors.append(city["name"])

    logger.info(
        f"Extracción completada: {len(raw_files)}/{len(cities)} exitosas")
    if errors:
        logger.warning(f"Ciudades con error: {errors}")

    return raw_files


if __name__ == "__main__":
    extract_all()
