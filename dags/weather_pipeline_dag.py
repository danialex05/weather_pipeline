from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner":            "daniel.bohorquez",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
}


@dag(
    dag_id="weather_pipeline_colombia",
    description="Pipeline ETL meteorológico para 5 ciudades colombianas — Open-Meteo",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def weather_pipeline():

    @task(
        task_id="extract_weather",
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    def extract_weather() -> list[str]:
        """
        Llama al módulo de extracción para las 5 ciudades configuradas.
        Retorna lista de rutas a archivos raw (como strings para XCom).
        """
        import sys
        sys.path.insert(0, "/opt/airflow")

        from src.extractor.weather_extractor import extract_all

        raw_files = extract_all()
        paths = [str(f) for f in raw_files]
        log.info(f"extract_weather: {len(paths)} archivos raw generados")
        return paths

    @task(task_id="validate_raw_data")
    def validate_raw_data(raw_paths: list[str]) -> list[str]:
        """
        Valida integridad de cada archivo raw antes de transformar.
        Falla la tarea si algún archivo es inválido (fuerza reintento/alerta).
        """
        import sys
        sys.path.insert(0, "/opt/airflow")

        from src.extractor.weather_extractor import validate_raw

        valid_paths = []
        invalid     = []

        for path in raw_paths:
            if validate_raw(Path(path)):
                valid_paths.append(path)
            else:
                invalid.append(path)
                log.error(f"validate_raw_data: archivo inválido → {path}")

        if invalid:
            raise ValueError(
                f"Validación fallida para {len(invalid)} archivo(s): {invalid}"
            )

        log.info(f"validate_raw_data: {len(valid_paths)} archivos válidos")
        return valid_paths

    @task(task_id="transform_weather")
    def transform_weather(raw_paths: list[str]) -> str:
        """
        Transforma archivos raw a DataFrame limpio con indicadores.
        Serializa el DataFrame a un archivo temporal Parquet para pasarlo entre tareas.
        (Evitar serializar DataFrames grandes en XCom directamente.)
        """
        import sys
        sys.path.insert(0, "/opt/airflow")

        from src.transformer.weather_transformer import transform_all
        from src.utils.config import load_config

        paths   = [Path(p) for p in raw_paths]
        df, reports = transform_all(raw_files=paths)

        # Serializar resultado temporal
        cfg      = load_config()
        tmp_dir  = Path(cfg["storage"]["dashboard_path"])
        tmp_dir.mkdir(parents=True, exist_ok=True)

        tmp_df      = tmp_dir / "_tmp_transformed.parquet"
        tmp_reports = tmp_dir / "_tmp_reports.json"

        df.to_parquet(tmp_df, index=False)
        with open(tmp_reports, "w") as f:
            json.dump(reports, f, default=str)

        log.info(f"transform_weather: {len(df)} registros | "
                 f"{df['city_slug'].nunique()} ciudades")
        return str(tmp_df)

    @task(task_id="load_parquet")
    def load_parquet(tmp_parquet_path: str) -> str:
        """
        Lee el DataFrame temporal y lo persiste en Parquet particionado.
        """
        import sys
        sys.path.insert(0, "/opt/airflow")

        import pandas as pd
        from src.loader.parquet_loader import save_parquet
        from src.utils.config import load_config

        cfg        = load_config()
        df         = pd.read_parquet(tmp_parquet_path)
        parquet_out = save_parquet(
            df,
            cfg["storage"]["parquet_path"],
            cfg["storage"]["partition_cols"],
        )

        log.info(f"load_parquet: datos guardados en {parquet_out}")
        return str(parquet_out)

    @task(task_id="generate_dashboard_dataset")
    def generate_dashboard_dataset(tmp_parquet_path: str) -> dict:
        """
        Genera el dataset mensual resumido para BI y el reporte de calidad.
        Lee el DataFrame temporal para no re-leer todos los parquets particionados.
        """
        import sys
        sys.path.insert(0, "/opt/airflow")

        import json
        import pandas as pd
        from src.loader.parquet_loader import (
            generate_dashboard_dataset as gen_dashboard,
            save_quality_report,
            verify_parquet,
        )
        from src.utils.config import load_config

        cfg           = load_config()
        df            = pd.read_parquet(tmp_parquet_path)
        reports_file  = Path(cfg["storage"]["dashboard_path"]) / "_tmp_reports.json"

        with open(reports_file) as f:
            reports = json.load(f)

        gen_dashboard(df, cfg["storage"]["dashboard_path"])
        save_quality_report(reports, cfg["storage"]["dashboard_path"])
        sample = verify_parquet(cfg["storage"]["parquet_path"])

        # Limpiar temporales
        Path(tmp_parquet_path).unlink(missing_ok=True)
        reports_file.unlink(missing_ok=True)

        result = {
            "dashboard_records": len(
                pd.read_parquet(
                    Path(cfg["storage"]["dashboard_path"]) / "weather_dashboard.parquet"
                )
            ),
            "parquet_records": len(sample),
            "cities": sample["city_name"].unique().tolist(),
        }
        log.info(f"generate_dashboard_dataset: pipeline completado | {result}")
        return result

    raw_paths     = extract_weather()
    valid_paths   = validate_raw_data(raw_paths)
    tmp_parquet   = transform_weather(valid_paths)
    parquet_path  = load_parquet(tmp_parquet)
    result        = generate_dashboard_dataset(tmp_parquet)

    parquet_path >> result


weather_pipeline()
