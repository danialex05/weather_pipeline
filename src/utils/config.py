from pathlib import Path
import yaml


def load_config(config_path: str = None) -> dict:
    """
    Carga el archivo de configuración YAML.
    Busca en la ruta indicada o sube directorios hasta encontrar 
    config/cities.yaml.
    """
    if config_path:
        path = Path(config_path)
    else:
        current = Path(__file__).resolve()
        for parent in current.parents:
            candidate = parent / "config" / "cities.yaml"
            if candidate.exists():
                path = candidate
                break
        else:
            raise FileNotFoundError(
                "No se encontró config/cities.yaml. "
                "Ejecuta desde la raíz del proyecto o pasa "
                "config_path explícitamente."
            )

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
