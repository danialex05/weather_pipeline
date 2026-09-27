"""
Pruebas unitarias básicas — Transformer y validaciones de calidad.

Cobertura:
  - Construcción correcta del DataFrame desde payload de API
  - Cálculo correcto de indicadores derivados
  - Detección de valores faltantes y duplicados
  - Validación de rango de fechas
  - Mapeo de nombres de columnas
"""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

# ── Fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_CONFIG = {
    "pipeline": {
        "start_date": "2026-01-01",
        "end_date":   "2026-01-05",
        "timezone":   "America/Bogota",
    },
    "thresholds": {
        "rain_day_mm":     1.0,
        "rain_heavy_mm":   20.0,
        "wind_strong_kmh": 50.0,
        "temp_cold_c":     10.0,
        "temp_hot_c":      35.0,
    },
    "storage": {
        "raw_path":       "data/raw",
        "parquet_path":   "data/processed/parquet",
        "dashboard_path": "data/dashboard",
        "partition_cols": ["city_slug", "year", "month"],
    },
}

SAMPLE_DAILY = {
    "time":                       ["2026-01-01", "2026-01-02", "2026-01-03",
                                   "2026-01-04", "2026-01-05"],
    "temperature_2m_max":         [28.5,  30.1,  25.0,  36.0,  22.0],
    "temperature_2m_min":         [18.0,  19.5,  15.0,  24.0,  12.0],
    "temperature_2m_mean":        [23.2,  24.8,  20.0,  30.0,  17.0],
    "precipitation_sum":          [0.0,   25.0,  0.5,   5.0,   0.0 ],
    "rain_sum":                   [0.0,   25.0,  0.5,   5.0,   0.0 ],
    "precipitation_hours":        [0,     8,     1,     3,     0   ],
    "windspeed_10m_max":          [10.0,  15.0,  8.0,   55.0,  12.0],
    "windgusts_10m_max":          [20.0,  30.0,  15.0,  60.0,  25.0],
    "et0_fao_evapotranspiration": [3.5,   2.8,   3.0,   4.2,   2.5 ],
}


@pytest.fixture
def sample_raw_file(tmp_path):
    """Crea un archivo raw JSON de prueba en un directorio temporal."""
    city_dir = tmp_path / "bogota"
    city_dir.mkdir()
    raw_file = city_dir / "bogota_20260101T000000Z.json"

    envelope = {
        "metadata": {
            "city_name":    "Bogotá",
            "city_slug":    "bogota",
            "department":   "Cundinamarca",
            "latitude":     4.711,
            "longitude":    -74.0721,
            "extracted_at": "20260101T000000Z",
            "source_url":   "https://archive-api.open-meteo.com/v1/archive",
        },
        "payload": {"daily": SAMPLE_DAILY},
    }

    with open(raw_file, "w") as f:
        json.dump(envelope, f)

    return raw_file


@pytest.fixture
def sample_df(sample_raw_file):
    from src.transformer.weather_transformer import raw_to_dataframe
    return raw_to_dataframe(sample_raw_file)


# ── Tests: raw_to_dataframe ───────────────────────────────────────────────────

class TestRawToDataframe:
    def test_returns_dataframe(self, sample_df):
        assert isinstance(sample_df, pd.DataFrame)

    def test_row_count(self, sample_df):
        assert len(sample_df) == 5

    def test_date_column_is_datetime(self, sample_df):
        assert pd.api.types.is_datetime64_any_dtype(sample_df["date"])

    def test_columns_renamed(self, sample_df):
        # Verifica que los nombres de API se convirtieron al estándar
        assert "temp_max_c"       in sample_df.columns
        assert "precipitation_mm" in sample_df.columns
        assert "windgusts_max_kmh" in sample_df.columns
        assert "temperature_2m_max" not in sample_df.columns

    def test_metadata_columns_added(self, sample_df):
        for col in ["city_name", "city_slug", "department", "latitude",
                    "longitude", "year", "month"]:
            assert col in sample_df.columns

    def test_year_month_correct(self, sample_df):
        assert (sample_df["year"] == 2026).all()
        assert (sample_df["month"] == 1).all()

    def test_numeric_types(self, sample_df):
        assert pd.api.types.is_float_dtype(sample_df["temp_max_c"])
        assert pd.api.types.is_float_dtype(sample_df["precipitation_mm"])


# ── Tests: add_indicators ─────────────────────────────────────────────────────

class TestIndicators:
    @pytest.fixture
    def df_with_indicators(self, sample_df):
        from src.transformer.weather_transformer import add_indicators
        return add_indicators(sample_df, SAMPLE_CONFIG["thresholds"])

    def test_rain_day_flag(self, df_with_indicators):
        df = df_with_indicators
        # Día 2: 25mm → rain_day=1; Día 1: 0mm → rain_day=0
        assert df.loc[df["precipitation_mm"] > 1.0, "rain_day"].all() == 1
        assert df.loc[df["precipitation_mm"] == 0.0, "rain_day"].sum() == 0

    def test_heavy_rain_flag(self, df_with_indicators):
        df = df_with_indicators
        # Solo día con 25mm debe ser heavy_rain
        heavy = df[df["heavy_rain"] == 1]
        assert len(heavy) == 1
        assert heavy["precipitation_mm"].iloc[0] == 25.0

    def test_strong_wind_flag(self, df_with_indicators):
        df = df_with_indicators
        # windgusts=60 km/h → strong_wind; resto < 50
        strong = df[df["strong_wind"] == 1]
        assert len(strong) == 1
        assert strong["windgusts_max_kmh"].iloc[0] == 60.0

    def test_hot_day_flag(self, df_with_indicators):
        df = df_with_indicators
        # temp_max=36°C → hot_day
        hot = df[df["hot_day"] == 1]
        assert len(hot) == 1
        assert hot["temp_max_c"].iloc[0] == 36.0

    def test_adverse_day_is_union(self, df_with_indicators):
        df = df_with_indicators
        # Día adverso = heavy_rain OR strong_wind OR cold_day OR hot_day
        expected = ((df["heavy_rain"] == 1) | (df["strong_wind"] == 1) |
                    (df["cold_day"] == 1)   | (df["hot_day"] == 1))
        assert (df["adverse_day"] == expected.astype(int)).all()

    def test_temp_range(self, df_with_indicators):
        df = df_with_indicators
        expected = (df["temp_max_c"] - df["temp_min_c"]).round(2)
        pd.testing.assert_series_equal(df["temp_range_c"], expected,
                                       check_names=False)


# ── Tests: check_quality ──────────────────────────────────────────────────────

class TestQuality:
    def test_no_nulls_clean_data(self, sample_df):
        from src.transformer.weather_transformer import check_quality
        report = check_quality(sample_df, SAMPLE_CONFIG)
        assert report["duplicates"] == 0
        assert report["total_records"] == 5

    def test_detects_duplicates(self, sample_df):
        from src.transformer.weather_transformer import check_quality
        df_dup = pd.concat([sample_df, sample_df.iloc[[0]]], ignore_index=True)
        report = check_quality(df_dup, SAMPLE_CONFIG)
        assert report["duplicates"] == 1

    def test_date_range_ok(self, sample_df):
        from src.transformer.weather_transformer import check_quality
        report = check_quality(sample_df, SAMPLE_CONFIG)
        assert report["date_range_ok"] is True

    def test_detects_nulls(self, sample_df):
        from src.transformer.weather_transformer import check_quality
        df_null = sample_df.copy()
        df_null.loc[0, "temp_max_c"] = None
        report = check_quality(df_null, SAMPLE_CONFIG)
        assert "temp_max_c" in report["null_fields"]


# ── Tests: remove_duplicates ──────────────────────────────────────────────────

class TestDeduplication:
    def test_removes_duplicates(self, sample_df):
        from src.transformer.weather_transformer import remove_duplicates
        df_dup = pd.concat([sample_df, sample_df.iloc[[0, 1]]], ignore_index=True)
        df_clean = remove_duplicates(df_dup)
        assert len(df_clean) == len(sample_df)

    def test_clean_data_unchanged(self, sample_df):
        from src.transformer.weather_transformer import remove_duplicates
        df_clean = remove_duplicates(sample_df)
        assert len(df_clean) == len(sample_df)
