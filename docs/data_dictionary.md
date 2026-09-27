# Diccionario de campos — Dataset meteorológico Colombia

## Dataset diario (Parquet particionado)
Ruta: `data/processed/parquet/city_slug=*/year=*/month=*/`

| Campo | Tipo | Descripción | Unidad | Fuente |
|-------|------|-------------|--------|--------|
| date | date | Fecha del registro | YYYY-MM-DD | Open-Meteo |
| city_name | string | Nombre de la ciudad | — | Config |
| city_slug | string | Identificador URL-friendly | — | Config |
| department | string | Departamento colombiano | — | Config |
| latitude | float | Latitud geográfica | grados decimales | Config |
| longitude | float | Longitud geográfica | grados decimales | Config |
| year | int | Año del registro | — | Derivado |
| month | int | Mes del registro (1-12) | — | Derivado |
| month_name | string | Nombre del mes en inglés | — | Derivado |
| day_of_week | string | Día de la semana | — | Derivado |
| temp_max_c | float | Temperatura máxima diaria | °C | Open-Meteo |
| temp_min_c | float | Temperatura mínima diaria | °C | Open-Meteo |
| temp_mean_c | float | Temperatura media diaria | °C | Open-Meteo |
| temp_range_c | float | Amplitud térmica diaria (max-min) | °C | Derivado |
| precipitation_mm | float | Precipitación total diaria | mm | Open-Meteo |
| rain_mm | float | Lluvia total diaria | mm | Open-Meteo |
| precipitation_hours | float | Horas con precipitación | horas | Open-Meteo |
| windspeed_max_kmh | float | Velocidad máxima del viento | km/h | Open-Meteo |
| windgusts_max_kmh | float | Velocidad máxima de ráfagas | km/h | Open-Meteo |
| evapotranspiration_mm | float | Evapotranspiración FAO-56 | mm | Open-Meteo |
| rain_day | int (0/1) | Día lluvioso (precipitation > 1mm) | binario | Derivado |
| heavy_rain | int (0/1) | Lluvia intensa (precipitation > 20mm) | binario | Derivado |
| strong_wind | int (0/1) | Viento fuerte (ráfagas > 50 km/h) | binario | Derivado |
| cold_day | int (0/1) | Temperatura fría (temp_max < 10°C) | binario | Derivado |
| hot_day | int (0/1) | Temperatura cálida (temp_max > 35°C) | binario | Derivado |
| adverse_day | int (0/1) | Día adverso (any extreme condition) | binario | Derivado |
| extracted_at | string | Timestamp de extracción (UTC) | ISO 8601 | Pipeline |

---

## Dataset dashboard (mensual)
Ruta: `data/dashboard/weather_dashboard.parquet` y `.csv`

| Campo | Tipo | Descripción | Unidad |
|-------|------|-------------|--------|
| city_name | string | Nombre de la ciudad | — |
| city_slug | string | Identificador de ciudad | — |
| department | string | Departamento | — |
| latitude / longitude | float | Coordenadas | grados decimales |
| year | int | Año | — |
| month | int | Mes | — |
| month_name | string | Nombre del mes | — |
| temp_mean_avg | float | Temperatura media mensual | °C |
| temp_max_avg | float | Promedio de temperaturas máximas | °C |
| temp_min_avg | float | Promedio de temperaturas mínimas | °C |
| temp_max_abs | float | Temperatura máxima absoluta del mes | °C |
| temp_min_abs | float | Temperatura mínima absoluta del mes | °C |
| precipitation_total_mm | float | Precipitación acumulada mensual | mm |
| rain_days | int | Días lluviosos en el mes | días |
| heavy_rain_days | int | Días con lluvia intensa | días |
| strong_wind_days | int | Días con viento fuerte | días |
| adverse_days | int | Días con condiciones adversas | días |
| avg_windspeed_max_kmh | float | Promedio de velocidades máximas | km/h |
| avg_windgusts_max_kmh | float | Promedio de ráfagas máximas | km/h |

---

## Particionamiento Parquet

```
data/processed/parquet/
├── city_slug=bogota/
│   ├── year=2026/
│   │   ├── month=1/part-0.parquet
│   │   ├── month=2/part-0.parquet
│   │   └── ...
├── city_slug=medellin/
│   └── ...
```

**Ventaja:** herramientas como DuckDB, pandas y Power BI aplican
*partition pruning* automáticamente al filtrar por ciudad o mes,
leyendo solo los archivos relevantes sin escanear todo el dataset.
