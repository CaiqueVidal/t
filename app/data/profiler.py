from typing import Any

from app.config.logger import get_logger
import pandas as pd

logger = get_logger(__name__)

DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%m/%Y",
    "%d/%m/%Y %H:%M:%S",
]


def profile_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    logger.info("Iniciando profile do dataset")
    df = pd.DataFrame(records)

    column_types = infer_column_types(df)

    missing_values = {
        column: round(float(df[column].isna().mean()), 4)
        for column in df.columns
    }

    unique_values = {
        column: int(df[column].nunique(dropna=True))
        for column in df.columns
    }

    unique_ratio = {
        column: round(
            float(df[column].nunique(dropna=True) / max(len(df), 1)),
            4,
        )
        for column in df.columns
    }

    constant_columns = [
        column
        for column in df.columns
        if df[column].nunique(dropna=True) <= 1
    ]

    high_null_columns = [
        column
        for column, ratio in missing_values.items()
        if ratio >= 0.4
    ]

    high_cardinality_columns = [
        column
        for column, ratio in unique_ratio.items()
        if ratio >= 0.87
    ]

    logger.info(
        "Profiling concluído | total_columns=%s",
        len(df.columns),
    )

    return {
        "total_records": len(df),
        "total_columns": len(df.columns),
        "columns": list(df.columns),
        "column_types": column_types,
        "missing_values_ratio": missing_values,
        "unique_values": unique_values,
        "unique_ratio": unique_ratio,
        "constant_columns": constant_columns,
        "high_null_columns": high_null_columns,
        "high_cardinality_columns": high_cardinality_columns,
    }


def infer_column_types(df: pd.DataFrame) -> dict[str, list[str]]:
    numeric_columns = []
    categorical_columns = []
    boolean_columns = []
    date_columns = []
    text_columns = []

    row_count = len(df)

    for column in df.columns:
        series = df[column].dropna()

        if series.empty:
            categorical_columns.append(column)
            continue

        if pd.api.types.is_bool_dtype(series):
            boolean_columns.append(column)
            continue

        if pd.api.types.is_numeric_dtype(series):
            numeric_columns.append(column)
            continue

        date_ratio = infer_date_ratio(series)

        if date_ratio >= 0.8:
            date_columns.append(column)
            continue

        avg_length = series.astype(str).str.len().mean()
        unique_ratio = series.nunique(dropna=True) / max(row_count, 1)

        if avg_length > 50 or unique_ratio > 0.8:
            text_columns.append(column)
        else:
            categorical_columns.append(column)

    return {
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
        "boolean_columns": boolean_columns,
        "date_columns": date_columns,
        "text_columns": text_columns,
    }

def infer_date_ratio(series: pd.Series) -> float:
    if series.empty:
        return 0.0

    string_series = series.astype(str).str.strip()

    best_ratio = 0.0

    for date_format in DATE_FORMATS:
        parsed_dates = pd.to_datetime(
            string_series,
            format=date_format,
            errors="coerce",
        )

        ratio = parsed_dates.notna().mean()
        best_ratio = max(best_ratio, ratio)

        if best_ratio >= 0.8:
            return best_ratio

    return best_ratio