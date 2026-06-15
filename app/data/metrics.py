from typing import Any

from app.config.logger import get_logger
import pandas as pd

logger = get_logger(__name__)


def calculate_basic_metrics(records: list[dict[str, Any]], profile: dict[str, Any]) -> dict[str, Any]:
    logger.info("Calculando métricas básicas")
    df = pd.DataFrame(records)

    numeric_columns = profile["column_types"]["numeric_columns"]
    categorical_columns = profile["column_types"]["categorical_columns"]
    boolean_columns = profile["column_types"]["boolean_columns"]

    numeric_summary = {}

    for column in numeric_columns:
        numeric_summary[column] = {
            "min": safe_number(df[column].min()),
            "max": safe_number(df[column].max()),
            "mean": safe_number(df[column].mean()),
            "median": safe_number(df[column].median()),
            "sum": safe_number(df[column].sum()),
        }

    categorical_summary = {}

    for column in categorical_columns[:10]:
        value_counts = df[column].value_counts(dropna=False).head(5)
        categorical_summary[column] = [
            {
                "value": str(index),
                "count": int(value)
            }
            for index, value in value_counts.items()
        ]

    boolean_summary = {}

    for column in boolean_columns:
        value_counts = df[column].value_counts(dropna=False)
        boolean_summary[column] = {
            str(index): int(value)
            for index, value in value_counts.items()
        }

    logger.info("Métricas básicas calculadas")

    return {
        "numeric_summary": numeric_summary,
        "categorical_summary": categorical_summary,
        "boolean_summary": boolean_summary,
    }


def safe_number(value: Any) -> float | int | None:
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    return value