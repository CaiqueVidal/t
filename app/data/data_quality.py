from typing import Any


def evaluate_data_quality(profile: dict[str, Any]) -> dict[str, Any]:
    total_records = profile.get("total_records", 0)
    total_columns = profile.get("total_columns", 0)
    high_null_columns = profile.get("high_null_columns", [])
    constant_columns = profile.get("constant_columns", [])
    missing_values_ratio = profile.get("missing_values_ratio", {})
    high_cardinality_columns = profile.get("high_cardinality_columns", [])

    issues: list[str] = []
    limitations: list[str] = []

    if total_records < 30:
        issues.append("Dataset possui menos de 30 registros.")
        limitations.append(
            "Amostra pequena pode reduzir a confiança dos insights gerados."
        )

    if total_columns < 3:
        issues.append("Dataset possui menos de 3 colunas.")
        limitations.append(
            "Poucas colunas limitam a possibilidade de segmentação e comparação."
        )

    for column in high_null_columns:
        ratio = missing_values_ratio.get(column)

        if ratio is not None:
            percentage = round(ratio * 100, 2)
            issues.append(
                f"A coluna '{column}' possui {percentage}% de valores nulos."
            )
            limitations.append(
                f"Análises envolvendo '{column}' podem ter menor confiança devido à alta ausência de dados."
            )

    for column in constant_columns:
        issues.append(
            f"A coluna '{column}' possui valor constante ou baixa variação."
        )
        limitations.append(
            f"A coluna '{column}' tende a ter baixa utilidade para segmentação."
        )

    if high_cardinality_columns:
        limitations.append(
            "Colunas de alta cardinalidade podem representar identificadores ou texto livre e devem ser usadas com cautela em agrupamentos."
        )

    score = calculate_quality_score(
        total_records=total_records,
        total_columns=total_columns,
        high_null_columns_count=len(high_null_columns),
        constant_columns_count=len(constant_columns),
    )

    usable_for_analysis = score >= 0.4 and total_records > 0 and total_columns >= 2

    if not issues:
        issues.append("Nenhum problema crítico de qualidade foi identificado pelas regras determinísticas.")

    if not limitations:
        limitations.append("As limitações devem ser complementadas pela interpretação dos agentes conforme o objetivo da análise.")

    return {
        "score": score,
        "quality_summary": build_quality_summary(score),
        "issues": issues,
        "limitations": limitations,
        "usable_for_analysis": usable_for_analysis,
    }


def calculate_quality_score(
    total_records: int,
    total_columns: int,
    high_null_columns_count: int,
    constant_columns_count: int,
) -> float:
    score = 1.0

    if total_records < 30:
        score -= 0.25
    elif total_records < 100:
        score -= 0.10

    if total_columns < 3:
        score -= 0.20

    score -= min(high_null_columns_count * 0.10, 0.30)
    score -= min(constant_columns_count * 0.05, 0.20)

    return round(max(score, 0.0), 2)


def build_quality_summary(score: float) -> str:
    if score >= 0.8:
        return "Dataset possui boa qualidade estrutural para análise exploratória."

    if score >= 0.7:
        return "Dataset possui qualidade moderada, com algumas limitações relevantes."

    if score >= 0.6:
        return "Dataset possui qualidade limitada e os insights devem ser interpretados com cautela."

    return "Dataset possui baixa qualidade estrutural para análise confiável."
