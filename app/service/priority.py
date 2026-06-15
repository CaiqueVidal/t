from typing import Any

from app.config.logger import get_logger

logger = get_logger(__name__)

LEVEL_SCORE = {
    "low": 1,
    "medium": 2,
    "high": 3,
}


def calculate_insight_priority_score(insight: dict[str, Any]) -> float:
    impact = LEVEL_SCORE.get(insight.get("estimated_impact"), 2)
    confidence = LEVEL_SCORE.get(insight.get("confidence"), 2)
    urgency = LEVEL_SCORE.get(insight.get("urgency"), 2)

    effort = infer_effort_from_actions(
        insight.get("suggested_actions", [])
    )

    evidence_strength = calculate_evidence_strength(
        insight.get("evidence", [])
    )

    limitations_penalty = calculate_limitations_penalty(
        insight.get("limitations", [])
    )

    score = (
        impact * 0.35
        + confidence * 0.25
        + urgency * 0.20
        + evidence_strength * 0.15
        - effort * 0.10
        - limitations_penalty
    )

    return round(score, 2)


def infer_effort_from_actions(actions: list[dict[str, Any]]) -> int:
    if not actions:
        return 2

    efforts = []

    for action in actions:
        effort = action.get("effort")
        efforts.append(LEVEL_SCORE.get(effort, 2))

    return round(sum(efforts) / len(efforts))


def calculate_evidence_strength(evidence: list[dict[str, Any]]) -> int:
    if not evidence:
        return 1

    if len(evidence) >= 3:
        return 3

    if len(evidence) == 2:
        return 2

    return 1


def calculate_limitations_penalty(limitations: list[str]) -> float:
    if not limitations:
        return 0.0

    if len(limitations) >= 3:
        return 0.3

    if len(limitations) == 2:
        return 0.2

    return 0.1


def prioritize_insights(insights: list[dict[str, Any]]) -> list[dict[str, Any]]:
    logger.info("Calculando pontuação de prioridade do insight")
    for insight in insights:
        insight["priority_score"] = calculate_insight_priority_score(insight)

    logger.info("Ordenando insight por prioridade")
    sorted_insights = sorted(
        insights,
        key=lambda item: item.get("priority_score", 0),
        reverse=True,
    )

    for index, insight in enumerate(sorted_insights, start=1):
        insight["priority_hint"] = index

    return sorted_insights