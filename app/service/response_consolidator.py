from typing import Any

from app.config.logger import get_logger
from app.service.priority import prioritize_insights

logger = get_logger(__name__)

def consolidate_response(
    analysis_goal: str,
    data_quality_result: dict[str, Any],
    semantic_profile_result: dict[str, Any],
    insight_result: dict[str, Any],
    risk_opportunity_result: dict[str, Any],
    hypothesis_result: dict[str, Any],
    recommendation_result: dict[str, Any],
) -> dict[str, Any]:
    logger.info("Consolidando retorno")

    semantic_profile = semantic_profile_result.get("semantic_profile", {})

    insights = insight_result.get("insights", [])
    recommendations = recommendation_result.get("recommendations", [])

    enriched_insights = attach_recommendations_to_insights(
        insights=insights,
        recommendations=recommendations,
    )

    enriched_insights = prioritize_insights(enriched_insights)

    overall_limitations = build_overall_limitations(
        data_quality_result=data_quality_result,
        semantic_profile=semantic_profile,
    )

    return {
        "inferred_domain": semantic_profile.get("inferred_domain", "unknown"),
        "domain_confidence": semantic_profile.get("domain_confidence", "low"),
        "analysis_goal": analysis_goal,
        "semantic_profile": semantic_profile,
        "summary": build_summary(
            semantic_profile=semantic_profile,
            insights=enriched_insights,
            data_quality_result=data_quality_result,
        ),
        "insights": enriched_insights,
        "risks": risk_opportunity_result.get("risks", []),
        "opportunities": risk_opportunity_result.get("opportunities", []),
        "hypotheses": hypothesis_result.get("hypotheses", []),
        "overall_limitations": overall_limitations,
    }


def attach_recommendations_to_insights(
    insights: list[dict[str, Any]],
    recommendations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    recommendation_by_title = {
        recommendation.get("insight_title"): recommendation.get("suggested_actions", [])
        for recommendation in recommendations
    }

    enriched = []

    for insight in insights:
        title = insight.get("title")

        suggested_actions = recommendation_by_title.get(title)

        if suggested_actions is None:
            suggested_actions = insight.get("suggested_actions", [])

        enriched.append(
            {
                "title": insight.get("title", "Insight sem título"),
                "description": insight.get("description", ""),
                "evidence": insight.get("evidence", []),
                "confidence": insight.get("confidence", "medium"),
                "estimated_impact": insight.get("estimated_impact", "medium"),
                "urgency": insight.get("urgency", "medium"),
                "suggested_actions": suggested_actions,
                "limitations": insight.get("limitations", []),
                "priority": insight.get("priority", insight.get("priority_hint", 999)),
            }
        )

    return enriched


def build_overall_limitations(
    data_quality_result: dict[str, Any],
    semantic_profile: dict[str, Any],
) -> list[str]:
    limitations = []

    data_quality = data_quality_result.get("data_quality", data_quality_result)

    limitations.extend(data_quality.get("limitations", []))

    if semantic_profile.get("domain_confidence") == "low":
        limitations.append(
            "O domínio do dataset foi inferido com baixa confiança."
        )

    if not limitations:
        limitations.append(
            "Nenhuma limitação geral crítica foi identificada, mas os resultados dependem da qualidade e representatividade do dataset."
        )

    return limitations


def build_summary(
    semantic_profile: dict[str, Any],
    insights: list[dict[str, Any]],
    data_quality_result: dict[str, Any],
) -> str:
    inferred_domain = semantic_profile.get("inferred_domain", "unknown")
    domain_confidence = semantic_profile.get("domain_confidence", "low")

    data_quality = data_quality_result.get("data_quality", data_quality_result)
    quality_summary = data_quality.get("quality_summary", "Qualidade dos dados avaliada.")

    return (
        f"A análise identificou o domínio provável como '{inferred_domain}' "
        f"com confiança '{domain_confidence}'. "
        f"Foram consolidados {len(insights)} insights priorizados. "
        f"{quality_summary}"
    )
