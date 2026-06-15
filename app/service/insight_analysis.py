from typing import Any

from fastapi import HTTPException
from pydantic import ValidationError

from app.client.openai_client import OpenAIClient
from app.client.stackspot_client import StackSpotClient
from app.config.logger import get_logger
from app.data.data_loader import load_records_from_json_file
from app.data.data_quality import evaluate_data_quality
from app.data.metrics import calculate_basic_metrics
from app.data.profiler import profile_records
from app.schema.analyze import AnalyzeRequest, AnalyzeResponse


logger = get_logger(__name__)

stackspot_client = StackSpotClient()
openai_client = OpenAIClient()


DEFAULT_ANALYSIS_GOAL = (
    "Gerar uma análise exploratória do dataset, identificando principais achados, "
    "riscos, oportunidades, hipóteses explicativas, recomendações acionáveis, "
    "priorização dos insights e limitações dos dados."
)


async def analyze_dataset(request: AnalyzeRequest) -> AnalyzeResponse:
    logger.info("Iniciando análise de insights")

    analysis_goal = f"""{DEFAULT_ANALYSIS_GOAL}
    {request.analysis_goal}
    """

    records = resolve_records_or_raise(request)

    profile = profile_records(records)

    data_quality = evaluate_data_quality(profile)
    print(data_quality)
    validate_data_quality_or_raise(data_quality)

    metrics = calculate_basic_metrics(
        records=records,
        profile=profile,
    )

    agent_response = await invoke_agent_or_raise({
        "analysis_goal": analysis_goal,
        "records": records,
        "profile": profile,
        "data_quality": data_quality,
        "metrics": metrics,
    })

    return build_response_or_raise(agent_response)


def resolve_records_or_raise(request: AnalyzeRequest) -> list[dict[str, Any]]:
    try:
        records = resolve_input_records(request)

        logger.info(
            "Dataset resolvido com sucesso | source=%s | records=%s",
            "path_dataset" if request.path_dataset else "records",
            len(records),
        )

        return records

    except Exception as exc:
        logger.exception("Erro ao resolver dataset de entrada")
        raise HTTPException(
            status_code=400,
            detail=f"Erro ao resolver dataset de entrada: {str(exc)}",
        )


def resolve_input_records(request: AnalyzeRequest) -> list[dict[str, Any]]:
    if request.path_dataset:
        return load_records_from_json_file(request.path_dataset)

    if request.records:
        return request.records

    raise ValueError("Informe path_dataset ou records")


def validate_data_quality_or_raise(data_quality: dict[str, Any]) -> None:
    if data_quality["score"] >= 0.6:
        return

    logger.warning(
        "Dataset reprovado por baixa qualidade | score=%s",
        data_quality.get("score"),
    )

    raise HTTPException(
        status_code=422,
        detail={
            "message": data_quality.get(
                "quality_summary",
                "Dataset com baixa qualidade estrutural.",
            ),
            "score": data_quality.get("score"),
            "issues": data_quality.get("issues", []),
            "limitations": data_quality.get("limitations", []),
        },
    )


async def invoke_agent_or_raise(
    agent_payload: dict[str, Any],
) -> dict[str, Any]:
    try:
        logger.info("Chamando fluxo multiagente")

        agent_response = await openai_client.invoke_agent(agent_payload)

        # Quando for trocar para StackSpot:
        # agent_response = await stackspot_client.invoke_agent(agent_payload)

        logger.info("Fluxo multiagente finalizado com sucesso")

        return agent_response

    except Exception as exc:
        logger.exception("Erro ao chamar fluxo multiagente")
        raise HTTPException(
            status_code=502,
            detail=f"Erro ao chamar OpenAI Agent temporário: {str(exc)}",
        )


def build_response_or_raise(agent_response: dict[str, Any]) -> AnalyzeResponse:
    try:
        final_response = AnalyzeResponse(
            **agent_response,
        )

        logger.info(
            "Resposta validada com sucesso | inferred_domain=%s | insights=%s",
            final_response.inferred_domain,
            len(final_response.insights),
        )

        return final_response

    except ValidationError as exc:
        logger.exception("Resposta do agente fora do schema esperado")
        raise HTTPException(
            status_code=422,
            detail={
                "message": "A resposta do agente não está no schema esperado",
                "errors": exc.errors(),
                "agent_response": agent_response,
            },
        )

    except Exception as exc:
        logger.exception("Erro ao processar resposta do agente")
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Erro ao processar resposta do agente",
                "error": str(exc),
                "agent_response": agent_response,
            },
        )
