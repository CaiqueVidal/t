from fastapi import FastAPI, HTTPException

from app.config.config import settings
from app.config.logger import setup_logging, get_logger
from app.schema.analyze import AnalyzeRequest, AnalyzeResponse
from app.service.insight_analysis import analyze_dataset


setup_logging()
logger = get_logger(__name__)

app = FastAPI(title=settings.app_name)


@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "app": settings.app_name,
        "env": settings.app_env,
    }


@app.post("/insights/analyze", response_model=AnalyzeResponse)
async def analyze_insights(request: AnalyzeRequest):
    try:
        return await analyze_dataset(request)

    except HTTPException:
        raise

    except Exception as exc:
        logger.exception("Erro inesperado no endpoint de análise")
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Erro inesperado ao processar análise",
                "error": str(exc),
            },
        )