from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class AnalyzeRequest(BaseModel):
    path_dataset: str | None = Field(
        default=None,
        description=(
            "Caminho opcional para um arquivo JSON contendo o dataset. "
            "Quando informado, o campo records será ignorado."
        ),
    )
    records: list[dict[str, Any]] | None = Field(
        default=None,
        description=(
            "Lista opcional de registros estruturados. "
            "Usado apenas quando path_dataset não for informado."
        ),
    )
    analysis_goal: str | None = Field(
        default=None,
        description=(
            "Objetivo opcional da análise. "
            "Se ausente, será feita análise exploratória."
        ),
    )

    @model_validator(mode="after")
    def validate_data_source(self):
        if not self.path_dataset and not self.records:
            raise ValueError(
                "Informe path_dataset ou records para realizar a análise."
            )

        if self.path_dataset:
            return self

        validate_records_structure(self.records)

        return self

def validate_records_structure(records: list[dict[str, Any]] | None) -> None:
    if not records:
        raise ValueError("records não pode estar vazio quando path_dataset não for informado")

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"record[{index}] precisa ser um objeto JSON")

        if not record:
            raise ValueError(f"record[{index}] não pode estar vazio")

    first_keys = set(records[0].keys())

    if len(first_keys) < 2:
        raise ValueError("dataset precisa ter pelo menos duas colunas")

    for index, record in enumerate(records):
        current_keys = set(record.keys())
        overlap = len(first_keys.intersection(current_keys)) / len(first_keys)

        if overlap < 0.5:
            raise ValueError(
                f"record[{index}] possui estrutura muito diferente dos demais registros"
            )


class Evidence(BaseModel):
    description: str
    source: str | None = None

class SuggestedAction(BaseModel):
    action: str
    owner_suggestion: str | None = None
    effort: Literal["low", "medium", "high"] | None = None

class Insight(BaseModel):
    title: str
    description: str
    evidence: list[Evidence] = Field(min_length=1)
    confidence: Literal["low", "medium", "high"]
    estimated_impact: Literal["low", "medium", "high"]
    urgency:  Literal["low", "medium", "high"]
    priority_hint: int
    suggested_actions: list[SuggestedAction] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)

class SemanticProfile(BaseModel):
    inferred_domain: str
    domain_confidence: Literal["low", "medium", "high"]
    domain_reasoning_summary: str
    likely_identifier_columns: list[str] = Field(default_factory=list)
    business_dimensions: list[str] = Field(default_factory=list)
    quantitative_metrics: list[str] = Field(default_factory=list)
    temporal_columns: list[str] = Field(default_factory=list)
    target_candidates: list[str] = Field(default_factory=list)
    columns_to_ignore_for_grouping: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

class AnalyzeResponse(BaseModel):
    summary: str
    inferred_domain: str
    domain_confidence: Literal["low", "medium", "high"]
    analysis_goal: str
    insights: list[Insight]
    risks: list[str] = Field(default_factory=list)
    opportunities: list[str] = Field(default_factory=list)
    hypotheses: list[str] = Field(default_factory=list)
    overall_limitations: list[str] = Field(default_factory=list)
