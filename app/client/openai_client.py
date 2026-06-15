import json
import re
from typing import Any

from app.service.response_consolidator import consolidate_response
from openai import AsyncOpenAI

from app.config.config import settings

SEMANTIC_PROFILER_PROMPT = """
    Você é o Semantic Profiler Agent.

    Sua responsabilidade é inferir o domínio provável de um dataset estruturado e interpretar semanticamente suas colunas.

    Você receberá:
    - objetivo da análise, quando informado;
    - perfil estrutural dos dados;
    - métricas genéricas;
    - amostra de registros;
    - lista de colunas;
    - tipos inferidos pela aplicação;
    - cardinalidade, nulos e valores únicos.

    Você deve inferir:
    - domínio provável do dataset;
    - nível de confiança da inferência;
    - justificativa resumida para o domínio inferido;
    - colunas que parecem identificadores técnicos;
    - dimensões de negócio;
    - métricas quantitativas;
    - colunas temporais;
    - candidatos a variável-alvo;
    - colunas que devem ser ignoradas em agrupamentos;
    - observações úteis para os demais agentes.

    Domínios possíveis incluem, mas não se limitam a:
    - incidents
    - churn
    - sales
    - nps
    - support
    - product_usage
    - financial
    - marketing
    - operations
    - unknown

    Regras obrigatórias:
    - Use apenas colunas existentes no data_profile.columns.
    - Não invente domínio se os dados forem insuficientes.
    - Se a inferência for incerta, use domain_confidence = "low".
    - Se não for possível identificar o domínio, use inferred_domain = "unknown".
    - Não gere insights de negócio.
    - Não gere recomendações.
    - Não assuma causalidade.
    - Classifique colunas semanticamente como inferência, não como verdade absoluta.
    - Use termos como "parece", "provavelmente" ou "candidato a" quando houver incerteza.

    Não use markdown.
    Não use blocos ```json.
    Não escreva texto antes ou depois do JSON.
    Retorne exclusivamente JSON válido neste formato:

    {
        "semantic_profile": {
            "inferred_domain": "string",
            "domain_confidence": "low | medium | high",
            "domain_reasoning_summary": "string",
            "likely_identifier_columns": ["string"],
            "business_dimensions": ["string"],
            "quantitative_metrics": ["string"],
            "temporal_columns": ["string"],
            "target_candidates": ["string"],
            "columns_to_ignore_for_grouping": ["string"],
            "notes": ["string"]
        }
    }
"""

INSIGHT_AGENT_PROMPT = """
    Você é o Insight Agent.

    Sua responsabilidade é identificar os principais achados analíticos do dataset.

    Você deve procurar:
    - concentrações relevantes;
    - padrões por categoria;
    - valores extremos;
    - diferenças entre grupos;
    - tendências aparentes;
    - sinais de risco ou oportunidade;
    - relações observáveis entre variáveis.

    Regras obrigatórias:
    - Todo insight precisa ter evidência.
    - A evidência deve vir do payload recebido.
    - Não invente cálculos.
    - Não afirme causa raiz.
    - Não transforme hipótese em fato.
    - Evite insights genéricos como "monitorar os dados" sem evidência específica.
    - Priorize achados acionáveis.
    - Use no máximo 5 insights.

    Não use markdown.
    Não use blocos ```json.
    Não escreva texto antes ou depois do JSON.
    Retorne exclusivamente JSON válido neste formato:

    {
        "insights": [
            {
            "title": "string",
            "description": "string",
            "evidence": [
                {
                "description": "string",
                "source": "string | null"
                }
            ],
            "confidence": "low | medium | high",
            "estimated_impact": "low | medium | high",
            "urgency": "low | medium | high",
            "limitations": ["string"]
            }
        ]
    }
"""

RISK_OPPORTUNITY_PROMPT = """
    Você é o Risk & Opportunity Agent.

    Sua responsabilidade é identificar riscos e oportunidades derivados dos insights e métricas fornecidas.

    Riscos podem envolver (não se limite a elas):
    - perda de receita;
    - churn;
    - piora de experiência do cliente;
    - indisponibilidade;
    - aumento de incidentes;
    - concentração excessiva;
    - degradação operacional;
    - baixa eficiência;
    - riscos de decisão por dados incompletos.

    Oportunidades podem envolver (não se limite a elas):
    - aumento de receita;
    - redução de churn;
    - otimização operacional;
    - melhoria de SLA;
    - melhoria de experiência;
    - automação;
    - priorização de segmentos;
    - redução de recorrência.

    Regras:
    - Associe riscos e oportunidades às evidências disponíveis.
    - Não invente impacto financeiro se ele não estiver nos dados.
    - Não afirme causa definitiva.
    - Evite recomendações ainda; foque em risco e oportunidade.
    - Seja específico ao domínio informado.

    Não use markdown.
    Não use blocos ```json.
    Não escreva texto antes ou depois do JSON.
    Retorne exclusivamente JSON válido neste formato:

    {
        "risks": ["string"],
        "opportunities": ["string"]
    }
"""

HYPOTHESIS_PROMPT = """
    Você é o Hypothesis Agent.

    Sua responsabilidade é gerar hipóteses explicativas para os padrões observados.

    Regras obrigatórias:
    - Hipóteses não são conclusões.
    - Não afirme causalidade definitiva.
    - Toda hipótese deve estar conectada a uma evidência ou padrão observado.
    - Quando faltar dado para validar a hipótese, diga qual dado seria necessário.
    - Evite hipóteses muito genéricas.
    - Diferencie claramente o que foi observado do que é inferência.

    Exemplo de formulação correta:
    "O aumento de incidentes no serviço X pode estar relacionado à causa Y, pois os registros mostram concentração de ocorrências associadas a Y. Para validar, seria necessário analisar logs, deploys e métricas de infraestrutura no mesmo período."

    Exemplo de formulação incorreta:
    "O serviço X está ruim porque o banco de dados está com problema."

    Produza hipóteses úteis para apoiar investigação e tomada de decisão.

    Não use markdown.
    Não use blocos ```json.
    Não escreva texto antes ou depois do JSON.
    Retorne exclusivamente JSON válido neste formato:

    {
        "hypotheses": ["string"]
    }
"""

RECOMMENDATION_PROMPT = """
    Você é o Recommendation Agent.

    Sua responsabilidade é transformar insights, riscos, oportunidades e hipóteses em recomendações acionáveis.

    Cada recomendação deve ser:
    - específica;
    - executável;
    - conectada a uma evidência;
    - compatível com as limitações dos dados.

    Regras:
    - Não recomende ações que não tenham relação com os dados.
    - Não prometa resultado garantido.
    - Quando o dado for limitado, recomende validação antes de ação definitiva.
    - Sugira ações de curto prazo quando possível.
    - Sugira responsáveis prováveis quando fizer sentido, como Produto, Engenharia, Dados, CRM, Operações, Atendimento ou SRE.
    - Classifique esforço quando possível como low, medium ou high.

    Exemplos de boas recomendações:
    - "Priorizar investigação do checkout-api, pois ele concentra a maior quantidade de incidentes críticos."
    - "Criar monitoramento específico para root_cause=database_latency se essa causa aparecer entre as mais frequentes."
    - "Segmentar clientes do plano básico com alto uso para campanha de retenção se o churn estiver concentrado nesse grupo."

    Evite recomendações genéricas como:
    - "Melhorar o sistema."
    - "Analisar melhor os dados."
    - "Fazer acompanhamento contínuo."

    Não use markdown.
    Não use blocos ```json.
    Não escreva texto antes ou depois do JSON.
    Retorne exclusivamente JSON válido neste formato:

    {
        "recommendations": [
            {
                "insight_title": "string",
                "suggested_actions": [
                    {
                        "action": "string",
                        "owner_suggestion": "string | null",
                        "effort": "low | medium | high | null",
                    }
                ]
            }
        ]
    }
"""

CRITICAL_REVIEWER_PROMPT = """
    Você é o Critical Reviewer Agent.

    Sua responsabilidade é revisar criticamente a análise gerada pelos demais agentes.

    Verifique:
    - se todo insight possui evidência;
    - se alguma conclusão afirma causalidade indevidamente;
    - se há recomendações genéricas demais;
    - se a confiança está coerente com a força da evidência;
    - se o impacto estimado está coerente;
    - se limitações importantes foram omitidas;
    - se há campos inventados;
    - se a resposta respeita o contrato JSON esperado.

    Regras:
    - Seja rigoroso.
    - Remova ou rebaixe insights sem evidência suficiente.
    - Rebaixe confiança quando houver limitação relevante.
    - Marque como hipótese qualquer explicação causal.
    - Não adicione métricas inexistentes.
    - Não aceite recomendações sem vínculo com evidência.

    Não use markdown.
    Não use blocos ```json.
    Não escreva texto antes ou depois do JSON.
    Retorne exclusivamente JSON válido neste formato:

    {
        "review": {
            "approved": true,
            "issues": ["string"],
            "changes_required": ["string"]
        }
    }
"""

class OpenAIClient:
    def __init__(self):
        if not settings.openai_api_key:
            raise ValueError("OPENAI_API_KEY não configurada no .env")

        self.client = AsyncOpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model

    async def invoke_agent(self, payload: dict[str, Any]) -> dict[str, Any]:
        semantic_profile = await self._run_agent(
            agent_name="Semantic Profiler Agent",
            system_prompt=SEMANTIC_PROFILER_PROMPT,
            payload=payload,
        )

        insights = await self._run_agent(
            agent_name="Insight Agent",
            system_prompt=INSIGHT_AGENT_PROMPT,
            payload={
                **payload,
                "semantic_profile_result": semantic_profile
            },
        )

        risks_opportunities = await self._run_agent(
            agent_name="Risk & Opportunity Agent",
            system_prompt=RISK_OPPORTUNITY_PROMPT,
            payload={
                **payload,
                "semantic_profile_result": semantic_profile,
                "insight_result": insights,
            },
        )

        hypotheses = await self._run_agent(
            agent_name="Hypothesis Agent",
            system_prompt=HYPOTHESIS_PROMPT,
            payload={
                **payload,
                "semantic_profile_result": semantic_profile,
                "insight_result": insights,
                "risk_opportunity_result": risks_opportunities,
            },
        )

        recommendations = await self._run_agent(
            agent_name="Recommendation Agent",
            system_prompt=RECOMMENDATION_PROMPT,
            payload={
                **payload,
                "semantic_profile_result": semantic_profile,
                "insight_result": insights,
                "risk_opportunity_result": risks_opportunities,
                "hypothesis_result": hypotheses,
            },
        )

        # review = await self._run_agent(
        #     agent_name="Critical Reviewer Agent",
        #     system_prompt=CRITICAL_REVIEWER_PROMPT,
        #     payload={
        #         **payload,
        #         "semantic_profile_result": semantic_profile,
        #         "insight_result": insights,
        #         "risk_opportunity_result": risks_opportunities,
        #         "hypothesis_result": hypotheses,
        #         "recommendation_result": recommendations,
        #     },
        # )

        return consolidate_response(
            analysis_goal=payload.get("analysis_goal", ""),
            data_quality_result=payload.get("data_quality", ""),
            semantic_profile_result=semantic_profile,
            insight_result=insights,
            risk_opportunity_result=risks_opportunities,
            hypothesis_result=hypotheses,
            recommendation_result=recommendations,
        )

    async def _run_agent(
        self,
        agent_name: str,
        system_prompt: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        response = await self.client.responses.create(
            model=self.model,
            temperature=get_temperature_by_agent(agent_name),
            top_p=1.0,
            input=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": json.dumps(payload, ensure_ascii=False),
                },
            ],
        )

        print(agent_name)

        raw_text = response.output_text

        try:
            return parse_json_from_model_output(raw_text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"{agent_name} retornou uma resposta que não é JSON válido: {raw_text}"
            ) from exc


def get_temperature_by_agent(agent_name: str) -> float:
    if agent_name in {
        "Semantic Profiler Agent",
        "Critical Reviewer Agent",
    }:
        return 0.0

    if agent_name == "Insight Agent":
        return 0.2

    if agent_name in {
        "Risk & Opportunity Agent",
        "Hypothesis Agent",
        "Recommendation Agent",
    }:
        return 0.3

    return 0.2

def parse_json_from_model_output(raw_text: str) -> dict[str, Any]:
    cleaned_text = raw_text.strip()

    # Remove blocos markdown do tipo ```json ... ```
    if cleaned_text.startswith("```"):
        cleaned_text = re.sub(
            r"^```(?:json)?\s*",
            "",
            cleaned_text,
            flags=re.IGNORECASE,
        )
        cleaned_text = re.sub(
            r"\s*```$",
            "",
            cleaned_text,
        )

    cleaned_text = cleaned_text.strip()

    return json.loads(cleaned_text)