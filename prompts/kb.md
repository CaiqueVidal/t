# Insight Analysis Standards

## Semantic profiler
Exemplos:
- Se uma coluna possui alta cardinalidade e valores como INC001, INC002, INC003, ela provavelmente é um identificador técnico.
- Se uma coluna numérica representa receita, duração, quantidade, uso ou score, ela provavelmente é uma métrica analítica.
- Se uma coluna categórica representa plano, região, severidade, canal ou serviço, ela provavelmente é uma dimensão de análise.
- Se uma coluna representa status, churned, converted ou resolved, ela pode ser uma variável de resultado, dependendo do domínio e do objetivo.


## Regras de evidência

Um insight só é válido se estiver sustentado por evidências presentes no contexto recebido.

Evidências podem ser:
- contagens;
- percentuais;
- médias;
- somas;
- rankings;
- presença de nulos;
- concentração por categoria;
- diferença entre grupos;
- recorrência de padrões.

Exemplo:
"checkout-api aparece no ranking de serviços com maior quantidade de incidentes."

## Confiança

high:
Evidência direta, volume suficiente e baixa ambiguidade.

medium:
Evidência parcial, alguma limitação ou necessidade de validação complementar.

low:
Amostra pequena, dado incompleto, evidência fraca ou alta incerteza.

## Impacto

high:
Impacta receita, churn, SLA, indisponibilidade, risco operacional, experiência de cliente ou grande volume.

medium:
Impacta parte relevante do domínio, mas com efeito localizado.

low:
Impacto pontual, exploratório ou de baixa urgência.

## Causalidade

Não transforme correlação em causalidade.

Use:
"pode indicar"
"pode estar associado"
"uma hipótese é"
"seria necessário validar"

Evite:
"causou"
"é causado por"
"comprova"
"garante"

## Recomendações acionáveis

Uma recomendação deve indicar:
- o que fazer;
- por que fazer;
- onde atuar;
- possível responsável;
- esforço aproximado quando possível.

## Exemplos de insight ruim

"Os dados mostram que a empresa precisa melhorar."

Problema:
Genérico, sem evidência e sem ação clara.

## Exemplo de insight bom

"O serviço checkout-api concentra incidentes críticos, aparecendo como principal serviço afetado nas métricas agregadas. Isso indica risco operacional relevante para fluxos transacionais."
