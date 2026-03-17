"""
Lambda: AgentCore Self-Managed Memory Pipeline
Trigger: SNS → Lambda
Flow: SNS event → S3 (mensagens) → Extraction → Consolidation → Reflection → AgentCore
"""

import boto3
import json
import os
import logging
from datetime import datetime, timezone
import time


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Clients ──────────────────────────────────────────────────────────────────
s3_client       = boto3.client("s3")
bedrock_runtime = boto3.client("bedrock-runtime",         region_name=os.environ.get("REGION", "us-east-1"))
agentcore_ctrl  = boto3.client("bedrock-agentcore-control", region_name=os.environ.get("REGION", "us-east-1"))
agentcore_rt    = boto3.client("bedrock-agentcore",       region_name=os.environ.get("REGION", "us-east-1"))

# ── Config (via env vars) ─────────────────────────────────────────────────────
MEMORY_ID   = ""    # ex: memory_self_managed_batch10-NjNmP1CdDX
STRATEGY_ID =  "" # ex: self_managed_batch10-5cIsF17PIM
MODEL_ID    = os.environ.get("MODEL_ID", "global.anthropic.claude-opus-4-6-v1")

# ── Prompts ───────────────────────────────────────────────────────────────────
EXTRACTION_SYSTEM_PROMPT = """Você é um especialista em análise de conversas. A sua tarefa é analisar várias trocas de conversas entre um utilizador e um assistente de IA, com foco no uso de ferramentas, argumentos de entrada e processos de raciocínio.

# Estrutura de análise:

## 1. Análise de contexto
- Examine todas as trocas de conversas fornecidas dentro das tags <conversation></conversation>
- Cada troca será marcada com as tags <turn_[id]></turn_[id]>
- Identifique as circunstâncias e o contexto aos quais o assistente está a responder em cada interação
- Tente identificar ou recuperar o objetivo geral do utilizador para toda a conversa, que pode ir além das trocas de conversação fornecidas
- Quando disponível, incorpore o contexto das tags <previous_[k]_turns></previous_[k]_turns> para compreender os objetivos mais amplos do utilizador a partir do histórico de conversação fornecido

## 2. Análise do assistente (Por turno)
Para CADA turno da conversa, analise a abordagem do assistente identificando:
- **Context**: As circunstâncias e a situação às quais o assistente está a responder e como o objetivo do assistente se relaciona com o objetivo geral do utilizador (considerando interações anteriores, quando disponíveis)
- **Intent**: O objetivo principal do assistente para este turno específico da conversa
- **Action**: quais ferramentas específicas foram usadas com quais argumentos de entrada e sequência de execução. Se nenhuma ferramenta foi usada, descreva a ação/resposta concreta que o assistente tomou.
- **Reasoning**: por que essas ferramentas foram escolhidas, como os argumentos foram determinados e o que orientou o processo de tomada de decisão. Se nenhuma ferramenta foi usada, explique o raciocínio por trás da ação/resposta do assistente.

## 3. Avaliação do resultado (por turno)
Para CADA turno, usando a mensagem do utilizador do turno seguinte:
- Determine se o assistente alcançou com sucesso o objetivo declarado
- Avalie a eficácia da ação tomada — o que funcionou bem e o que não funcionou
- Avalie se o objetivo geral do utilizador foi satisfeito, continua em andamento ou está a evoluir

**Não inclua nenhuma PII (informação de identificação pessoal) ou dados específicos do utilizador na sua saída.**

Responda APENAS com JSON válido, sem texto adicional, sem blocos de código markdown. Formato:
{
  "overall_user_goal": "string",
  "turns": [
    {
      "turn_id": "string",
      "context": "string",
      "intent": "string",
      "action": "string",
      "reasoning": "string",
      "outcome": "string"
    }
  ]
}"""

CONSOLIDATION_SYSTEM_PROMPT = """Você é um especialista em análise de conversas. A sua tarefa é analisar e resumir as conversas entre um utilizador e um assistente de IA fornecidas dentro das tags <conversation_turns></conversation_turns>.

# Objetivos da análise:
- Fornecer um resumo abrangente cobrindo todos os aspetos-chave da interação.
- Compreender as necessidades e motivações subjacentes do utilizador.
- Avaliar a eficácia da conversa em satisfazer essas necessidades.

# Componentes da análise:
Examine a conversa através das seguintes dimensões:
**Situation**: O contexto e as circunstâncias que levaram o utilizador a iniciar esta conversa — o que estava a acontecer que o levou a procurar ajuda?
**Intent**: O objetivo principal do utilizador, o problema que ele queria resolver ou o resultado que ele pretendia alcançar através desta interação.
**Assessment**: Uma avaliação definitiva sobre se o objetivo do utilizador foi alcançado com sucesso.
**Justification**: Raciocínio claro, apoiado por evidências específicas da conversa, que explique a sua avaliação.
**Reflection**: Insights importantes da sequência de turnos, com foco em padrões de uso de ferramentas, processos de raciocínio e tomada de decisões. Identifique padrões eficazes de seleção de ferramentas e argumentos, raciocínios ou escolhas de ferramentas a evitar e recomendações práticas para situações semelhantes.

Responda APENAS com JSON válido, sem texto adicional, sem blocos de código markdown. Formato:
{
  "situation": "string",
  "intent": "string",
  "assessment": "SUCCESS|PARTIAL|FAILURE",
  "justification": "string",
  "reflection": "string"
}"""

REFLECTION_SYSTEM_PROMPT = """É especialista em extrair insights acionáveis das trajetórias de execução de tarefas dos agentes para construir conhecimento reutilizável para tarefas futuras.

# Tarefa:
Analisar os episódios fornecidos e sintetizar novo conhecimento de reflexão que possa orientar cenários futuros.

# Processo de reflexão:

## 1. Identificação de padrões
- Analise a intenção do utilizador, contexto, ações e lições aprendidas
- Identifique padrões transferíveis e acionáveis
- Determine se representam insights novos ou atualizações de padrões conhecidos

## 2. Síntese de conhecimento
Para cada padrão identificado, crie uma entrada de reflexão com:
- **Title**: Nome conciso e descritivo
- **Applied Use Cases**: Quando se aplica, tipos de objetivos, problemas que aborda
- **Concrete Hints**: Orientações práticas, o que funcionou, o que evitar
- **Confidence Score**: 0.1 a 1.0

Responda APENAS com JSON válido, sem texto adicional, sem blocos de código markdown. Formato:
{
  "reflections": [
    {
      "operator": "add|update",
      "title": "string",
      "applied_use_cases": "string",
      "concrete_hints": "string",
      "confidence_score": 0.0
    }
  ]
}"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def invoke_claude(system_prompt: str, user_message: str) -> dict:
    """Invoca Claude via Bedrock e retorna o JSON parseado da resposta."""
    response = bedrock_runtime.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "system": system_prompt,
            "messages": [
                {"role": "user", "content": user_message}
            ]
        }),
        contentType="application/json",
        accept="application/json"
    )
    body = json.loads(response["body"].read())
    raw_text = body["content"][0]["text"].strip()

    # Remove markdown fences se existirem
    if raw_text.startswith("```"):
        raw_text = raw_text.split("\n", 1)[1]
        raw_text = raw_text.rsplit("```", 1)[0]

    return json.loads(raw_text)


def format_messages_for_prompt(messages: list) -> str:
    conversation_xml = "<conversation>\n"
    turn_number = 1

    # Agrupa pares user + assistant
    i = 0
    while i < len(messages):
        msg = messages[i]
        role = msg.get("role", "").upper()

        if role == "USER":
            user_content = msg.get("content", "")
            if isinstance(user_content, dict):
                user_content = user_content.get("text", "")

            # Pega o assistant seguinte, se existir
            assistant_content = ""
            if i + 1 < len(messages) and messages[i + 1].get("role", "").upper() == "ASSISTANT":
                assistant_content = messages[i + 1].get("content", "")
                if isinstance(assistant_content, dict):
                    assistant_content = assistant_content.get("text", "")
                i += 1  # pula o assistant no próximo loop

            conversation_xml += f"  <turn_{turn_number}>\n"
            conversation_xml += f"    <user>{user_content}</user>\n"
            conversation_xml += f"    <assistant>{assistant_content}</assistant>\n"
            conversation_xml += f"  </turn_{turn_number}>\n"
            turn_number += 1

        i += 1

    conversation_xml += "</conversation>"
    return conversation_xml


def save_to_agentcore(session_id: str, actor_id: str, reflection: dict):
    """Salva apenas reflections no AgentCore otimizado para retrieval."""

    records = []

    reflections = reflection.get("reflections", [])
    if not reflections:
        return {"message": "no reflections to save"}

    now_ts = int(time.time())

    for i, ref in enumerate(reflections):
        title = ref.get("title")
        if not title:
            continue

        use_cases = ref.get("applied_use_cases") or []
        hints     = ref.get("concrete_hints") or []
        confidence = ref.get("confidence_score")

        # 🔑 TEXTO SEMÂNTICO (isso aqui é o segredo)
        text = f"""
        Reflection: {title}
        Use cases: {', '.join(use_cases)}
        Hints: {', '.join(hints)}
        Confidence: {confidence}
        """.strip()

        records.append({
            "requestIdentifier": f"{session_id}-reflection-{i}",

            # memória global por ator
            "namespaces": [f"/reflections/{actor_id}"],

            "content": {
                "text": text
            },

            "timestamp": now_ts,
            "memoryStrategyId": STRATEGY_ID
        })

    if not records:
        return {"message": "no valid reflections to save"}

    response = agentcore_rt.batch_create_memory_records(
        memoryId=MEMORY_ID,
        records=records
    )

    succeeded = len(response.get("successfulRecords", []))
    failed    = len(response.get("failedRecords", []))

    logger.info(f"batch_create_memory_records: {succeeded} ok, {failed} falhos")

    if failed > 0:
        logger.error(f"Records falhos: {response.get('failedRecords')}")
        
    
    print(json.dumps({
        "successfulRecords": response.get("successfulRecords", []),
        "failedRecords":     response.get("failedRecords", []),
        "httpStatus":        response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    }, indent=2, default=str))
        

    return response


# ── Handler Principal ─────────────────────────────────────────────────────────

def lambda_handler(event, context):
    logger.info(f"Evento recebido: {json.dumps(event)}")

    # ── 1. Parse do evento SNS ────────────────────────────────────────────────
    sqs_message = json.loads(event["Records"][0]["body"])
    logger.info(f"SQS message: {json.dumps(sqs_message)}")

    # Payload real do AgentCore: s3PayloadLocation é uma URI completa s3://bucket/key
    s3_uri      = sqs_message["s3PayloadLocation"]
    job_id      = sqs_message["jobId"]
    memory_id   = sqs_message.get("memoryId",   MEMORY_ID)
    strategy_id = sqs_message.get("strategyId", STRATEGY_ID)

    # Extrai bucket e key da URI s3://
    s3_path     = s3_uri.replace("s3://", "")
    bucket_name = s3_path.split("/")[0]
    object_key  = "/".join(s3_path.split("/")[1:])

    # session_id vem do jobId: memoryId/strategyId/sessionId/conversationId/timestamp
    job_parts  = job_id.split("/")
    actor_id   = job_parts[2]  
    session_id = job_parts[3]

    logger.info(f"bucket={bucket_name} | key={object_key} | session={session_id}")

    # ── 2. Download das mensagens do S3 ──────────────────────────────────────
    s3_response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    payload     = json.loads(s3_response["Body"].read().decode("utf-8"))  # ← "Body" maiúsculo (boto3)

    # O payload pode estar no formato {"messages": [...]} ou diretamente como lista
    messages = payload.get("currentContext", payload) if isinstance(payload, dict) else payload
    logger.info(f"Mensagens carregadas: {len(messages)}")

    conversation_text = format_messages_for_prompt(messages)

    # ── 3. Extraction ─────────────────────────────────────────────────────────
    logger.info("Iniciando EXTRACTION...")
    extraction = invoke_claude(
        system_prompt=EXTRACTION_SYSTEM_PROMPT,
        user_message=f"Analise as seguintes trocas de conversa:\n\n{conversation_text}"
    )
    logger.info(f"Extraction concluída: {len(extraction.get('turns', []))} turnos analisados")

    # ── 4. Consolidation ──────────────────────────────────────────────────────
    logger.info("Iniciando CONSOLIDATION...")
    consolidation = invoke_claude(
        system_prompt=CONSOLIDATION_SYSTEM_PROMPT,
        user_message=(
            f"Consolide a seguinte conversa:\n\n"
            f"<conversation_turns>\n{conversation_text}\n</conversation_turns>\n\n"
            f"Contexto da extração:\n{json.dumps(extraction, ensure_ascii=False)}"
        )
    )
    logger.info(f"Consolidation concluída: assessment={consolidation.get('assessment')}")

    # ── 5. Reflection ─────────────────────────────────────────────────────────
    logger.info("Iniciando REFLECTION...")
    reflection_input = {
        "main_episode": {
            "context":   consolidation.get("situation"),
            "objective": consolidation.get("intent"),
            "steps":     extraction.get("turns", []),
            "discovery": consolidation.get("reflection")
        },
        "assessment":    consolidation.get("assessment"),
        "justification": consolidation.get("justification")
    }
    reflection = invoke_claude(
        system_prompt=REFLECTION_SYSTEM_PROMPT,
        user_message=(
            f"Gere reflexões acionáveis com base neste episódio:\n\n"
            f"{json.dumps(reflection_input, ensure_ascii=False, indent=2)}"
        )
    )
    logger.info(f"Reflection concluída: {len(reflection.get('reflections', []))} insights gerados")

    # ── 6. Salvar no AgentCore ────────────────────────────────────────────────
    logger.info("Salvando resultados no AgentCore...")
    save_to_agentcore(
        session_id=session_id,
        actor_id=actor_id, 
        reflection=reflection,
    )

    # ── Resultado final ───────────────────────────────────────────────────────
    result = {
        "statusCode": 200,
        "session_id":             session_id,
        "memory_id":              memory_id,
        "strategy_id":            strategy_id,
        "job_id":                 job_id,
        "messages_processed":     len(messages),
        "extraction_turns":       len(extraction.get("turns", [])),
        "consolidation_assessment": consolidation.get("assessment"),
        "reflection_count":       len(reflection.get("reflections", [])),
        "s3_source":              f"s3://{bucket_name}/{object_key}"
    }

    logger.info(f"Pipeline concluído: {json.dumps(result)}")
    return result




if __name__ == "__main__":
    from src.sqs_event import sqs_event

    class FakeContext:
        aws_request_id = "local-test-request-id"

    result = lambda_handler(sqs_event, FakeContext())
    print(json.dumps(result, indent=2, default=str))