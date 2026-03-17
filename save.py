from bedrock_agentcore.memory import MemoryClient
import time

client = MemoryClient(region_name="us-east-1")

memory_id = ""

actor_id = ""
session_id = ""


turns = [
    ("Estou tendo muitos erros nas queries geradas pelo meu sistema com LLM", 
     "Que tipo de erro aparece com mais frequência nas queries?"),

    ("Erros de sintaxe e colunas que não existem", 
     "Entendi, isso geralmente acontece por falta de contexto do schema. Você está passando metadados das tabelas para o modelo?"),

    ("Estou passando só o nome das tabelas, não as colunas", 
     "Aí está um dos problemas. Sem as colunas, o modelo tenta inferir e acaba errando."),

    ("Mas passar todas as colunas não deixa o prompt muito grande?", 
     "Sim, pode deixar. Uma alternativa é usar retrieval para trazer só o schema relevante para cada pergunta."),

    ("Interessante, como eu faria esse retrieval?", 
     "Você pode armazenar descrições de tabelas e colunas em um sistema de memória ou vetor e buscar por similaridade."),

    ("Tipo usar embeddings das tabelas?", 
     "Exatamente. Assim você traz só o contexto necessário para a query específica."),

    ("E como evitar erro de sintaxe SQL?", 
     "Uma boa prática é usar um segundo passo de validação ou correção automática antes de executar a query."),

    ("Tipo um pós-processamento com outra LLM?", 
     "Sim, ou até regras determinísticas simples para validar estrutura básica da query."),

    ("Isso ajudaria a reduzir erros no Athena?", 
     "Com certeza. Principalmente se você combinar contexto correto + validação antes da execução."),

    ("Perfeito, vou testar isso", 
     "Ótimo. Se quiser, posso te ajudar a montar esse fluxo completo com validação e retry automático.")
]

print("Enviando 10 eventos (turnos) para AgentCore...")

for i, (user_msg, assistant_msg) in enumerate(turns, start=1):

    print(f"Evento {i}")

    client.create_event(
        memory_id=memory_id,
        actor_id=actor_id,
        session_id=session_id,
        messages=[
            (user_msg, "USER"),
            (assistant_msg, "ASSISTANT")
        ]
    )

    time.sleep(0.5)

print("10 eventos enviados")

print("Aguardando trigger do AgentCore...")
time.sleep(10)

print("✔ payload deve aparecer no S3")
print("✔ SNS deve disparar notificação")